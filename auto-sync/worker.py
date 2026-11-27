"""auto-sync: keep a PostGIS road history in sync with OSM + Overture, serve live H3 closures.

The service has three jobs:

1. Ingest road, building and elevation features from the OpenStreetMap Overpass API
   and from Overture Maps release parquet files, versioning every change in
   ``road_history`` (road_id, h3_index, version, created_at, author_name, source, geom).
2. Track live road closures by H3 cell and expose them to Valhalla as ``exclude_polygons``.
3. Generate offline PMTiles extracts by shelling out to the ``headway-cli`` binary
   built from ``libheadway-main``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import subprocess
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Literal, Sequence

import h3
import httpx
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field, field_validator
from shapely.geometry import shape
from shapely.validation import make_valid

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("auto-sync")


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except ValueError:
        return default


DATABASE_URL = _env("DATABASE_URL", "postgresql://ocal:ocal@postgres:5432/ocal")
H3_RESOLUTION = _env_int("H3_RESOLUTION", 9)
SYNC_BBOX = os.environ.get("SYNC_BBOX", "").strip()
OVERPASS_URL = _env("OVERPASS_URL", "https://overpass-api.de/api/interpreter")
OVERPASS_TIMEOUT = _env_int("OVERPASS_TIMEOUT", 180)
OVERPASS_RETRIES = _env_int("OVERPASS_RETRIES", 2)
OVERPASS_FALLBACK_URLS = [
    url.strip()
    for url in os.environ.get(
        "OVERPASS_FALLBACK_URLS",
        "https://overpass.kumi.systems/api/interpreter,"
        "https://overpass.private.coffee/api/interpreter",
    ).split(",")
    if url.strip()
]
OVERTURE_ENABLED = _env("OVERTURE_ENABLED", "1").lower() not in ("0", "false", "no")
OVERTURE_RELEASE = os.environ.get("OVERTURE_RELEASE", "").strip()
OVERTURE_S3_BUCKET = _env("OVERTURE_S3_BUCKET", "overturemaps-us-west-2")
OVERTURE_PARQUET_GLOB = os.environ.get("OVERTURE_PARQUET_GLOB", "").strip()
OVERTURE_LIMIT = _env_int("OVERTURE_LIMIT", 50000)
UPSERT_BATCH = _env_int("UPSERT_BATCH", 500)
ELEVATION_PROVIDER = _env("ELEVATION_PROVIDER", "none").lower()
ELEVATION_ENDPOINT = _env("ELEVATION_ENDPOINT", "https://api.open-meteo.com/v1/elevation")
LIBHEADWAY_CLI = _env("LIBHEADWAY_CLI", "headway-cli")
LIBHEADWAY_STORAGE_DIR = _env("LIBHEADWAY_STORAGE_DIR", "/data/libheadway")
PMTILES_DIR = Path(_env("PMTILES_DIR", "/data/pmtiles"))
PMTILES_SOURCE_URL = os.environ.get("PMTILES_SOURCE_URL", "").strip()
PMTILES_TIMEOUT = _env_int("PMTILES_TIMEOUT", 3600)
VALHALLA_URL = _env("VALHALLA_URL", "http://valhalla:8002").rstrip("/")
VALHALLA_COSTING_PROFILES = _env(
    "VALHALLA_COSTING_PROFILES", "/config/costing_profiles.json"
)
VALHALLA_DEFAULT_PROFILE = _env("VALHALLA_DEFAULT_PROFILE", "village_car")
MAX_CLOSURE_POLYGONS = _env_int("MAX_CLOSURE_POLYGONS", 40)
HTTP_USER_AGENT = _env("HTTP_USER_AGENT", "ocal-auto-sync/1.0 (+https://maps.earth)")


# --------------------------------------------------------------------------- #
# Database
# --------------------------------------------------------------------------- #

SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS road_history (
    id            BIGSERIAL PRIMARY KEY,
    road_id       TEXT        NOT NULL,
    h3_index      BIGINT      NOT NULL,
    h3_cells      BIGINT[]    NOT NULL DEFAULT '{}',
    version       INTEGER     NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    author_name   TEXT        NOT NULL DEFAULT 'unknown',
    source        TEXT        NOT NULL,
    road_class    TEXT,
    name          TEXT,
    elevation_m   DOUBLE PRECISION,
    geom          geometry(Geometry, 4326) NOT NULL,
    tags          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    content_hash  TEXT        NOT NULL,
    UNIQUE (road_id, version)
);

CREATE INDEX IF NOT EXISTS road_history_h3_index_idx ON road_history (h3_index);
CREATE INDEX IF NOT EXISTS road_history_h3_cells_idx ON road_history USING GIN (h3_cells);
CREATE INDEX IF NOT EXISTS road_history_geom_idx ON road_history USING GIST (geom);
CREATE INDEX IF NOT EXISTS road_history_road_idx ON road_history (road_id, version DESC);
CREATE INDEX IF NOT EXISTS road_history_source_idx ON road_history (source);
CREATE INDEX IF NOT EXISTS road_history_tags_idx ON road_history USING GIN (tags);
CREATE INDEX IF NOT EXISTS road_history_created_idx ON road_history (created_at DESC);

CREATE OR REPLACE VIEW road_current AS
    SELECT DISTINCT ON (road_id) *
    FROM road_history
    ORDER BY road_id, version DESC;

CREATE TABLE IF NOT EXISTS road_closures (
    id          BIGSERIAL PRIMARY KEY,
    h3_index    BIGINT      NOT NULL,
    road_id     TEXT,
    reason      TEXT,
    source      TEXT        NOT NULL DEFAULT 'manual',
    starts_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    ends_at     TIMESTAMPTZ,
    active      BOOLEAN     NOT NULL DEFAULT TRUE,
    geom        geometry(Geometry, 4326),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS road_closures_h3_idx ON road_closures (h3_index);
CREATE INDEX IF NOT EXISTS road_closures_active_idx ON road_closures (active, ends_at);
CREATE INDEX IF NOT EXISTS road_closures_geom_idx ON road_closures USING GIST (geom);

CREATE TABLE IF NOT EXISTS sync_runs (
    id                BIGSERIAL PRIMARY KEY,
    source            TEXT        NOT NULL,
    status            TEXT        NOT NULL DEFAULT 'running',
    bbox              TEXT,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    features_seen     INTEGER     NOT NULL DEFAULT 0,
    versions_created  INTEGER     NOT NULL DEFAULT 0,
    features_skipped  INTEGER     NOT NULL DEFAULT 0,
    error             TEXT
);

CREATE INDEX IF NOT EXISTS sync_runs_started_idx ON sync_runs (started_at DESC);
"""


class Database:
    def __init__(self, conninfo: str) -> None:
        self.pool = ConnectionPool(
            conninfo,
            min_size=1,
            max_size=_env_int("DB_POOL_SIZE", 10),
            kwargs={"row_factory": dict_row},
            open=False,
        )

    def open(self, attempts: int = 30, delay: float = 2.0) -> None:
        self.pool.open(wait=False)
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                self.pool.wait(timeout=10)
                with self.pool.connection() as conn:
                    conn.execute("SELECT 1")
                log.info("connected to postgres")
                return
            except Exception as exc:  # noqa: BLE001 - retry on any connection error
                last_error = exc
                log.warning("postgres not ready (%s/%s): %s", attempt, attempts, exc)
                time.sleep(delay)
        raise RuntimeError(f"could not connect to postgres: {last_error}")

    def init_schema(self) -> None:
        with self.pool.connection() as conn:
            conn.execute(SCHEMA_SQL)
            conn.commit()
        log.info("database schema ready")

    def close(self) -> None:
        self.pool.close()


db = Database(DATABASE_URL)


# --------------------------------------------------------------------------- #
# H3 helpers
# --------------------------------------------------------------------------- #


def h3_to_int(cell: str) -> int:
    return h3.str_to_int(cell)


def h3_to_str(value: int | str) -> str:
    return value if isinstance(value, str) else h3.int_to_str(int(value))


def cell_boundary_lonlat(cell: str) -> list[list[float]]:
    """Closed ring of [lon, lat] pairs, the shape Valhalla's exclude_polygons wants."""
    ring = [[lon, lat] for lat, lon in h3.cell_to_boundary(cell)]
    if ring and ring[0] != ring[-1]:
        ring.append(ring[0])
    return ring


def h3_cells_for_geometry(geom: dict[str, Any], resolution: int) -> list[str]:
    """Every H3 cell touched by a GeoJSON geometry, primary cell first."""
    try:
        parsed = shape(geom)
        if not parsed.is_valid:
            parsed = make_valid(parsed)
    except Exception:  # noqa: BLE001 - malformed upstream geometry
        return []

    cells: list[str] = []
    seen: set[str] = set()

    def add(cell: str) -> None:
        if cell not in seen:
            seen.add(cell)
            cells.append(cell)

    if parsed.is_empty:
        return cells

    if parsed.geom_type in ("Polygon", "MultiPolygon"):
        try:
            for cell in sorted(h3.geo_to_cells(geom, resolution)):
                add(cell)
        except Exception:  # noqa: BLE001 - invalid ring, fall back to the centroid
            pass
    elif parsed.geom_type in ("LineString", "MultiLineString"):
        lines = [parsed] if parsed.geom_type == "LineString" else list(parsed.geoms)
        for line in lines:
            coords = list(line.coords)
            if len(coords) == 1:
                add(h3.latlng_to_cell(coords[0][1], coords[0][0], resolution))
            for (lon_a, lat_a), (lon_b, lat_b) in zip(coords, coords[1:]):
                start = h3.latlng_to_cell(lat_a, lon_a, resolution)
                end = h3.latlng_to_cell(lat_b, lon_b, resolution)
                if start == end:
                    add(start)
                    continue
                for cell in h3.grid_path_cells(start, end):
                    add(cell)

    if not cells:
        centroid = parsed.centroid
        add(h3.latlng_to_cell(centroid.y, centroid.x, resolution))
    return cells


def primary_h3(geom: dict[str, Any], cells: Sequence[str]) -> str:
    if cells:
        return cells[0]
    centroid = shape(geom).centroid
    return h3.latlng_to_cell(centroid.y, centroid.x, H3_RESOLUTION)


def _round_coords(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 7)
    if isinstance(value, list):
        return [_round_coords(item) for item in value]
    if isinstance(value, dict):
        return {key: _round_coords(item) for key, item in value.items()}
    return value


# --------------------------------------------------------------------------- #
# Feature model
# --------------------------------------------------------------------------- #


@dataclass(slots=True)
class RoadFeature:
    road_id: str
    source: str
    road_class: str
    geom: dict[str, Any]
    author_name: str = "unknown"
    name: str | None = None
    elevation_m: float | None = None
    tags: dict[str, Any] = field(default_factory=dict)
    h3_index: str = ""
    h3_cells: list[str] = field(default_factory=list)

    def finalize(self, resolution: int) -> "RoadFeature":
        self.h3_cells = h3_cells_for_geometry(self.geom, resolution)
        self.h3_index = primary_h3(self.geom, self.h3_cells)
        return self

    def content_hash(self) -> str:
        tags = {k: v for k, v in self.tags.items() if k != "_fetched_at"}
        payload = {
            "source": self.source,
            "road_class": self.road_class,
            "name": self.name,
            "elevation_m": self.elevation_m,
            "geom": _round_coords(self.geom),
            "tags": _round_coords(tags),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


INSERT_SQL = """
INSERT INTO road_history (
    road_id, h3_index, h3_cells, version, author_name, source,
    road_class, name, elevation_m, geom, tags, content_hash
) VALUES (
    %(road_id)s, %(h3_index)s, %(h3_cells)s, %(version)s, %(author_name)s, %(source)s,
    %(road_class)s, %(name)s, %(elevation_m)s,
    ST_Force2D(ST_SetSRID(ST_GeomFromGeoJSON(%(geom)s), 4326)),
    %(tags)s, %(content_hash)s
)
"""


@dataclass
class UpsertStats:
    seen: int = 0
    created: int = 0
    skipped: int = 0

    def add(self, other: "UpsertStats") -> None:
        self.seen += other.seen
        self.created += other.created
        self.skipped += other.skipped

    def __str__(self) -> str:
        return f"seen={self.seen} created={self.created} skipped={self.skipped}"


def upsert_features(
    conn: Any,
    features: Iterable[RoadFeature],
    resolution: int,
    batch_size: int = UPSERT_BATCH,
) -> UpsertStats:
    """Insert a new version only when a feature's content actually changed.

    Work is committed in batches so a long sync never holds a transaction (and
    the DDL locks another process may need) open for minutes.
    """
    stats = UpsertStats()
    batch: list[RoadFeature] = []
    for feature in features:
        batch.append(feature)
        if len(batch) >= batch_size:
            _upsert_batch(conn, batch, resolution, stats)
            batch.clear()
            log.info("upsert progress: %s", stats)
    if batch:
        _upsert_batch(conn, batch, resolution, stats)
    return stats


def _upsert_batch(
    conn: Any, batch: Sequence[RoadFeature], resolution: int, stats: UpsertStats
) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            for feature in batch:
                stats.seen += 1
                feature.finalize(resolution)
                content_hash = feature.content_hash()
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))", (feature.road_id,)
                )
                cur.execute(
                    """
                    SELECT version, content_hash
                    FROM road_history
                    WHERE road_id = %s
                    ORDER BY version DESC
                    LIMIT 1
                    """,
                    (feature.road_id,),
                )
                previous = cur.fetchone()
                if previous and previous["content_hash"] == content_hash:
                    stats.skipped += 1
                    continue
                version = (previous["version"] if previous else 0) + 1
                cur.execute(
                    INSERT_SQL,
                    {
                        "road_id": feature.road_id,
                        "h3_index": h3_to_int(feature.h3_index),
                        "h3_cells": [h3_to_int(cell) for cell in feature.h3_cells],
                        "version": version,
                        "author_name": feature.author_name or "unknown",
                        "source": feature.source,
                        "road_class": feature.road_class,
                        "name": feature.name,
                        "elevation_m": feature.elevation_m,
                        "geom": json.dumps(feature.geom),
                        "tags": Jsonb(feature.tags),
                        "content_hash": content_hash,
                    },
                )
                stats.created += 1


# --------------------------------------------------------------------------- #
# Bounding boxes
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class BoundingBox:
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float

    def __post_init__(self) -> None:
        if not (-180 <= self.min_lon < self.max_lon <= 180):
            raise ValueError("longitude range must satisfy -180 <= min_lon < max_lon <= 180")
        if not (-90 <= self.min_lat < self.max_lat <= 90):
            raise ValueError("latitude range must satisfy -90 <= min_lat < max_lat <= 90")

    def as_list(self) -> list[float]:
        return [self.min_lon, self.min_lat, self.max_lon, self.max_lat]

    def as_text(self) -> str:
        return ",".join(f"{value:.7f}" for value in self.as_list())

    @classmethod
    def parse(cls, value: str) -> "BoundingBox":
        parts = [part.strip() for part in value.split(",")]
        if len(parts) != 4:
            raise ValueError("bbox must be 'min_lon,min_lat,max_lon,max_lat'")
        try:
            return cls(*(float(part) for part in parts))
        except ValueError as exc:
            raise ValueError("bbox values must be numbers") from exc


def default_bbox() -> BoundingBox | None:
    if not SYNC_BBOX:
        return None
    try:
        return BoundingBox.parse(SYNC_BBOX)
    except ValueError:
        log.warning("ignoring invalid SYNC_BBOX=%r", SYNC_BBOX)
        return None


# --------------------------------------------------------------------------- #
# Elevation lookup
# --------------------------------------------------------------------------- #


class ElevationIndex:
    """Average nearby OSM ``ele`` nodes so roads get a terrain height."""

    def __init__(self, resolution: int = 9) -> None:
        self.resolution = resolution
        self._buckets: dict[str, list[float]] = {}

    def add(self, lat: float, lon: float, value: float) -> None:
        cell = h3.latlng_to_cell(lat, lon, self.resolution)
        self._buckets.setdefault(cell, []).append(value)

    def lookup(self, lat: float, lon: float) -> float | None:
        cell = h3.latlng_to_cell(lat, lon, self.resolution)
        for candidate in [cell, *h3.grid_disk(cell, 1)]:
            values = self._buckets.get(candidate)
            if values:
                return round(sum(values) / len(values), 2)
        return None

    def __bool__(self) -> bool:
        return bool(self._buckets)


def _parse_ele(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", ".").split(";")[0].strip())
    except (TypeError, ValueError):
        return None


def sample_elevations(points: Sequence[tuple[float, float]]) -> list[float | None]:
    """Optional DEM fallback (Open-Meteo) for roads without nearby ``ele`` nodes."""
    if ELEVATION_PROVIDER != "open-meteo" or not points:
        return [None] * len(points)
    results: list[float | None] = []
    try:
        with httpx.Client(timeout=30, headers={"User-Agent": HTTP_USER_AGENT}) as client:
            for start in range(0, len(points), 100):
                batch = points[start : start + 100]
                response = client.get(
                    ELEVATION_ENDPOINT,
                    params={
                        "latitude": ",".join(f"{lat:.6f}" for lat, _ in batch),
                        "longitude": ",".join(f"{lon:.6f}" for _, lon in batch),
                    },
                )
                response.raise_for_status()
                values = response.json().get("elevation", [])
                results.extend(
                    float(value) if value is not None else None for value in values
                )
    except Exception as exc:  # noqa: BLE001 - elevation is best effort
        log.warning("elevation sampling failed: %s", exc)
        return [None] * len(points)
    if len(results) < len(points):
        results.extend([None] * (len(points) - len(results)))
    return results


# --------------------------------------------------------------------------- #
# OpenStreetMap / Overpass
# --------------------------------------------------------------------------- #

OVERPASS_QUERY = """
[out:json][timeout:{timeout}];
(
  way["highway"]({south},{west},{north},{east});
  way["building"]({south},{west},{north},{east});
  node["ele"]({south},{west},{north},{east});
);
out body geom meta;
"""


def fetch_overpass(bbox: BoundingBox) -> dict[str, Any]:
    query = OVERPASS_QUERY.format(
        timeout=OVERPASS_TIMEOUT,
        south=bbox.min_lat,
        west=bbox.min_lon,
        north=bbox.max_lat,
        east=bbox.max_lon,
    )
    last_error: Exception | None = None
    # The public Overpass instances are frequently overloaded, so every endpoint
    # in the list is tried before the sync is reported as failed.
    for url in [OVERPASS_URL, *OVERPASS_FALLBACK_URLS]:
        for attempt in range(1, OVERPASS_RETRIES + 1):
            try:
                with httpx.Client(
                    timeout=OVERPASS_TIMEOUT + 30,
                    headers={"User-Agent": HTTP_USER_AGENT},
                ) as client:
                    response = client.post(url, data={"data": query})
                    if response.status_code in (429, 500, 502, 503, 504):
                        raise RuntimeError(f"overpass busy ({response.status_code})")
                    response.raise_for_status()
                    return response.json()
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                log.warning("overpass %s attempt %s failed: %s", url, attempt, exc)
                time.sleep(3 * attempt)
    raise RuntimeError(f"overpass request failed: {last_error}")


def _way_geometry(element: dict[str, Any], close: bool = False) -> dict[str, Any] | None:
    points = element.get("geometry") or []
    coords = [
        [point["lon"], point["lat"]]
        for point in points
        if "lon" in point and "lat" in point
    ]
    if len(coords) < 2:
        return None
    if not close:
        return {"type": "LineString", "coordinates": coords}
    if coords[0] != coords[-1]:
        coords.append(coords[0])
    return {"type": "Polygon", "coordinates": [coords]}


def overpass_features(payload: dict[str, Any]) -> Iterator[RoadFeature]:
    elevation = ElevationIndex()

    for element in payload.get("elements", []):
        if element.get("type") == "node":
            value = _parse_ele((element.get("tags") or {}).get("ele"))
            if value is not None and "lat" in element and "lon" in element:
                elevation.add(element["lat"], element["lon"], value)

    for element in payload.get("elements", []):
        tags = element.get("tags") or {}
        element_type = element.get("type")
        element_id = element.get("id")
        author = element.get("user") or "unknown"
        meta = {
            "osm_version": element.get("version"),
            "osm_timestamp": element.get("timestamp"),
            "osm_changeset": element.get("changeset"),
        }

        if element_type == "way" and tags.get("highway"):
            geom = _way_geometry(element)
            if geom is None:
                continue
            centroid = shape(geom).centroid
            height = _parse_ele(tags.get("ele"))
            if height is None and elevation:
                height = elevation.lookup(centroid.y, centroid.x)
            yield RoadFeature(
                road_id=f"osm/way/{element_id}",
                source="osm",
                road_class=tags.get("highway", "unknown"),
                name=tags.get("name"),
                author_name=author,
                elevation_m=height,
                geom=geom,
                tags={**tags, **meta},
            )
        elif element_type == "way" and tags.get("building"):
            geom = _way_geometry(element, close=True)
            if geom is None:
                continue
            centroid = shape(geom).centroid
            yield RoadFeature(
                road_id=f"osm/building/{element_id}",
                source="osm",
                road_class="building",
                name=tags.get("name"),
                author_name=author,
                elevation_m=(
                    elevation.lookup(centroid.y, centroid.x) if elevation else None
                ),
                geom=geom,
                tags={**tags, **meta},
            )
        elif element_type == "node" and _parse_ele(tags.get("ele")) is not None:
            yield RoadFeature(
                road_id=f"osm/node/{element_id}",
                source="osm",
                road_class="elevation",
                name=tags.get("name"),
                author_name=author,
                elevation_m=_parse_ele(tags.get("ele")),
                geom={
                    "type": "Point",
                    "coordinates": [element["lon"], element["lat"]],
                },
                tags={**tags, **meta},
            )


# --------------------------------------------------------------------------- #
# Overture Maps
# --------------------------------------------------------------------------- #


def overture_release() -> str:
    if OVERTURE_RELEASE:
        return OVERTURE_RELEASE
    try:
        with httpx.Client(timeout=30, headers={"User-Agent": HTTP_USER_AGENT}) as client:
            catalog = client.get("https://stac.overturemaps.org/catalog.json")
            catalog.raise_for_status()
            release = catalog.json().get("latest")
            if release:
                return str(release)
    except Exception as exc:  # noqa: BLE001
        log.warning("could not discover Overture release: %s", exc)
    return "2026-11-18.0"


def overture_glob(theme: str, kind: str, release: str) -> str:
    if OVERTURE_PARQUET_GLOB:
        return OVERTURE_PARQUET_GLOB.replace("{theme}", theme).replace("{type}", kind)
    return (
        f"s3://{OVERTURE_S3_BUCKET}/release/{release}"
        f"/theme={theme}/type={kind}/*.parquet"
    )


def _duckdb_connect() -> Any:
    import duckdb  # imported lazily: only needed when Overture sync runs

    conn = duckdb.connect()
    conn.execute("SET enable_progress_bar=false")
    for extension in ("httpfs", "spatial"):
        try:
            conn.execute(f"INSTALL {extension}")
            conn.execute(f"LOAD {extension}")
        except Exception as exc:  # noqa: BLE001 - offline images may preload them
            log.debug("duckdb extension %s unavailable: %s", extension, exc)
    return conn


def overture_features(
    bbox: BoundingBox, release: str, limit: int = OVERTURE_LIMIT
) -> Iterator[RoadFeature]:
    conn = _duckdb_connect()
    try:
        yield from _overture_theme(
            conn, bbox, release, "transportation", "segment", "class", limit
        )
        yield from _overture_theme(
            conn, bbox, release, "buildings", "building", "'building'", limit
        )
    finally:
        conn.close()


def _overture_theme(
    conn: Any,
    bbox: BoundingBox,
    release: str,
    theme: str,
    kind: str,
    road_class_expr: str,
    limit: int,
) -> Iterator[RoadFeature]:
    glob = overture_glob(theme, kind, release).replace("'", "''")
    predicate = (
        f"bbox.xmin <= {bbox.max_lon} AND bbox.xmax >= {bbox.min_lon} "
        f"AND bbox.ymin <= {bbox.max_lat} AND bbox.ymax >= {bbox.min_lat}"
    )
    # The bbox predicate is cheap (parquet row-group stats), but geometry is not.
    # Locate the files that can contain matches first, then read only those.
    candidates = conn.execute(
        f"SELECT DISTINCT filename FROM read_parquet('{glob}', filename = true) "
        f"WHERE {predicate}"
    ).fetchall()
    files = [str(row[0]) for row in candidates]
    if OVERTURE_PARQUET_GLOB and "{" not in OVERTURE_PARQUET_GLOB:
        # One glob can match several themes; keep only this theme's files.
        scoped = [p for p in files if f"theme={theme}" in p or f"/{theme}/" in p]
        if scoped:
            files = scoped
    if not files:
        log.info("overture %s/%s: no files intersect bbox", theme, kind)
        return
    log.info("overture query %s/%s: %d candidate file(s)", theme, kind, len(files))
    sources = ", ".join("'" + path.replace("'", "''") + "'" for path in files)
    height_column = "height" if kind == "building" else "NULL AS height"
    sql = f"""
        SELECT
            id,
            {road_class_expr} AS road_class,
            names.primary AS name,
            {height_column},
            ST_AsGeoJSON(geometry) AS geom_json,
            sources
        FROM read_parquet([{sources}], hive_partitioning = true)
        WHERE {predicate}
        LIMIT {int(limit)}
    """
    cursor = conn.execute(sql)
    columns = [description[0] for description in cursor.description]
    while True:
        rows = cursor.fetchmany(500)
        if not rows:
            break
        for row in rows:
            record = dict(zip(columns, row))
            try:
                geom = json.loads(record["geom_json"])
            except (TypeError, json.JSONDecodeError):
                continue
            tags: dict[str, Any] = {"overture_theme": theme, "overture_type": kind}
            if record.get("height") is not None:
                tags["building_height_m"] = float(record["height"])
            sources = record.get("sources")
            if isinstance(sources, str):
                try:
                    sources = json.loads(sources)
                except json.JSONDecodeError:
                    sources = None
            if sources:
                tags["overture_sources"] = json.loads(json.dumps(sources, default=str))
            yield RoadFeature(
                road_id=f"overture/{kind}/{record['id']}",
                source="overture",
                road_class=str(record.get("road_class") or kind),
                name=record.get("name"),
                author_name="overture",
                elevation_m=(
                    float(record["height"]) if record.get("height") is not None else None
                ),
                geom=geom,
                tags=tags,
            )


# --------------------------------------------------------------------------- #
# Sync runs
# --------------------------------------------------------------------------- #

SYNC_LOCK = threading.Lock()


def _create_run(source: str, bbox: BoundingBox | None) -> int:
    with db.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO sync_runs (source, bbox) VALUES (%s, %s) RETURNING id",
                (source, bbox.as_text() if bbox else None),
            )
            run_id = cur.fetchone()["id"]
        conn.commit()
    return run_id


def _finish_run(
    run_id: int, status: str, stats: UpsertStats, error: str | None = None
) -> None:
    with db.pool.connection() as conn:
        conn.execute(
            """
            UPDATE sync_runs
            SET status = %s, finished_at = now(), features_seen = %s,
                versions_created = %s, features_skipped = %s, error = %s
            WHERE id = %s
            """,
            (status, stats.seen, stats.created, stats.skipped, error, run_id),
        )
        conn.commit()


def run_sync(
    run_id: int,
    source: Literal["osm", "overture", "all"],
    bbox: BoundingBox | None,
    resolution: int,
) -> None:
    """Background entry point: serialised by SYNC_LOCK, releases it in ``finally``."""
    stats = UpsertStats()
    status = "ok"
    error: str | None = None
    try:
        if bbox is None:
            raise ValueError("no bbox supplied; pass one in the request or set SYNC_BBOX")
        if source in ("osm", "all"):
            payload = fetch_overpass(bbox)
            with db.pool.connection() as conn:
                partial = upsert_features(conn, overpass_features(payload), resolution)
            stats.add(partial)
            log.info("osm sync: %s", partial)

        if source in ("overture", "all") and OVERTURE_ENABLED:
            release = overture_release()
            with db.pool.connection() as conn:
                partial = upsert_features(
                    conn, overture_features(bbox, release), resolution
                )
            stats.add(partial)
            log.info("overture sync (%s): %s", release, partial)
    except Exception as exc:  # noqa: BLE001 - failure is reported through sync_runs
        status = "failed"
        error = str(exc)
        log.exception("sync %s failed", source)
    finally:
        try:
            _finish_run(run_id, status, stats, error)
        finally:
            SYNC_LOCK.release()


def trigger_sync(
    background: BackgroundTasks,
    source: Literal["osm", "overture", "all"],
    bbox: BoundingBox | None,
    resolution: int,
) -> int:
    if not SYNC_LOCK.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="a sync run is already in progress")
    try:
        run_id = _create_run(source, bbox)
    except Exception:
        SYNC_LOCK.release()
        raise
    background.add_task(run_sync, run_id, source, bbox, resolution)
    return run_id


# --------------------------------------------------------------------------- #
# Valhalla
# --------------------------------------------------------------------------- #

_profile_cache: tuple[float, dict[str, Any]] = (0.0, {})


def valhalla_profiles() -> dict[str, Any]:
    global _profile_cache
    path = Path(VALHALLA_COSTING_PROFILES)
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return {}
    if mtime != _profile_cache[0]:
        try:
            _profile_cache = (mtime, json.loads(path.read_text()))
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("could not read %s: %s", path, exc)
            return {}
    return _profile_cache[1]


def active_closure_rows(
    conn: Any, at: datetime | None = None, h3_filter: int | None = None
) -> list[dict[str, Any]]:
    clause = "active AND (ends_at IS NULL OR ends_at > %(at)s)"
    params: dict[str, Any] = {"at": at or datetime.now(timezone.utc)}
    if h3_filter is not None:
        clause += " AND h3_index = %(h3)s"
        params["h3"] = h3_filter
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, h3_index, road_id, reason, source, starts_at, ends_at,
                   ST_AsGeoJSON(geom)::jsonb AS geom
            FROM road_closures
            WHERE {clause}
            ORDER BY starts_at DESC
            """,
            params,
        )
        return cur.fetchall()


def closure_polygons(limit: int = MAX_CLOSURE_POLYGONS) -> list[dict[str, Any]]:
    with db.pool.connection() as conn:
        rows = active_closure_rows(conn)
    polygons: list[dict[str, Any]] = []
    for row in rows[:limit]:
        polygons.append(
            {
                "closure_id": row["id"],
                "h3_index": h3_to_str(row["h3_index"]),
                "road_id": row["road_id"],
                "reason": row["reason"],
                "polygon": cell_boundary_lonlat(h3_to_str(row["h3_index"])),
            }
        )
    return polygons


# --------------------------------------------------------------------------- #
# PMTiles via libheadway
# --------------------------------------------------------------------------- #


def pmtiles_extract(
    bbox: BoundingBox, name: str, source_url: str | None = None
) -> dict[str, Any]:
    PMTILES_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = "".join(
        char for char in name if char.isalnum() or char in ("-", "_", ".")
    ).strip(".")
    if not safe_name:
        raise HTTPException(status_code=422, detail="invalid output name")
    if not safe_name.endswith(".pmtiles"):
        safe_name += ".pmtiles"
    output = PMTILES_DIR / safe_name
    cli = shutil.which(LIBHEADWAY_CLI) or (
        LIBHEADWAY_CLI if Path(LIBHEADWAY_CLI).exists() else None
    )
    if cli is None:
        raise HTTPException(
            status_code=503,
            detail=f"libheadway CLI {LIBHEADWAY_CLI!r} not found; build libheadway-main first",
        )
    source = source_url or PMTILES_SOURCE_URL
    if not source:
        raise HTTPException(
            status_code=422,
            detail="PMTILES_SOURCE_URL is not configured; point it at a planet-scale .pmtiles file",
        )
    command = [
        cli,
        "extract",
        "--storage-dir",
        LIBHEADWAY_STORAGE_DIR,
        "--source-url",
        source,
        "--output",
        str(output),
        "--min-lon",
        str(bbox.min_lon),
        "--min-lat",
        str(bbox.min_lat),
        "--max-lon",
        str(bbox.max_lon),
        "--max-lat",
        str(bbox.max_lat),
    ]
    log.info("running %s", " ".join(command))
    try:
        completed = subprocess.run(
            command, capture_output=True, text=True, timeout=PMTILES_TIMEOUT
        )
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(status_code=504, detail="pmtiles extraction timed out") from exc
    if completed.returncode != 0:
        log.error("pmtiles extract failed: %s", completed.stderr[-2000:])
        raise HTTPException(
            status_code=502,
            detail=f"pmtiles extraction failed: {completed.stderr[-500:]}",
        )
    return {
        "file": safe_name,
        "path": str(output),
        "size_bytes": output.stat().st_size if output.exists() else 0,
        "bbox": bbox.as_list(),
        "download_url": f"/api/pmtiles/{safe_name}",
        "stdout": completed.stdout[-1000:],
    }


# --------------------------------------------------------------------------- #
# API models
# --------------------------------------------------------------------------- #


class BBoxModel(BaseModel):
    min_lon: float = Field(ge=-180, le=180)
    min_lat: float = Field(ge=-90, le=90)
    max_lon: float = Field(ge=-180, le=180)
    max_lat: float = Field(ge=-90, le=90)

    def to_bbox(self) -> BoundingBox:
        return BoundingBox(self.min_lon, self.min_lat, self.max_lon, self.max_lat)


class SyncRequest(BaseModel):
    bbox: BBoxModel | None = None
    resolution: int = Field(default=H3_RESOLUTION, ge=5, le=13)


class ClosureCreate(BaseModel):
    h3_index: str | None = None
    road_id: str | None = None
    reason: str | None = None
    source: str = "manual"
    starts_at: datetime | None = None
    ends_at: datetime | None = None
    geom: dict[str, Any] | None = None

    @field_validator("geom")
    @classmethod
    def _geom_must_be_geojson(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        if value is not None and "type" not in value:
            raise ValueError("geom must be a GeoJSON geometry")
        return value


class PmtilesRequest(BaseModel):
    bbox: BBoxModel
    name: str = "extract"


# --------------------------------------------------------------------------- #
# Application
# --------------------------------------------------------------------------- #


@asynccontextmanager
async def lifespan(_: FastAPI):
    db.open()
    db.init_schema()
    # A run left in 'running' belongs to a previous process; nothing can finish it.
    with db.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE sync_runs SET status = 'interrupted', finished_at = now() "
                "WHERE status = 'running'"
            )
    try:
        PMTILES_DIR.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        log.warning("cannot create PMTILES_DIR %s: %s", PMTILES_DIR, exc)
    yield
    db.close()


app = FastAPI(
    title="ocal auto-sync",
    version="1.0.0",
    description=(
        "OSM/Overture road history, live H3 closures and offline PMTiles generation "
        "for the ocal maps stack."
    ),
    lifespan=lifespan,
)


def _parse_h3(value: str) -> int:
    try:
        return int(value) if value.isdigit() else int(value, 16)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"invalid h3 index: {value}") from exc


def _row_to_road(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    row["h3_index"] = h3_to_str(row["h3_index"])
    row["h3_cells"] = [h3_to_str(cell) for cell in row.get("h3_cells") or []]
    return row


@app.get("/health")
def health() -> dict[str, Any]:
    with db.pool.connection() as conn:
        conn.execute("SELECT 1")
    return {
        "status": "ok",
        "database": "ok",
        "h3_resolution": H3_RESOLUTION,
        "sync_running": SYNC_LOCK.locked(),
    }


@app.get("/api/roads/history")
def roads_history(
    h3_index: str | None = None,
    bbox: str | None = None,
    source: str | None = None,
    road_class: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = Query(200, ge=1, le=2000),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    clauses = ["TRUE"]
    params: dict[str, Any] = {"limit": limit, "offset": offset}
    if h3_index:
        clauses.append("%(h3)s = ANY(h3_cells)")
        params["h3"] = _parse_h3(h3_index)
    if bbox:
        try:
            box = BoundingBox.parse(bbox)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        clauses.append(
            "geom && ST_MakeEnvelope(%(min_lon)s, %(min_lat)s, %(max_lon)s, %(max_lat)s, 4326)"
        )
        params.update(
            min_lon=box.min_lon,
            min_lat=box.min_lat,
            max_lon=box.max_lon,
            max_lat=box.max_lat,
        )
    if source:
        clauses.append("source = %(source)s")
        params["source"] = source
    if road_class:
        clauses.append("road_class = %(road_class)s")
        params["road_class"] = road_class
    if since:
        clauses.append("created_at >= %(since)s")
        params["since"] = since
    if until:
        clauses.append("created_at <= %(until)s")
        params["until"] = until

    where = " AND ".join(clauses)
    count_params = {k: v for k, v in params.items() if k not in ("limit", "offset")}
    with db.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, road_id, h3_index, h3_cells, version, created_at,
                       author_name, source, road_class, name, elevation_m,
                       tags, content_hash, ST_AsGeoJSON(geom)::jsonb AS geom
                FROM road_history
                WHERE {where}
                ORDER BY created_at DESC, id DESC
                LIMIT %(limit)s OFFSET %(offset)s
                """,
                params,
            )
            rows = [_row_to_road(row) for row in cur.fetchall()]
            cur.execute(
                f"SELECT count(*) AS total FROM road_history WHERE {where}", count_params
            )
            total = cur.fetchone()["total"]
    return {"count": len(rows), "total": total, "versions": rows}


@app.get("/api/roads/{road_id:path}/history")
def road_history(
    road_id: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    with db.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, road_id, h3_index, h3_cells, version, created_at,
                       author_name, source, road_class, name, elevation_m,
                       tags, content_hash, ST_AsGeoJSON(geom)::jsonb AS geom
                FROM road_history
                WHERE road_id = %s
                ORDER BY version DESC
                LIMIT %s OFFSET %s
                """,
                (road_id, limit, offset),
            )
            rows = [_row_to_road(row) for row in cur.fetchall()]
    if not rows:
        raise HTTPException(status_code=404, detail=f"no history for road_id {road_id}")
    return {"road_id": road_id, "count": len(rows), "versions": rows}


@app.get("/api/roads/{road_id:path}")
def road_current(road_id: str) -> dict[str, Any]:
    with db.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, road_id, h3_index, h3_cells, version, created_at,
                       author_name, source, road_class, name, elevation_m,
                       tags, content_hash, ST_AsGeoJSON(geom)::jsonb AS geom
                FROM road_current
                WHERE road_id = %s
                """,
                (road_id,),
            )
            row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"road_id {road_id} not found")
    return _row_to_road(row)


@app.get("/api/h3/{h3_index}/roads")
def h3_roads(h3_index: str, limit: int = Query(500, ge=1, le=2000)) -> dict[str, Any]:
    cell = _parse_h3(h3_index)
    with db.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, road_id, h3_index, h3_cells, version, created_at,
                       author_name, source, road_class, name, elevation_m,
                       tags, content_hash, ST_AsGeoJSON(geom)::jsonb AS geom
                FROM road_current
                WHERE %s = ANY(h3_cells)
                ORDER BY road_class, road_id
                LIMIT %s
                """,
                (cell, limit),
            )
            rows = [_row_to_road(row) for row in cur.fetchall()]
    return {"h3_index": h3_to_str(cell), "count": len(rows), "roads": rows}


@app.get("/api/closures")
def list_closures(
    h3_index: str | None = None,
    bbox: str | None = None,
    active_only: bool = True,
    at: datetime | None = None,
) -> dict[str, Any]:
    clauses = ["TRUE"]
    params: dict[str, Any] = {}
    if active_only:
        clauses.append("active AND (ends_at IS NULL OR ends_at > %(at)s)")
        params["at"] = at or datetime.now(timezone.utc)
    if h3_index:
        clauses.append("h3_index = %(h3)s")
        params["h3"] = _parse_h3(h3_index)
    if bbox:
        try:
            box = BoundingBox.parse(bbox)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        clauses.append(
            "geom && ST_MakeEnvelope(%(min_lon)s, %(min_lat)s, %(max_lon)s, %(max_lat)s, 4326)"
        )
        params.update(
            min_lon=box.min_lon,
            min_lat=box.min_lat,
            max_lon=box.max_lon,
            max_lat=box.max_lat,
        )
    with db.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, h3_index, road_id, reason, source, starts_at, ends_at,
                       active, created_at, updated_at,
                       ST_AsGeoJSON(geom)::jsonb AS geom
                FROM road_closures
                WHERE {' AND '.join(clauses)}
                ORDER BY starts_at DESC
                """,
                params,
            )
            rows = cur.fetchall()
    for row in rows:
        row["h3_index"] = h3_to_str(row["h3_index"])
    return {"count": len(rows), "closures": rows}


@app.post("/api/closures", status_code=201)
def create_closure(body: ClosureCreate) -> dict[str, Any]:
    geom = body.geom
    h3_value: str | None = body.h3_index

    if geom is None and body.road_id:
        with db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT h3_index, ST_AsGeoJSON(geom)::jsonb AS geom
                    FROM road_current WHERE road_id = %s
                    """,
                    (body.road_id,),
                )
                road = cur.fetchone()
        if road is None:
            raise HTTPException(status_code=404, detail=f"road_id {body.road_id} not found")
        geom = road["geom"]
        h3_value = h3_value or h3_to_str(road["h3_index"])

    if h3_value is None and geom is not None:
        cells = h3_cells_for_geometry(geom, H3_RESOLUTION)
        if not cells:
            raise HTTPException(status_code=422, detail="could not derive an H3 cell")
        h3_value = cells[0]
    if h3_value is None:
        raise HTTPException(status_code=422, detail="provide h3_index, road_id or geom")

    cell = _parse_h3(h3_value)
    if geom is None:
        geom = {"type": "Polygon", "coordinates": [cell_boundary_lonlat(h3_value)]}

    with db.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO road_closures
                    (h3_index, road_id, reason, source, starts_at, ends_at, geom)
                VALUES (%s, %s, %s, %s, COALESCE(%s::timestamptz, now()), %s,
                        ST_Force2D(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)))
                RETURNING id, h3_index, road_id, reason, source, starts_at, ends_at,
                          active, created_at, updated_at, ST_AsGeoJSON(geom)::jsonb AS geom
                """,
                (
                    cell,
                    body.road_id,
                    body.reason,
                    body.source,
                    body.starts_at,
                    body.ends_at,
                    json.dumps(geom),
                ),
            )
            row = cur.fetchone()
        conn.commit()
    row["h3_index"] = h3_to_str(row["h3_index"])
    return row


@app.delete("/api/closures/{closure_id}")
def deactivate_closure(closure_id: int) -> dict[str, Any]:
    with db.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE road_closures
                SET active = FALSE, updated_at = now(), ends_at = COALESCE(ends_at, now())
                WHERE id = %s
                RETURNING id, h3_index, road_id, reason, source, starts_at, ends_at, active
                """,
                (closure_id,),
            )
            row = cur.fetchone()
        conn.commit()
    if row is None:
        raise HTTPException(status_code=404, detail=f"closure {closure_id} not found")
    row["h3_index"] = h3_to_str(row["h3_index"])
    return row


@app.get("/api/closures/valhalla")
def valhalla_closures(limit: int = Query(MAX_CLOSURE_POLYGONS, ge=1, le=500)) -> dict[str, Any]:
    polygons = closure_polygons(limit=limit)
    return {
        "exclude_polygons": [item["polygon"] for item in polygons],
        "closures": polygons,
        "count": len(polygons),
    }


@app.post("/api/valhalla/route")
def valhalla_route(body: dict[str, Any]) -> Any:
    """Proxy a Valhalla route request with live H3 closures + village costing defaults."""
    payload = json.loads(json.dumps(body))
    profile_name = str(payload.pop("costing_profile", VALHALLA_DEFAULT_PROFILE))
    profile = valhalla_profiles().get(profile_name) or {}

    if profile:
        payload.setdefault("costing", profile.get("costing", "auto"))
        target = payload["costing"]
        defaults = (profile.get("costing_options") or {}).get(target, {})
        requested = payload.setdefault("costing_options", {}).setdefault(target, {})
        for key, value in defaults.items():
            requested.setdefault(key, value)

    polygons = closure_polygons()
    if polygons:
        rings = [item["polygon"] for item in polygons]
        existing = payload.get("exclude_polygons")
        if isinstance(existing, dict) and existing.get("type") == "FeatureCollection":
            existing["features"] = [
                *existing.get("features", []),
                *[
                    {
                        "type": "Feature",
                        "properties": {"reason": item["reason"]},
                        "geometry": {"type": "Polygon", "coordinates": [item["polygon"]]},
                    }
                    for item in polygons
                ],
            ]
        else:
            payload["exclude_polygons"] = list(existing or []) + rings

    try:
        response = httpx.post(f"{VALHALLA_URL}/route", json=payload, timeout=120)
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502, detail=f"valhalla unreachable at {VALHALLA_URL}: {exc}"
        ) from exc
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text[:2000])
    return response.json()


@app.post("/api/sync/{source}", status_code=202)
def start_sync(
    source: Literal["osm", "overture", "all"],
    background: BackgroundTasks,
    body: SyncRequest | None = None,
) -> dict[str, Any]:
    body = body or SyncRequest()
    bbox = body.bbox.to_bbox() if body.bbox else default_bbox()
    if bbox is None:
        raise HTTPException(
            status_code=422, detail="no bbox: pass {'bbox': {...}} or set SYNC_BBOX"
        )
    run_id = trigger_sync(background, source, bbox, body.resolution)
    return {
        "run_id": run_id,
        "source": source,
        "bbox": bbox.as_list(),
        "status": "accepted",
    }


@app.get("/api/sync/runs")
def sync_runs(limit: int = Query(20, ge=1, le=200)) -> dict[str, Any]:
    with db.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, source, status, bbox, started_at, finished_at,
                       features_seen, versions_created, features_skipped, error
                FROM sync_runs
                ORDER BY id DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
    return {"running": SYNC_LOCK.locked(), "count": len(rows), "runs": rows}


@app.post("/api/pmtiles/extract", status_code=201)
def pmtiles_extract_endpoint(body: PmtilesRequest) -> dict[str, Any]:
    return pmtiles_extract(body.bbox.to_bbox(), body.name)


@app.get("/api/pmtiles")
def list_pmtiles() -> dict[str, Any]:
    files = sorted(PMTILES_DIR.glob("*.pmtiles")) if PMTILES_DIR.exists() else []
    return {
        "count": len(files),
        "files": [
            {
                "file": path.name,
                "size_bytes": path.stat().st_size,
                "download_url": f"/api/pmtiles/{path.name}",
            }
            for path in files
        ],
    }


@app.get("/api/pmtiles/{name}")
def download_pmtiles(name: str) -> FileResponse:
    safe = Path(name).name
    path = PMTILES_DIR / safe
    if not safe.endswith(".pmtiles") or not path.is_file():
        raise HTTPException(status_code=404, detail=f"pmtiles file {safe} not found")
    return FileResponse(path, media_type="application/vnd.pmtiles", filename=safe)


@app.get("/api/config")
def config_summary() -> dict[str, Any]:
    bbox = default_bbox()
    return {
        "h3_resolution": H3_RESOLUTION,
        "default_bbox": bbox.as_list() if bbox else None,
        "overpass_url": OVERPASS_URL,
        "overture_enabled": OVERTURE_ENABLED,
        "overture_release": OVERTURE_RELEASE or "latest",
        "elevation_provider": ELEVATION_PROVIDER,
        "valhalla_url": VALHALLA_URL,
        "valhalla_default_profile": VALHALLA_DEFAULT_PROFILE,
        "valhalla_profiles": sorted(valhalla_profiles()),
        "libheadway_cli": LIBHEADWAY_CLI,
        "pmtiles_dir": str(PMTILES_DIR),
    }
