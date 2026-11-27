# the-ocal-is-core

Maps, routing and geodata plumbing for ocal. Three services form the core, and the
heavy reference projects run behind Docker profiles so they never compete for RAM
with the ingestion pipeline.

```
                    ┌──────────────┐
 Overpass API ─────▶│              │
 Overture (S3) ────▶│  auto-sync   │──▶ PostGIS (road history, H3 index, closures)
                    │  (FastAPI)   │
                    └──────┬───────┘
                           │ headway-cli extract
                    ┌──────▼───────┐
                    │  libheadway  │──▶ regional .pmtiles files
                    └──────┬───────┘
                           │ tiles
                    ┌──────▼───────┐
                    │   Valhalla   │  narrow streets, tracks, flyovers, H3 blocks
                    └──────────────┘
```

## Quick start

```bash
./start.sh            # Linux/macOS
start.bat             # Windows (Docker Desktop)
```

The scripts copy `.env.example` to `.env`, start `postgres`, `auto-sync` and
`libheadway`, wait for the health check and trigger the first OSM + Overture sync.

Optional profiles:

```bash
./start.sh maps       # headway frontend, Valhalla, tileserver, travelmux
./start.sh billing    # lago-main API, worker, frontend, Postgres, Redis
./start.sh ai         # ollama-main
./start.sh all        # everything, needs ~16 GB RAM
```

## Repository layout

| Path | What it is |
| --- | --- |
| `auto-sync/` | New ingestion + API service (this repo's own code). |
| `headway-main/` | Headway frontend + Valhalla stack, `valhalla/` holds our routing config. |
| `libheadway-main/` | Rust tileserver/PMTiles library, extended with a `headway-cli` binary. |
| `h3-master (1)/` | Uber H3 reference implementation. |
| `maplibre-gl-js-main/` | MapLibre GL JS reference. |
| `kong-master/` | Kong gateway reference. |
| `lago-main/` | Billing platform, run under the `billing` profile. |
| `supertokens-core-master/` | Auth core reference. |
| `ollama-main/` | Local LLM runtime, run under the `ai` profile. |

## auto-sync

FastAPI worker that turns OpenStreetMap and Overture Maps data into a versioned,
H3-indexed road history in PostGIS.

* **Sources** – Overpass API (`highway=*`, `building=*`, `ele` nodes) and Overture
  Maps `transportation/segment` + `buildings/building` parquet, read with DuckDB
  straight from the public S3 bucket.
* **H3** – every geometry is indexed at resolution 9 (configurable). The primary
  cell drives lookups; all intersecting cells are stored in `h3_cells` for
  spatial joins and closure fan-out.
* **Road history** – `road_history` keeps one row per version with `road_id`,
  `h3_index`, `version`, `created_at`, `author_name`, `source`, `geom`,
  `elevation_m`, `road_class`, `name`, `tags`, `content_hash`. Re-syncing an
  unchanged feature is a no-op; a changed feature gets `version + 1`.
* **Live closures** – `road_closures` stores blocked H3 cells or single roads,
  optionally with a time window. Active rows are exposed as Valhalla
  `exclude_polygons`.
* **PMTiles** – offline extracts are cut from a planet-scale archive by
  `headway-cli` (built from `libheadway-main`), then served for download.

An Overture sync reads the parquet `bbox` column of every file that intersects
the requested area, so the first run against a small bbox still pulls a few
hundred MB from S3 (minutes on a fast link, tens of minutes on a slow one). The
result is cached in PostGIS: re-running the same bbox only creates versions for
features that actually changed.

### API

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/health` | liveness + database + H3 resolution |
| GET | `/api/config` | effective configuration |
| POST | `/api/sync/{source}` | run a sync: `osm`, `overture` or `all` |
| GET | `/api/sync/runs` | recent sync runs with counters |
| GET | `/api/roads/history` | paginated road history (filter by bbox, source, road_class) |
| GET | `/api/roads/{road_id}/history` | full version chain for one road |
| GET | `/api/roads/{road_id}` | current version of one road |
| GET | `/api/h3/{h3_index}/roads` | roads whose primary cell is this H3 index |
| GET | `/api/closures` | closures (active by default) |
| POST | `/api/closures` | block an H3 cell (`h3_index`) or a road (`road_id`) |
| DELETE | `/api/closures/{id}` | deactivate a closure |
| GET | `/api/closures/valhalla` | active closures as `exclude_polygons` |
| POST | `/api/valhalla/route` | proxy a Valhalla route with closures + costing profile injected |
| POST | `/api/pmtiles/extract` | cut a bbox out of the planet archive |
| GET | `/api/pmtiles` | list generated extracts |
| GET | `/api/pmtiles/{name}` | download an extract |

Examples:

```bash
# block a village lane by H3 cell, then by road id
curl -X POST localhost:8000/api/closures -H 'content-type: application/json' \
  -d '{"h3_index":"8919690d08fffff","reason":"flooded"}'
curl -X POST localhost:8000/api/closures -H 'content-type: application/json' \
  -d '{"road_id":"osm/way/1060029968","reason":"market day","ends_at":"2026-12-01T18:00:00Z"}'

# route around the block with the narrow-village profile
curl -X POST localhost:8000/api/valhalla/route -H 'content-type: application/json' -d '{
  "costing_profile": "village_van",
  "locations": [{"lat": 52.0, "lon": 5.2}, {"lat": 52.02, "lon": 5.23}]
}'

# offline extract for a village
curl -X POST localhost:8000/api/pmtiles/extract -H 'content-type: application/json' -d '{
  "bbox": {"min_lon": 5.2, "min_lat": 52.0, "max_lon": 5.23, "max_lat": 52.02},
  "name": "village"
}'
```

### Configuration

All settings are environment variables (see `.env.example`). The important ones:

| Variable | Default | Meaning |
| --- | --- | --- |
| `SYNC_BBOX` | `5.20,52.00,5.23,52.02` | `min_lon,min_lat,max_lon,max_lat` to ingest |
| `H3_RESOLUTION` | `9` | H3 resolution for indexing |
| `OVERPASS_URL` / `OVERPASS_TIMEOUT` | public endpoint / `180` | Overpass endpoint and per-request timeout |
| `OVERTURE_ENABLED` | `1` | read Overture parquet |
| `OVERTURE_RELEASE` | latest STAC release | pin an Overture release |
| `OVERTURE_LIMIT` | `50000` | max rows read per Overture theme per sync |
| `UPSERT_BATCH` | `500` | features per commit while writing road history |
| `ELEVATION_PROVIDER` | `none` | `opentopodata` or `open-elevation` when OSM `ele` tags are missing |
| `PMTILES_SOURCE_URL` | empty | planet-scale `.pmtiles` used for extracts |
| `VALHALLA_URL` | `http://valhalla:8002` | Valhalla endpoint for the route proxy |
| `VALHALLA_DEFAULT_PROFILE` | `village_car` | profile used when a request names none |

## Valhalla configuration

`headway-main/valhalla/valhalla.json` and `costing_profiles.json` implement the
routing behaviour: narrow village streets stay usable, rural tracks are preferred
by the `rural_track` profile, flyover/overpass guidance is kept by preserving
direction tags and landmarks, vehicle sizes are enforced per profile, and live H3
blocks are injected as `exclude_polygons`. See
[`headway-main/valhalla/README.md`](headway-main/valhalla/README.md) for the
details and the tile build command.

## libheadway

`libheadway-main/libheadway-main/Dockerfile` builds `headway-cli` from the Rust
workspace (`common/headway-cli`):

```bash
headway-cli serve     # tileserver on :9123
headway-cli extract --min-lon 5.2 --min-lat 52.0 --max-lon 5.23 --max-lat 52.02 \
                    --output /data/village.pmtiles
```

`auto-sync` shells out to the same binary, so both containers share the extraction
logic.

## Data directory

`./data/auto-sync` is bind-mounted into the container and holds generated PMTiles
(`data/auto-sync/pmtiles`) and libheadway storage
(`data/auto-sync/libheadway`). The directory is created by the start scripts and
handed to the container user by the entrypoint.

## Tests

There is no test suite yet. The pipeline was verified manually end to end:

* Overpass sync of a Dutch village bbox: 679 features, idempotent on re-run.
* Version bump on a changed feature (`version 1 → 2`), unchanged re-run skipped.
* Overture parquet ingestion with bbox filtering and building heights.
* `headway-cli extract` producing a valid clustered PMTiles archive, served by
  `headway-cli serve` and downloaded through `/api/pmtiles/{name}`.
* Valhalla proxy injecting `exclude_polygons` and the `village_van` costing profile.
* `docker compose` stack: postgres, auto-sync and libheadway healthy, PMTiles
  extracted inside the `auto-sync` container through the bundled `headway-cli`,
  closure and history endpoints answered from the compose network.
