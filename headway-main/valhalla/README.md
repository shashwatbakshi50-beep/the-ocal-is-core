# Valhalla configuration for narrow village streets

This directory holds the routing configuration the ocal stack mounts into the
Valhalla container.

| File | Purpose |
| --- | --- |
| `valhalla.json` | Full Valhalla service + tile build configuration (generated from Valhalla's own `valhalla_build_config`, then tuned). |
| `costing_profiles.json` | Named costing profiles (`village_car`, `rural_track`, `village_van`, `truck`, `bus`, `bicycle`, `pedestrian`) applied by `auto-sync` when it proxies a route request. |

## What the configuration changes

### Narrow village streets

`loki.service_defaults` widens the location search so rural addresses still snap
to the correct lane (`radius: 200`, `street_radius: 300`,
`search_cutoff: 50000`, `node_snap_tolerance: 5`).

The `village_car` and `village_van` profiles keep residential/service roads
attractive while still penalising them relative to arterials:

* `use_living_streets: 0.6` – living streets (woonerf, narrow village lanes) stay usable.
* `low_class_penalty: 10` / `low_class_factor: 1.1` – a light penalty for dropping
  onto residential or service roads, so the router does not zig-zag through every
  back lane when a village main street exists.
* `service_penalty: 20`, `service_factor: 1.2` – driveways and farm service roads
  are usable but not free.
* `use_highways: 0.3` – long detours onto motorways are discouraged for short
  village trips.

### Rural tracks

`rural_track` sets `use_tracks: 0.9`, `low_class_penalty: 0`, `service_penalty: 0`
and `exclude_unpaved: 0`, so farm tracks and unpaved ways are preferred. The
truck profile keeps `use_tracks: 0.15` and `exclude_unpaved: 1` so HGVs stay on
paved roads unless a track is the only option.

### Flyover / overpass instructions

Grade-separated junctions need the guidance tags from OSM, not just geometry.
The tile build therefore enables:

* `mjolnir.data_processing.use_direction_on_ways: true` – keeps `destination`,
  `destination:ref` and turn guidance on ways, which is what produces
  "take the flyover towards ..." style instructions.
* `mjolnir.landmarks: true` – landmark-based instructions at complex junctions.
* `mjolnir.data_processing.infer_turn_channels: true` and
  `infer_internal_intersections: true` – correct manoeuvres across dual
  carriageways and slip roads.
* `mjolnir.reclassify_links: true` – ramps and links are classified correctly.
* `mjolnir.hierarchy: true` and `shortcuts: true` – keep long-route performance.

Odin's markup is disabled (`markup_formatter.markup_enabled: false`) so plain
text instructions are returned; the response language is chosen per request with
Valhalla's `language` option.

### Vehicle sizes

`costing_profiles.json` sets explicit `height`, `width`, `length` and `weight`
per profile, plus `axle_load`, `axle_count` and `hazmat` for trucks. Valhalla
then honours `maxheight`, `maxwidth`, `maxweight` and `maxaxleload` restrictions
from OSM. `village_van` (2.4 m × 2.0 m × 5.4 m, 3.5 t) is the safe default for
narrow streets; `truck` uses the full HGV envelope.

### Live H3 road blocks

Valhalla has no H3 concept, so live blocks are delivered as
`exclude_polygons` (the supported "avoid area" mechanism). `service_limits` is
raised so a realistic number of blocked cells fits in one request:

```json
"max_exclude_locations": 100,
"max_exclude_polygons_length": 60000,
"max_exclude_polygons_vertices": 12000
```

`auto-sync` keeps the closures in PostGIS and either

* returns the polygons from `GET /api/closures/valhalla`, or
* proxies the whole request with `POST /api/valhalla/route`, injecting the
  polygons and the selected costing profile before calling Valhalla.

## Using the config

The root `docker-compose.yml` mounts this directory read-only at `/config` and
starts Valhalla with:

```yaml
command: ["valhalla_service", "/config/valhalla.json", "1"]
```

`headway-main/docker-compose.yaml` does the same for the standalone Headway
stack. Tile paths in `valhalla.json` (`/data/valhalla`, `tiles.tar`,
`admin.sqlite`, `tz_world.sqlite`, `elevation/`) match the layout produced by
`valhalla-init` in `headway-main/docker-compose.yaml`.

To rebuild tiles with this configuration:

```bash
valhalla_build_tiles -c headway-main/valhalla/valhalla.json extract.osm.pbf
```

If the Headway image ships its own entrypoint, keep the image's default command
and instead bind-mount `valhalla.json` over the config path it expects (usually
`/data/valhalla.json`).

## Checking a route with live blocks

```bash
curl -s -X POST localhost:8000/api/closures \
  -H 'content-type: application/json' \
  -d '{"h3_index":"8919690d08fffff","reason":"flooded lane"}'

curl -s -X POST localhost:8000/api/valhalla/route \
  -H 'content-type: application/json' \
  -d '{"costing_profile":"village_van","locations":[{"lat":52.0,"lon":5.2},{"lat":52.02,"lon":5.23}]}' \
  | jq '.trip.summary'
```
