#!/usr/bin/env bash
# Start the ocal core backend (PostGIS + auto-sync + libheadway).
#
#   ./start.sh                 core services only
#   ./start.sh maps            core + headway/Valhalla/tileserver
#   ./start.sh billing ai      core + lago and ollama
#   ./start.sh all             everything (needs a machine with plenty of RAM)
#   ./start.sh --build         rebuild images first
#   ./start.sh --no-sync       skip auto-sync (database + tileserver only)
set -euo pipefail

cd "$(dirname "$0")"

usage() {
    sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
}

if docker compose version >/dev/null 2>&1; then
    DC=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
    DC=(docker-compose)
else
    echo "docker compose is required" >&2
    exit 1
fi

PROFILES=()
BUILD_ARGS=()
RUN_SYNC=1

for arg in "$@"; do
    case "$arg" in
        maps|billing|ai) PROFILES+=(--profile "$arg") ;;
        all) PROFILES+=(--profile maps --profile billing --profile ai) ;;
        --build) BUILD_ARGS+=(--build) ;;
        --no-sync) RUN_SYNC=0 ;;
        -h|--help) usage; exit 0 ;;
        *)
            echo "unknown argument: $arg" >&2
            usage
            exit 2
            ;;
    esac
done

if [ ! -f .env ]; then
    cp .env.example .env
    echo "created .env from .env.example"
fi

mkdir -p data/auto-sync

CORE=(postgres libheadway)
if [ "$RUN_SYNC" = 1 ]; then
    CORE=(postgres auto-sync libheadway)
fi

echo "==> starting: ${CORE[*]} ${PROFILES[*]:-}"
"${DC[@]}" "${PROFILES[@]+"${PROFILES[@]}"}" up -d "${BUILD_ARGS[@]+"${BUILD_ARGS[@]}"}" "${CORE[@]}"

AUTO_SYNC_PORT="$(grep -E '^AUTO_SYNC_PORT=' .env | tail -1 | cut -d= -f2)"
AUTO_SYNC_PORT="${AUTO_SYNC_PORT:-8000}"
LIBHEADWAY_PORT="$(grep -E '^LIBHEADWAY_PORT=' .env | tail -1 | cut -d= -f2)"
LIBHEADWAY_PORT="${LIBHEADWAY_PORT:-9123}"

if [ "$RUN_SYNC" = 1 ]; then
    echo "==> waiting for auto-sync on http://127.0.0.1:${AUTO_SYNC_PORT}"
    for _ in $(seq 1 60); do
        if health="$(curl -fsS "http://127.0.0.1:${AUTO_SYNC_PORT}/health" 2>/dev/null)"; then
            echo "    ${health}"
            break
        fi
        sleep 2
    done

    echo "==> kicking off the first OSM + Overture sync"
    curl -fsS -X POST "http://127.0.0.1:${AUTO_SYNC_PORT}/api/sync/all" >/dev/null || \
        echo "    sync request failed, trigger it later with POST /api/sync/all"
fi

cat <<EOF

auto-sync   http://127.0.0.1:${AUTO_SYNC_PORT}      (docs: /docs)
libheadway  http://127.0.0.1:${LIBHEADWAY_PORT}     (tiles: /tileserver/data/default.json)
postgres    postgresql://127.0.0.1:5432

Useful calls:
  curl "http://127.0.0.1:${AUTO_SYNC_PORT}/api/roads/history?limit=5"
  curl "http://127.0.0.1:${AUTO_SYNC_PORT}/api/closures"
  curl -X POST "http://127.0.0.1:${AUTO_SYNC_PORT}/api/pmtiles/extract" \\
       -H 'content-type: application/json' \\
       -d '{"bbox":{"min_lon":5.20,"min_lat":52.00,"max_lon":5.23,"max_lat":52.02},"name":"village"}'

Logs:  docker compose logs -f auto-sync
Stop:  docker compose down
EOF
