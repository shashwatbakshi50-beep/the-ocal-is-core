#!/bin/sh
set -e

# Bind-mounted data directories are created by Docker as root; hand them to the
# runtime user before dropping privileges.
if [ "$(id -u)" = "0" ]; then
    mkdir -p "${PMTILES_DIR:-/data/pmtiles}" "${LIBHEADWAY_STORAGE_DIR:-/data/libheadway}"
    chown -R autosync:autosync /data 2>/dev/null || true
    exec gosu autosync "$@"
fi

exec "$@"
