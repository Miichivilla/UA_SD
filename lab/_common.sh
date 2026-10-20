#!/usr/bin/env bash
# Carga lab.env y calcula nombres. Se incluye con:  source "$(dirname "$0")/_common.sh"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$HERE/lab.env" ]; then set -a; . "$HERE/lab.env"; set +a; fi
: "${KAFKA:?Define KAFKA en lab/lab.env (host:puerto del broker)}"
: "${CENTRAL:?Define CENTRAL en lab/lab.env (host:puerto de los sockets de WM_Central)}"
NET=wm-net
docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET" >/dev/null
slug() { echo "$1" | tr 'A-Z_' 'a-z-'; }
