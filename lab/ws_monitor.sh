#!/usr/bin/env bash
# Lanza el MONITOR (WM_WS_M) de una estación.   Uso:  ./ws_monitor.sh WS-01
# Alta de una WS nueva en Central:               ./ws_monitor.sh WS-07 "Parque Nuevo"
set -euo pipefail
source "$(dirname "$0")/_common.sh"
ID="${1:?Uso: $0 <ID_WS> [ubicacion para dar de alta]}"
EXTRA=()
if [ -n "${2:-}" ]; then EXTRA=(--register --location "$2"); fi
NAME="wm-$(slug "$ID")-m"
docker rm -f "$NAME" >/dev/null 2>&1 || true
exec docker run -it --rm --name "$NAME" --network "$NET" wm-ws \
  python WM_WS_M.py --id "$ID" --central "$CENTRAL" --engine-port 6000 ${EXTRA[@]+"${EXTRA[@]}"}
