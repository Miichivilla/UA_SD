#!/usr/bin/env bash
# Lanza el ENGINE (WM_WS_E) de una estación (debe estar arrancado su monitor).  Uso:  ./ws_engine.sh WS-01
# Teclas:  r regar manual · f finalizar riego · k simular KO/recuperar · s estado · q salir
set -euo pipefail
source "$(dirname "$0")/_common.sh"
ID="${1:?Uso: $0 <ID_WS>}"
S="$(slug "$ID")"
docker rm -f "wm-$S-e" >/dev/null 2>&1 || true
# El volumen conserva el estado del riego aunque el contenedor "muera" (docker kill)
exec docker run -it --rm --name "wm-$S-e" --network "$NET" -v "wm-$S-data:/data" wm-ws \
  python WM_WS_E.py --kafka "$KAFKA" --monitor "wm-$S-m:6000" --data-dir /data
