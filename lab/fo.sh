#!/usr/bin/env bash
# Lanza un Field Operator.   Uso:  ./fo.sh OP-01            (menú interactivo)
#                                  ./fo.sh OP-01 servicios_ejemplo.txt   (lee los servicios de un fichero)
set -euo pipefail
source "$(dirname "$0")/_common.sh"
ID="${1:?Uso: $0 <ID_OPERARIO> [fichero_servicios]}"
ARGS=(--kafka "$KAFKA" --id "$ID")
MOUNT=()
if [ -n "${2:-}" ]; then
  F="$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"
  MOUNT=(-v "$F:/servicios.txt:ro"); ARGS+=(--file /servicios.txt)
fi
docker rm -f "wm-fo-$(slug "$ID")" >/dev/null 2>&1 || true
exec docker run -it --rm --name "wm-fo-$(slug "$ID")" ${MOUNT[@]+"${MOUNT[@]}"} wm-fo python WM_FO.py "${ARGS[@]}"
