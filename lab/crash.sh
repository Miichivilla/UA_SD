#!/usr/bin/env bash
# Simula la caída SÚBITA de un módulo (equivale a cerrar la ventana o Ctrl+C a lo bruto).
#   ./crash.sh WS-01 monitor     ./crash.sh WS-01 engine     ./crash.sh OP-01 fo
set -euo pipefail
ID="${1:?Uso: $0 <ID> <monitor|engine|fo>}"; WHAT="${2:?}"
S="$(echo "$ID" | tr 'A-Z_' 'a-z-')"
case "$WHAT" in
  monitor) docker kill "wm-$S-m" ;;
  engine)  docker kill "wm-$S-e" ;;
  fo)      docker kill "wm-fo-$S" ;;
  *) echo "módulo desconocido: $WHAT"; exit 1 ;;
esac
