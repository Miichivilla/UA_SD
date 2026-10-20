#!/usr/bin/env bash
# Copia shared/wm_common.py (fuente única) a cada módulo para que cada carpeta
# sea autocontenida (Railway / docker build usan la carpeta del módulo como contexto).
set -euo pipefail
cd "$(dirname "$0")"
for d in WM_Central WM_WS WM_FO; do
  cp shared/wm_common.py "$d/wm_common.py"
  echo "wm_common.py -> $d/"
done
