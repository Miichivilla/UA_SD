#!/usr/bin/env bash
# Construye las imágenes Docker de los módulos del laboratorio (WM_WS y WM_FO).
set -euo pipefail
cd "$(dirname "$0")/.."
docker build -t wm-ws ./WM_WS
docker build -t wm-fo ./WM_FO
echo "Imágenes listas: wm-ws, wm-fo"
