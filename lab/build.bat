@echo off
cd /d "%~dp0.."
docker build -t wm-ws .\WM_WS || exit /b 1
docker build -t wm-fo .\WM_FO || exit /b 1
echo Imagenes listas: wm-ws, wm-fo
