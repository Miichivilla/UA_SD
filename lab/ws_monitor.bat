@echo off
rem Uso: ws_monitor.bat WS-01        (alta nueva:  ws_monitor.bat WS-07 "Parque Nuevo")
if "%~1"=="" ( echo Uso: %0 ID_WS [ubicacion] & exit /b 1 )
call "%~dp0_common.bat" || exit /b 1
set ID=%~1
set EXTRA=
if not "%~2"=="" set EXTRA=--register --location "%~2"
docker rm -f wm-%ID%-m >nul 2>&1
docker run -it --rm --name wm-%ID%-m --network %NET% wm-ws python WM_WS_M.py --id %ID% --central %CENTRAL% --engine-port 6000 %EXTRA%
