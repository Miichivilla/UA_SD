@echo off
rem Uso: ws_engine.bat WS-01      Teclas: r regar | f finalizar | k simular KO | s estado | q salir
if "%~1"=="" ( echo Uso: %0 ID_WS & exit /b 1 )
call "%~dp0_common.bat" || exit /b 1
set ID=%~1
docker rm -f wm-%ID%-e >nul 2>&1
docker run -it --rm --name wm-%ID%-e --network %NET% -v wm-%ID%-data:/data wm-ws python WM_WS_E.py --kafka %KAFKA% --monitor wm-%ID%-m:6000 --data-dir /data
