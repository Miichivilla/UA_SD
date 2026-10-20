@echo off
rem Uso: fo.bat OP-01            |   fo.bat OP-01 C:\ruta\servicios.txt
if "%~1"=="" ( echo Uso: %0 ID_OPERARIO [fichero] & exit /b 1 )
call "%~dp0_common.bat" || exit /b 1
set ID=%~1
docker rm -f wm-fo-%ID% >nul 2>&1
if "%~2"=="" (
  docker run -it --rm --name wm-fo-%ID% wm-fo python WM_FO.py --kafka %KAFKA% --id %ID%
) else (
  docker run -it --rm --name wm-fo-%ID% -v "%~f2:/servicios.txt:ro" wm-fo python WM_FO.py --kafka %KAFKA% --id %ID% --file /servicios.txt
)
