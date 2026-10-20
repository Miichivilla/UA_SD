@echo off
rem Carga lab\lab.env (líneas CLAVE=valor; las que empiezan por # se ignoran)
if exist "%~dp0lab.env" (
  for /f "usebackq eol=# tokens=1,* delims==" %%a in ("%~dp0lab.env") do set "%%a=%%b"
)
if "%KAFKA%"=="" ( echo Define KAFKA en lab\lab.env & exit /b 1 )
if "%CENTRAL%"=="" ( echo Define CENTRAL en lab\lab.env & exit /b 1 )
set NET=wm-net
docker network inspect %NET% >nul 2>&1 || docker network create %NET% >nul
exit /b 0
