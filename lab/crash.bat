@echo off
rem Uso: crash.bat WS-01 monitor | crash.bat WS-01 engine | crash.bat OP-01 fo
if "%~2"=="" ( echo Uso: %0 ID monitor^|engine^|fo & exit /b 1 )
if /i "%~2"=="monitor" docker kill wm-%~1-m
if /i "%~2"=="engine"  docker kill wm-%~1-e
if /i "%~2"=="fo"      docker kill wm-fo-%~1
