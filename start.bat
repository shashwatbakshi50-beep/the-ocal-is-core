@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0"

rem Start the ocal core backend (PostGIS + auto-sync + libheadway).
rem
rem   start.bat                 core services only
rem   start.bat maps            core + headway/Valhalla/tileserver
rem   start.bat billing ai      core + lago and ollama
rem   start.bat all             everything (needs plenty of RAM)
rem   start.bat --build         rebuild images first
rem   start.bat --no-sync       skip auto-sync

set "PROFILES="
set "BUILD_ARGS="
set "RUN_SYNC=1"
set "CORE=postgres auto-sync libheadway"

:parse
if "%~1"=="" goto ready
if /I "%~1"=="maps" (
    set "PROFILES=!PROFILES! --profile maps"
    shift
    goto parse
)
if /I "%~1"=="billing" (
    set "PROFILES=!PROFILES! --profile billing"
    shift
    goto parse
)
if /I "%~1"=="ai" (
    set "PROFILES=!PROFILES! --profile ai"
    shift
    goto parse
)
if /I "%~1"=="all" (
    set "PROFILES=--profile maps --profile billing --profile ai"
    shift
    goto parse
)
if /I "%~1"=="--build" (
    set "BUILD_ARGS=--build"
    shift
    goto parse
)
if /I "%~1"=="--no-sync" (
    set "RUN_SYNC=0"
    shift
    goto parse
)
if /I "%~1"=="-h" goto usage
if /I "%~1"=="--help" goto usage
echo unknown argument: %~1
goto usage

:usage
echo Usage: start.bat [maps^|billing^|ai^|all] [--build] [--no-sync]
exit /b 2

:ready
where docker >nul 2>&1
if errorlevel 1 (
    echo Docker Desktop is required. Install it and run this script again.
    exit /b 1
)

docker compose version >nul 2>&1
if errorlevel 1 (
    set "DC=docker-compose"
) else (
    set "DC=docker compose"
)

if not exist ".env" (
    copy /y ".env.example" ".env" >nul
    echo created .env from .env.example
)

if not exist "data\auto-sync" mkdir "data\auto-sync"

if "%RUN_SYNC%"=="0" set "CORE=postgres libheadway"

echo ==^> starting: %CORE% %PROFILES%
%DC% %PROFILES% up -d %BUILD_ARGS% %CORE%
if errorlevel 1 exit /b 1

for /f "tokens=2 delims==" %%p in ('findstr /b /c:"AUTO_SYNC_PORT=" .env 2^>nul') do set "AUTO_SYNC_PORT=%%p"
if "%AUTO_SYNC_PORT%"=="" set "AUTO_SYNC_PORT=8000"
for /f "tokens=2 delims==" %%p in ('findstr /b /c:"LIBHEADWAY_PORT=" .env 2^>nul') do set "LIBHEADWAY_PORT=%%p"
if "%LIBHEADWAY_PORT%"=="" set "LIBHEADWAY_PORT=9123"

if "%RUN_SYNC%"=="1" call :first_sync

:summary
echo.
echo auto-sync   http://127.0.0.1:%AUTO_SYNC_PORT%      ^(docs: /docs^)
echo libheadway  http://127.0.0.1:%LIBHEADWAY_PORT%
echo postgres    postgresql://127.0.0.1:5432
echo.
echo Logs:  docker compose logs -f auto-sync
echo Stop:  docker compose down
endlocal
exit /b 0

:first_sync
echo ==^> waiting for auto-sync on http://127.0.0.1:%AUTO_SYNC_PORT%
for /l %%i in (1,1,60) do (
    powershell -NoProfile -Command "try { (Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:%AUTO_SYNC_PORT%/health' -TimeoutSec 3).Content } catch { exit 1 }" >nul 2>&1
    if not errorlevel 1 goto sync_ready
    timeout /t 2 /nobreak >nul
)
echo    auto-sync did not become healthy, check: docker compose logs auto-sync
exit /b 1

:sync_ready
echo    health endpoint is up
echo ==^> kicking off the first OSM + Overture sync
powershell -NoProfile -Command "try { Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:%AUTO_SYNC_PORT%/api/sync/all' ^| ConvertTo-Json -Compress } catch { Write-Output ('    sync request failed: ' + $_.Exception.Message) }"
exit /b 0
