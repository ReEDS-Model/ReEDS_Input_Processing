@echo off
setlocal

cd /d "%~dp0"

set "CONFIG=%~1"
if "%CONFIG%"=="" set "CONFIG=aeo_pipeline_config.json"

python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not available on PATH.
    exit /b 9009
)

echo [1/4] Running beta regression...
python aeo_beta_regression.py --config "%CONFIG%"
if errorlevel 1 goto :fail

echo [2/4] Syncing beta outputs to alpha inputs...
python sync_beta_to_alpha_inputs.py --config "%CONFIG%"
if errorlevel 1 goto :fail

echo [3/4] Running alpha regression...
python aeo_alpha_regression.py --config "%CONFIG%"
if errorlevel 1 goto :fail

echo [4/4] Generating visualization and validation...
python visualization.py --config "%CONFIG%"
if errorlevel 1 goto :fail

echo.
echo NG pipeline finished successfully.
exit /b 0

:fail
set "CODE=%errorlevel%"
echo.
echo NG pipeline failed with exit code %CODE%.
exit /b %CODE%
