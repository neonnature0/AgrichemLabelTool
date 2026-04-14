@echo off
REM Double-click this to launch AgrichemLabelTool in your browser.
REM First run creates a Python venv and installs dependencies (one-time, ~1 min).
REM Subsequent runs start instantly.

setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo First-time setup: creating virtual environment...
    python -m venv .venv
    if errorlevel 1 (
        echo.
        echo ERROR: Failed to create virtual environment. Is Python 3.12+ installed?
        echo   Download from: https://www.python.org/downloads/
        echo.
        pause
        exit /b 1
    )
    echo Installing dependencies...
    call .venv\Scripts\activate.bat
    python -m pip install --upgrade pip
    python -m pip install -e .[dev]
    if errorlevel 1 (
        echo.
        echo ERROR: Dependency install failed. See log above.
        echo.
        pause
        exit /b 1
    )
) else (
    call .venv\Scripts\activate.bat
)

REM Open the browser a moment after the server starts.
start "" /b cmd /c "timeout /t 2 /nobreak >nul && start http://127.0.0.1:8000"

echo.
echo ============================================================
echo  AgrichemLabelTool running at http://127.0.0.1:8000
echo  Close this window (or press Ctrl+C) to stop.
echo ============================================================
echo.

python -m uvicorn tool.app:app --port 8000

endlocal
