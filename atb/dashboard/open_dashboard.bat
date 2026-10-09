@echo off
rem Opens the ATB inputs dashboard in your browser. Double-click this file.
rem Streamlit is installed in the Anaconda base environment, not in reeds2,
rem so this uses base. Change PY below if you install it somewhere else.
set "PY=%LOCALAPPDATA%\anaconda3\python.exe"
if not exist "%PY%" (
    echo Python not found at %PY%
    echo Edit the PY line in this file to point at a Python that has streamlit.
    pause
    exit /b 1
)
cd /d "%~dp0.."
"%PY%" -m streamlit run dashboard\app.py
pause
