@echo off
REM Install Python using Winget
winget install -e --id Python.Python.3 -h

REM Check if pip is installed
python -m ensurepip --upgrade

REM Install required packages
pip install pandas tk

REM Run the Python script
python "%~dp0prep_upload.py"

pause