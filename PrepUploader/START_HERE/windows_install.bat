@echo off
REM Installation Script for Python and Required Packages

echo Installing Python using Winget...
winget install -e --id Python.Python.3 -h

echo Ensuring pip is installed and upgraded...
python -m ensurepip --upgrade

echo Installing required Python packages: pandas and tk...
pip install pandas tk

echo Installation completed successfully.
pause