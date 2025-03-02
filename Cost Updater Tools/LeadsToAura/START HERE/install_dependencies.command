#!/bin/bash
# Change directory to the location of this script
cd "$(dirname "$0")"

echo "Installing required dependencies..."

# Upgrade pip (optional)
python3 -m pip install --upgrade pip

# Install required packages
python3 -m pip install requests pandas

echo "All dependencies installed successfully."
echo "Press any key to exit..."
read -n 1 -s -r