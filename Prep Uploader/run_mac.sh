#!/bin/bash

# Install Homebrew if not already installed
if ! command -v brew &> /dev/null
then
    echo "Homebrew not found. Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
fi

# Install Python using Homebrew
brew install python

# Install required packages
pip3 install pandas tk

# Run the Python script
python3 "$(dirname "$0")/prep_upload.py"