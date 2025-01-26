#!/bin/bash

# Installation Script for Python and Required Packages

echo "Checking for Homebrew..."

# Install Homebrew if not installed
if ! command -v brew &> /dev/null
then
    echo "Homebrew not found. Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
else
    echo "Homebrew is already installed."
fi

echo "Installing Python..."
brew install python

echo "Installing required Python packages: pandas and tk..."
pip3 install pandas tk

echo "Installation completed successfully."