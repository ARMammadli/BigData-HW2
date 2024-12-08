#!/bin/bash

# Define dataset directory and file paths
DATA_DIR=${DATA_DIR:-"data"}
ZIP_FILE="$DATA_DIR/car-sales-data.zip"
DATA_FILE="$DATA_DIR/car_sales_data.csv"
RENAMED_FILE="$DATA_DIR/data.csv"
ENV_DIR=".env"  # Virtual environment directory
REQUIREMENTS_FILE="requirements.txt"  # Path to requirements.txt

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed. Please install Python 3 and try again."
    exit 1
fi

# Create and activate a virtual environment
if [ ! -d "$ENV_DIR" ]; then
    echo "Creating virtual environment in $ENV_DIR..."
    python3 -m venv "$ENV_DIR"
fi

echo "Activating virtual environment..."
source "$ENV_DIR/bin/activate"

# Ensure pip is up-to-date
echo "Upgrading pip in virtual environment..."
pip install --upgrade pip

# Check if the renamed dataset already exists
if [ -f "$RENAMED_FILE" ]; then
    echo "Dataset already exists at $RENAMED_FILE. Skipping download and extraction."
else
    # Create data directory if it doesn't exist
    mkdir -p "$DATA_DIR"

    # Download dataset using Kaggle API
    echo "Downloading dataset from Kaggle..."
    if kaggle datasets download -d suraj520/car-sales-data -p "$DATA_DIR"; then
        echo "Download complete."
    else
        echo "Error: Failed to download the dataset. Check your Kaggle API credentials and internet connection."
        exit 1
    fi

    # Unzip the downloaded dataset
    echo "Extracting dataset..."
    if unzip -qo "$ZIP_FILE" -d "$DATA_DIR"; then
        echo "Dataset successfully extracted to $DATA_DIR."
    else
        echo "Error: Failed to extract the dataset."
        exit 1
    fi

    # Rename the main dataset file to data.csv
    if [ -f "$DATA_FILE" ]; then
        mv "$DATA_FILE" "$RENAMED_FILE"
        echo "Dataset renamed to $RENAMED_FILE"
    else
        echo "Error: Dataset file $DATA_FILE not found after extraction."
        exit 1
    fi
fi

# Verify the renamed dataset
if [ -f "$RENAMED_FILE" ]; then
    echo "Dataset is ready for processing at $RENAMED_FILE"
else
    echo "Error: Renamed dataset file not found."
    exit 1
fi

# Generate requirements.txt
echo "Generating requirements.txt..."
pip freeze > "$REQUIREMENTS_FILE"

# Verify requirements.txt
if [ -f "$REQUIREMENTS_FILE" ]; then
    echo "requirements.txt has been created successfully:"
    cat "$REQUIREMENTS_FILE"
else
    echo "Error: Failed to create requirements.txt."
    exit 1
fi

# Do not deactivate the virtual environment
echo "Virtual environment remains active."