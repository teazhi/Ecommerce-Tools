import pandas as pd
from dotenv import load_dotenv
import os
import requests
from io import StringIO

# Load environment variables from .env file
load_dotenv()

# Fetch the Google Sheet URL from environment variables
TEVIN_SHEET = os.getenv('TEVIN_SHEET')
if not TEVIN_SHEET:
    raise ValueError("TEVIN_SHEET environment variable is not set.")

def fetch_google_sheet(url):
    """Fetches the Google Sheet CSV data and returns a pandas DataFrame."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data, dtype=str)
        print("Google Sheet data fetched successfully.")
        return df
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Google Sheet: {e}")
        raise
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV data: {e}")
        raise

# File paths
aura_file = 'aura.csv'
output_updated_aura_file = 'aura_updated.csv'

# Load the Google Sheets CSV data (TEVIN_SHEET)
tevin_sheet_df = fetch_google_sheet(TEVIN_SHEET)

# Load aura.csv
aura_df = pd.read_csv(aura_file)

# Normalize columns for matching
tevin_sheet_df['ASIN'] = tevin_sheet_df['ASIN'].astype(str).str.strip()
aura_df['asin'] = aura_df['asin'].astype(str).str.strip()

# Clean and convert COGS column (remove dollar signs and convert to numeric)
tevin_sheet_df['COGS'] = (
    tevin_sheet_df['COGS']
    .astype(str)  # Ensure it's a string
    .str.replace('$', '', regex=False)  # Remove dollar signs
    .str.replace(',', '', regex=False)  # Remove commas (if present)
    .replace('', None)  # Replace empty strings with None
    .astype(float)  # Convert to float
)

# Ensure the 'cost' column in aura_df has a compatible dtype
if 'cost' in aura_df.columns:
    aura_df['cost'] = pd.to_numeric(aura_df['cost'], errors='coerce')
# Track updated rows
updated_rows = []

# Update costs in aura.csv using TEVIN_SHEET values (only for rows with empty cost)
for index, row in aura_df.iterrows():
    if pd.isna(row['cost']):  # Only update rows with empty cost
        match = tevin_sheet_df[tevin_sheet_df['ASIN'] == row['asin']]
        if not match.empty:
            new_cost = match.iloc[0]['COGS']
            if not pd.isna(new_cost):  # Only update if COGS is not NaN
                aura_df.at[index, 'cost'] = float(new_cost)  # Explicitly cast to float
                # Track the updated row
                updated_rows.append({
                    'index': index,
                    'asin': row['asin'],
                    'old_cost': row['cost'],
                    'new_cost': float(new_cost)
                })

# Save the updated DataFrame to a new CSV file
aura_df.to_csv(output_updated_aura_file, index=False)
print(f"Updated file saved as {output_updated_aura_file}")

# Display which rows were updated
if updated_rows:
    print("\nRows updated:")
    for row in updated_rows:
        print(f"Index: {row['index']}, ASIN: {row['asin']}, Old Cost: {row['old_cost']}, New Cost: {row['new_cost']}")
else:
    print("\nNo rows were updated.")