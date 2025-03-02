import json
import os
import requests
import pandas as pd
from io import StringIO
import tkinter as tk
from tkinter import filedialog

# ANSI color codes (will be ignored on unsupported terminals)
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"
BOLD = "\033[1m"

CONFIG_FILE = 'config.json'

def print_separator(char="=", length=60):
    print(f"{CYAN}{char * length}{RESET}")

def print_banner(text, length=60):
    print_separator("=", length)
    print(f"{BOLD}{text.center(length)}{RESET}")
    print_separator("=", length)

def print_section_header(text, length=60):
    print(f"\n{BOLD}{text.center(length)}{RESET}")
    print_separator("-", length)

# -------------------------------
# Config file handling functions
# -------------------------------
def load_config(config_path=CONFIG_FILE):
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = json.load(f)
    else:
        config = {}
    config.setdefault("google_sheet_url", "")
    config.setdefault("column_mapping", {}) 
    return config

def save_config(config, config_path=CONFIG_FILE):
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=4)
    print(f"{GREEN}Configuration saved to {config_path}.{RESET}\n")

# -------------------------------
# Helper functions
# -------------------------------
def prompt_google_sheet_url():
    print_section_header("Google Sheet URL Setup")
    url = input(f"{BOLD}Enter your Google Sheet CSV URL:{RESET} ").strip()
    return url

def fetch_google_sheet(url):
    print_section_header("Fetching Google Sheet Data")
    try:
        response = requests.get(url)
        response.raise_for_status()
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data, dtype=str)
        print(f"{GREEN}Google Sheet data fetched successfully.{RESET}")
        return df
    except requests.exceptions.RequestException as e:
        print(f"{YELLOW}Error fetching Google Sheet: {e}{RESET}")
        raise
    except pd.errors.ParserError as e:
        print(f"{YELLOW}Error parsing CSV data: {e}{RESET}")
        raise

def prompt_for_column(sheet_df, expected_name):
    columns = list(sheet_df.columns)
    print_section_header(f"Select Column for {expected_name}")
    
    # Create a table with two columns
    col_width = 30
    table_header = f"| {'No.':^4} | {'Column Name':^{col_width}} |    | {'No.':^4} | {'Column Name':^{col_width}} |"
    border = f"+{'-'*6}+{'-'*(col_width+2)}+    +{'-'*6}+{'-'*(col_width+2)}+"
    print(border)
    print(table_header)
    print(border)
    
    num_items = len(columns)
    num_rows = (num_items + 1) // 2
    for i in range(num_rows):
        left_index = i
        right_index = i + num_rows
        left_num = f"{left_index}"
        left_name = columns[left_index] if left_index < num_items else ""
        if right_index < num_items:
            right_num = f"{right_index}"
            right_name = columns[right_index]
        else:
            right_num = ""
            right_name = ""
        print(f"| {left_num:^4} | {left_name:^{col_width}} |    | {right_num:^4} | {right_name:^{col_width}} |")
    print(border)
    
    # Prompt until valid input is provided
    while True:
        try:
            choice = int(input(f"{BOLD}Enter the number for the column to use as {expected_name}:{RESET} ").strip())
            if 0 <= choice < len(columns):
                selected_column = columns[choice]
                print(f"{GREEN}Selected '{selected_column}' for {expected_name}.{RESET}")
                return selected_column
            else:
                print(f"{YELLOW}Invalid choice. Please enter a valid number from the list.{RESET}")
        except ValueError:
            print(f"{YELLOW}Invalid input. Please enter a number.{RESET}")

def select_file(prompt_message):
    print_section_header("Select File")
    print(f"{BOLD}{prompt_message}{RESET}")
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename()
    root.destroy()
    if not file_path:
        raise ValueError("No file selected.")
    return file_path

def print_updated_rows_table(updated_rows):
    if not updated_rows:
        print(f"{YELLOW}No rows were updated.{RESET}")
        return
    
    header = f"+{'-'*6}+{'-'*20}+{'-'*12}+{'-'*12}+"
    print(header)
    print(f"| {'Index':^4} | {'ASIN':^18} | {'Old Cost':^10} | {'New Cost':^10} |")
    print(header)
    for row in updated_rows:
        print(f"| {str(row['index']):^4} | {row['asin']:^18} | {str(row['old_cost']):^10} | {str(row['new_cost']):^10} |")
    print(header)

# -------------------------------
# Main processing logic
# -------------------------------
def main():
    print_banner("Aura Cost Updater Tool")
    config = load_config()

    # Google Sheet URL
    if config["google_sheet_url"]:
        sheet_url = config["google_sheet_url"].strip()
        print(f"{GREEN}Using stored Google Sheet URL from config.{RESET}\n")
    else:
        sheet_url = prompt_google_sheet_url()
        config["google_sheet_url"] = sheet_url
        save_config(config)
    
    # Fetch the Google Sheet data
    sheet_df = fetch_google_sheet(sheet_url)
    
    # Determine column mapping for ASIN and COGS
    columns_lower = {col.lower(): col for col in sheet_df.columns}
    
    asin_column = config["column_mapping"].get("ASIN", "")
    if asin_column and asin_column not in sheet_df.columns:
        print(f"{YELLOW}Stored ASIN column '{asin_column}' not found. Please re-select.{RESET}")
        asin_column = ""
    if not asin_column:
        asin_column = columns_lower.get('asin')
        if not asin_column:
            asin_column = prompt_for_column(sheet_df, 'ASIN')
        config["column_mapping"]["ASIN"] = asin_column

    cogs_column = config["column_mapping"].get("COGS", "")
    if cogs_column and cogs_column not in sheet_df.columns:
        print(f"{YELLOW}Stored COGS column '{cogs_column}' not found. Please re-select.{RESET}")
        cogs_column = ""
    if not cogs_column:
        cogs_column = columns_lower.get('cogs')
        if not cogs_column:
            cogs_column = prompt_for_column(sheet_df, 'COGS')
        config["column_mapping"]["COGS"] = cogs_column

    save_config(config)
    
    # Rename columns for consistency
    sheet_df = sheet_df.rename(columns={asin_column: 'ASIN', cogs_column: 'COGS'})
    sheet_df['COGS'] = (
        sheet_df['COGS']
        .astype(str)
        .str.replace('$', '', regex=False)
        .str.replace(',', '', regex=False)
        .replace('', None)
        .astype(float)
    )
    
    aura_file = select_file("Please select your aura.csv file using the file dialog:")
    print_section_header("Processing aura CSV File")
    aura_df = pd.read_csv(aura_file)
    
    if 'asin' not in aura_df.columns or 'cost' not in aura_df.columns:
        raise ValueError("The aura CSV file must have both 'asin' and 'cost' columns.")
    
    aura_df['asin'] = aura_df['asin'].astype(str).str.strip()
    sheet_df['ASIN'] = sheet_df['ASIN'].astype(str).str.strip()
    aura_df['cost'] = pd.to_numeric(aura_df['cost'], errors='coerce')
    
    updated_rows = []
    for index, row in aura_df.iterrows():
        if pd.isna(row['cost']):
            match = sheet_df[sheet_df['ASIN'] == row['asin']]
            if not match.empty:
                new_cost = match.iloc[0]['COGS']
                if not pd.isna(new_cost):
                    aura_df.at[index, 'cost'] = float(new_cost)
                    updated_rows.append({
                        'index': index,
                        'asin': row['asin'],
                        'old_cost': row['cost'],
                        'new_cost': float(new_cost)
                    })
    
    # Save updated CSV in the START HERE folder
    script_dir = os.path.dirname(os.path.abspath(__file__))
    leads_to_aura_dir = os.path.dirname(script_dir)  # one level up
    start_here_dir = os.path.join(leads_to_aura_dir, "START HERE")
    output_file = os.path.join(start_here_dir, 'aura_updated.csv')
    aura_df.to_csv(output_file, index=False)
    
    print_section_header("Update Summary")
    print(f"{BOLD}Rows Updated:{RESET}")
    print_updated_rows_table(updated_rows)
    print(f"{GREEN}Updated file saved as:{RESET} {output_file}\n")
    print_separator()

if __name__ == "__main__":
    main()