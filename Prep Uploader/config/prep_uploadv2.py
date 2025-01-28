import os
import sys
import pandas as pd
import requests

def get_google_sheet_url(prompt):
    """
    Prompts the user to input a Google Sheets CSV URL.
    """
    print(prompt, end="")
    url = input().strip()
    if not url:
        print("No URL provided. Exiting.")
        sys.exit(1)
    print(f"Received Google Sheet URL: {url}")
    return url

def fetch_google_sheet(url):
    """
    Fetches the Google Sheet CSV data from the provided URL and returns a pandas DataFrame.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises HTTPError for bad responses
        # Assuming the sheet is published as CSV
        from io import StringIO
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data)
        print("Google Sheet data fetched successfully.")
        return df
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Google Sheet: {e}")
        sys.exit(1)
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV data: {e}")
        sys.exit(1)

def convert_date_format(date_value, row_number):
    """
    Converts a date to MM/DD/YYYY format.
    If the date format is invalid, prints an error and exits.
    Utilizes pandas' to_datetime for robust date parsing.
    """
    try:
        # Attempt to parse the date using pandas
        parsed_date = pd.to_datetime(date_value, errors='raise')
        return parsed_date.strftime("%m/%d/%Y")
    except Exception:
        print(f"Date format error in row {row_number}: {date_value}")
        sys.exit(1)

def start_conversion(leads_df, prep_sheet_path):
    """
    Converts the leads DataFrame to a format compatible with Instant Fulfillment's import feature.
    """
    print("Starting conversion...")
    
    try:
        # Load the prep CSV
        prep_df = pd.read_csv(prep_sheet_path)
        
        # Identify the first column name
        first_col = leads_df.columns[0]
        other_cols = leads_df.columns[1:]
        
        # Create a mask for rows where only the first column is filled
        mask_first_col_filled = leads_df[first_col].notna() & leads_df[first_col].astype(str).str.strip().ne('')
        mask_other_cols_empty = leads_df[other_cols].isnull().all(axis=1) | leads_df[other_cols].astype(str).apply(lambda x: x.str.strip()).eq('').all(axis=1)
        marker_row_mask = mask_first_col_filled & mask_other_cols_empty
        
        # Find marker rows
        marker_rows = leads_df[marker_row_mask]
        # total_rows = len(leads_df)
        # print(f"Total rows in DataFrame: {total_rows}")
        
        if not marker_rows.empty:
            # Get the first marker row index
            first_marker_idx = marker_rows.index[0]
            # print(f"\nMarker row found at DataFrame index: {first_marker_idx} (Excel row {first_marker_idx + 1})")
            # Slice the DataFrame to include only rows after the marker row
            data_to_process = leads_df.iloc[first_marker_idx + 1:]
            print(f"Number of new buys to process: {len(data_to_process)}")
            
            # Debug: Print the rows to be processed
            print("\nRows to Process:")
            print(data_to_process)
        else:
            print("\nNo marker row found. Processing all rows.")
            data_to_process = leads_df
        
        if data_to_process.empty:
            print("No data to process after the marker row.")
            sys.exit(0)
        
        # Initialize a list to collect processed data
        processed_data = []
        
        # Iterate through each row after the marker
        for idx, row in data_to_process.iterrows():
            excel_row_number = idx + 2  # +1 for 1-based indexing and +1 for marker row
            date_value = row.get('Date', None)
            if pd.isna(date_value):
                print(f"Missing 'Date' in row {excel_row_number}. Exiting.")
                sys.exit(1)
            converted_date = convert_date_format(date_value, excel_row_number)
            
            # Extract other required fields with default values
            item_name = row.get('Name', 'N/A')
            asin = row.get('ASIN', 'N/A')
            cogs = row.get('COGS', 0)
            sale_price = row.get('Sale Price', 0)
            fba_or_fbm = "FBA"
            supplier_retailer = "temp"
            size_color = "N/A"
            
            # Process 'Bundled?' column
            bundled_raw = row.get('Bundled?', None)
            bundled = "Yes" if pd.notna(bundled_raw) and str(bundled_raw).strip() != "" else "No"
            
            # Handle '# Units in Bundle'
            if bundled == "Yes":
                try:
                    units_in_bundle = int(bundled_raw)
                except (ValueError, TypeError):
                    print(f"Invalid '# Units in Bundle' in row {excel_row_number}: {bundled_raw}")
                    sys.exit(1)
            else:
                units_in_bundle = ""
            
            units_expected = row.get('Amount Purchased', 0)
            
            # Append the processed row to the list
            processed_data.append({
                'Order Date': converted_date,
                'Item Name / Description': item_name,
                'ASIN': asin,
                # 'Order #': row['Order #'],  # Uncomment if needed
                'COGS': cogs,
                'Requested List Price': sale_price,
                'FBA or FBM': fba_or_fbm,
                'Supplier / Retailer': supplier_retailer,
                'Size / Color': size_color,
                'Bundled?': bundled,
                '# Units in Bundle': units_in_bundle,
                '# Units Expected': units_expected
            })
        
        # Create the output DataFrame from the processed data
        output_df = pd.DataFrame(processed_data, columns=prep_df.columns)
        
        # Ensure the output directory exists
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        
        # Save the processed data to CSV
        output_file_path = os.path.join(output_dir, "Processed_Instant_Fulfillment_Template.csv")
        output_df.to_csv(output_file_path, index=False)
        
        print(f"\nConversion completed successfully. File saved as: {output_file_path}")

    except FileNotFoundError as fnf_error:
        print(f"File not found error: {fnf_error}")
        sys.exit(1)
    except pd.errors.ParserError as e:
        print(f"CSV parsing error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error during conversion: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        print("Convert your leads sheet to a sheet that works with Instant Fulfillment's import feature.\n")
        leads_url = get_google_sheet_url("Please enter the Google Sheet CSV URL and press ENTER: ")
        leads_df = fetch_google_sheet(leads_url)
        prep_path = "config/IF_PREP_SHEET.csv"
        if not os.path.exists(prep_path):
            print(f"Prep sheet not found at {prep_path}. Exiting.")
            sys.exit(1)
        start_conversion(leads_df, prep_path)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
