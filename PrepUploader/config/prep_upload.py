import os
import sys
import pandas as pd
from tkinter import filedialog, Tk

def get_file_path(prompt):
    """
    Prompts the user and opens a file dialog to select a file.
    """
    print(prompt, end="")
    input()  # Wait for the user to press Enter
    root = Tk()
    root.withdraw()  # Hide the root Tkinter window
    print("Select file dialog has opened. Please move this dialog if you do not see it.")
    file_path = filedialog.askopenfilename()
    root.destroy()
    if not file_path:
        print("No file selected. Exiting.")
        sys.exit(1)
    print(f"Selected file: {file_path}")
    return file_path

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

def start_conversion(leads_sheet_path, prep_sheet_path):
    """
    Converts the leads sheet to a format compatible with Instant Fulfillment's import feature.
    """
    print("Starting conversion...")
    
    try:
        # Load the leads Excel file
        leads_df = pd.read_excel(leads_sheet_path)
        # print("\nLeads DataFrame Preview:")
        # print(leads_df.head())
        
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
        total_rows = len(leads_df)
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
            date_value = row['Date']
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
            bundled = "Yes" if pd.notna(row.get('Bundled?')) and str(row['Bundled?']).strip() != "" else "No"
            
            # Handle '# Units in Bundle'
            if bundled == "Yes":
                try:
                    units_in_bundle = int(row['Bundled?'])
                except (ValueError, TypeError):
                    print(f"Invalid '# Units in Bundle' in row {excel_row_number}: {row['Bundled?']}")
                    sys.exit(1)
            else:
                units_in_bundle = 0
            
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
    except Exception as e:
        print(f"Error during conversion: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        print("Convert your leads sheet to a sheet that works with Instant Fulfillment's import feature.\n")
        leads_path = get_file_path("Please upload leads file (PRESS ENTER):")
        prep_path = "config/IF_PREP_SHEET.csv"
        start_conversion(leads_path, prep_path)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
