import os
import pandas as pd
import datetime
from tkinter import filedialog, Tk

def get_file_path(prompt):
    print(prompt, end="")
    input()  # Wait for the user to press Enter
    root = Tk()
    root.withdraw()  # Hide the root Tkinter window
    file_path = filedialog.askopenfilename()
    root.destroy()
    if not file_path:
        raise FileNotFoundError("No file selected.")
    print(f"Selected file: {file_path}")
    return file_path

def start_conversion(leads_sheet_path, prep_sheet_path):
    if not leads_sheet_path or not prep_sheet_path:
        print("Error: Both Leads and Prep sheets must be uploaded before starting the conversion.")
        return

    print("Starting conversion...")

    try:
        # Load the files
        leads_df = pd.read_excel(leads_sheet_path)
        prep_df = pd.read_csv(prep_sheet_path)

        # Initialize the output DataFrame
        output_df = pd.DataFrame(columns=prep_df.columns)

        # Convert date format
        def convert_date_format(date_value):
            if isinstance(date_value, pd.Timestamp):
                return date_value.strftime("%m/%d/%Y")
            elif isinstance(date_value, str):
                return datetime.datetime.strptime(date_value, "%Y-%m-%d").strftime("%m/%d/%Y")
            return None

        # Populate the output DataFrame
        output_df['Order Date'] = leads_df['Date'].apply(convert_date_format)
        output_df['Item Name / Description'] = leads_df['Name']
        output_df['ASIN'] = leads_df['ASIN']
        output_df['Order #'] = leads_df['Order #']
        output_df['COGS'] = leads_df['COGS']
        output_df['Requested List Price'] = leads_df['Sale Price']
        output_df['FBA or FBM'] = "FBA"
        output_df['Supplier / Retailer'] = "temp"
        output_df['Size / Color'] = "N/A"

        # Process Bundled and # Units in Bundle
        output_df['Bundled?'] = leads_df['Bundled?'].apply(lambda x: "Yes" if pd.notna(x) else "No")
        output_df['# Units in Bundle'] = leads_df['Bundled?']

        output_df['# Units Expected'] = leads_df['Amount Purchased']

        # Ensure output directory exists
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)

        # Save the processed data
        output_file_path = os.path.join(output_dir, "Processed_Instant_Fulfillment_Template.csv")
        output_df.to_csv(output_file_path, index=False)

        print(f"Conversion completed. File saved as: {output_file_path}")
    except Exception as e:
        print(f"Error during conversion: {e}")

if __name__ == "__main__":
    try:
        leads_path = get_file_path("Please upload leads file (PRESS ENTER):")
        prep_path = get_file_path("Please upload prep sheet file (PRESS ENTER):")

        start_conversion(leads_path, prep_path)
    except Exception as e:
        print(f"Error: {e}")
