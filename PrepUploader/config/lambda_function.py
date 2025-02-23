import json
import os
import pandas as pd
import requests
import smtplib
import ssl
from email.message import EmailMessage
from dotenv import load_dotenv
import boto3
from io import StringIO
import csv

# Load environment variables
load_dotenv()

EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
CONFIG_S3_BUCKET = os.getenv("CONFIG_S3_BUCKET")
TEVIN_SHEET = os.getenv("TEVIN_SHEET")
DAVID_SHEET = os.getenv("DAVID_SHEET")
OSCAR_SHEET = os.getenv("OSCAR_SHEET")
TEVIN_EMAIL = os.getenv("TEVIN_EMAIL")
DAVID_EMAIL = os.getenv("DAVID_EMAIL")
OSCAR_EMAIL = os.getenv("OSCAR_EMAIL")


def get_last_processed_date():
    """Retrieve the last processed date from the config file in S3."""
    s3_client = boto3.client('s3')
    config_key = "config.json"
    try:
        response = s3_client.get_object(Bucket=CONFIG_S3_BUCKET, Key=config_key)
        config_data = json.loads(response['Body'].read().decode('utf-8'))
        return config_data.get("last_processed_date", "2000-01-01")
    except Exception as e:
        print(f"Error fetching last processed date: {e}")
        return "2000-01-01"


def update_last_processed_date(new_date):
    """Update the last processed date in the config file stored in S3."""
    s3_client = boto3.client('s3')
    config_key = "config.json"
    new_config = json.dumps({"last_processed_date": new_date})
    try:
        s3_client.put_object(Bucket=CONFIG_S3_BUCKET, Key=config_key, Body=new_config)
        print(f"Updated last processed date to: {new_date} in S3.")
    except Exception as e:
        print(f"Error updating last processed date: {e}")


def send_email(attachment_data, attachment_filename, recipient_email):
    """Sends an email with the processed IF Prep Sheet attached."""
    msg = EmailMessage()
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = recipient_email
    msg['Subject'] = "Processed Instant Fulfillment Sheet"
    msg.set_content("Attached is the updated IF Prep Sheet.")

    try:
        # Attach file correctly with proper MIME type
        msg.add_attachment(
            attachment_data.encode("utf-8"),
            maintype="text",
            subtype="csv",
            filename=attachment_filename
        )
    except Exception as e:
        print(f"Failed to add attachment: {e}")
        return

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.send_message(msg)
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")


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


def start_conversion(leads_df, recipient_email):
    """
    Converts the leads sheet to match the Instant Fulfillment template,
    filtering by last processed date and sending the result via email.
    
    Returns:
        str  -- a string containing the latest processed date (e.g. "2025-01-01") 
                if new data was processed
        None -- if no new data exists or if an error occurs
    """
    print("Starting conversion...")

    try:
        # Get last processed date
        last_processed_date = get_last_processed_date()
        leads_df["Date"] = pd.to_datetime(leads_df["Date"], errors="coerce")
        
        # Find all dates after last_processed_date
        mask = leads_df["Date"] > pd.to_datetime(last_processed_date)
        filtered_dates = leads_df.loc[mask, "Date"]

        # If there is no new data, return None
        if filtered_dates.empty:
            print("No new data to process.")
            return None

        # Find the earliest date after last_processed_date
        earliest_date = filtered_dates.min()
        
        # Filter to include all rows from earliest_date onward
        leads_df = leads_df[leads_df["Date"] >= earliest_date]

        # Define the headers we want to use manually
        REQUIRED_HEADERS = [
            "Order Date", "Supplier / Retailer", "Item Name / Description",
            "Size / Color", "Bundled?", "# Units in Bundle", "# Units Expected",
            "ASIN", "COGS", "Requested List Price", "Seller Notes / Prep Request",
            "Tracking #", "Custom MSKU", "Order #", "UPC #", "FBA or FBM"
        ]

        output_data = []

        for _, row in leads_df.iterrows():
            # Format the date to "YYYY-MM-DD"
            date_value = row.get("Date", "")
            if pd.isnull(date_value):
                date_str = ""
            else:
                date_str = date_value.strftime("%Y-%m-%d")

            # Remove $ symbols and handle non-numeric sale prices
            sale_price_str = str(row.get("Sale Price", "0")).replace("$", "").replace(",", "").strip()
            try:
                sale_price = float(sale_price_str)
                requested_price = round(sale_price * 1.15, 2)
            except ValueError:
                requested_price = ""  # If Sale Price is not a valid number, set to empty

            # Ensure "Prep Notes" is empty if it is null
            prep_notes = row.get("Prep Notes", "")
            if pd.isna(prep_notes):
                prep_notes = ""

            mapped_row = {
                "Order Date": date_str,  # Use formatted date here
                "Supplier / Retailer": "N/A",
                "Item Name / Description": str(row.get("Name", "")),
                "Size / Color": str(row.get("Size/Color", "N/A")),
                "Bundled?": "Yes" if pd.notna(row.get("Bundled?")) and str(row.get("Bundled?")).strip() != "" else "No",
                "# Units in Bundle": str(row.get("Bundled?", "")) if pd.notna(row.get("Bundled?")) else "",
                "# Units Expected": str(row.get("Amount Purchased", "")),
                "ASIN": str(row.get("ASIN", "")),
                "COGS": str(row.get("COGS", "")),
                "Requested List Price": "Replen" if "REPLEN" in sale_price_str.upper() else str(requested_price),
                "Seller Notes / Prep Request": prep_notes,
                "Tracking #": "",
                "Custom MSKU": "",
                "Order #": str(row.get("Order #", "")),
                "UPC #": "",
                "FBA or FBM": "FBA"
            }
            output_data.append(mapped_row)

        # Convert to DataFrame using our required headers
        output_df = pd.DataFrame(output_data)
        output_df = output_df.reindex(columns=REQUIRED_HEADERS, fill_value="")
        output_df = output_df.astype(str)

        # Write the DataFrame to a CSV in memory
        csv_buffer = StringIO()
        output_df.to_csv(
            csv_buffer,
            index=False,
            header=True,
            quoting=csv.QUOTE_ALL
        )
        
        # Send email with the CSV attachment
        send_email(csv_buffer.getvalue(), "IF_Prep_Sheet.csv", recipient_email)

        # Return the latest date found in the processed data
        latest_date = str(leads_df["Date"].max().date())
        print("Conversion process complete.")
        return latest_date

    except Exception as e:
        print(f"Error during conversion: {e}")
        # Return None to indicate failure or no date
        return None


def lambda_handler(event, context):
    """AWS Lambda Entry Point."""
    try:
        last_processed_dates = []
        
        # Process Tevin's sheet
        leads_df1 = fetch_google_sheet(TEVIN_SHEET)
        tevin_latest = start_conversion(leads_df1, TEVIN_EMAIL)
        if tevin_latest:
            # Only append if we actually got a date (string)
            last_processed_dates.append(pd.to_datetime(tevin_latest))

        # Process David's sheet
        leads_df2 = fetch_google_sheet(DAVID_SHEET)
        david_latest = start_conversion(leads_df2, DAVID_EMAIL)
        if david_latest:
            last_processed_dates.append(pd.to_datetime(david_latest))

        # Process Oscar's sheet
        leads_df3 = fetch_google_sheet(OSCAR_SHEET)
        oscar_latest = start_conversion(leads_df3, OSCAR_EMAIL)
        if oscar_latest:
            last_processed_dates.append(pd.to_datetime(oscar_latest))

        # Update config only once with the latest date from all sheets
        if last_processed_dates:
            new_last_date = max(last_processed_dates).strftime('%Y-%m-%d')
            update_last_processed_date(new_last_date)
            print(f"Final last processed date updated to: {new_last_date}")
        else:
            print("No new dates processed from any sheet.")

        return {"statusCode": 200, "body": "Process completed for all sheets."}

    except Exception as e:
        print(f"Error in Lambda function: {e}")
        return {"statusCode": 500, "body": f"Error: {str(e)}"}
