import json
import os
import sys
import pandas as pd
import requests
import smtplib
import ssl
from email.message import EmailMessage
from dotenv import load_dotenv
import boto3
from io import StringIO
import csv
from datetime import datetime
from zoneinfo import ZoneInfo

# Load environment variables
load_dotenv()

EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
CONFIG_S3_BUCKET = os.getenv("CONFIG_S3_BUCKET")

# Key for your new user config file in S3
USERS_CONFIG_KEY = "users.json"

# Define a Tee class to duplicate stdout writes to multiple streams
class Tee:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for stream in self.streams:
            stream.write(data)

    def flush(self):
        for stream in self.streams:
            stream.flush()

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
        print(f"Error updating last_processed_date: {e}")

def send_error_email(error_message):
    """Sends an email notification if the script encounters an error."""
    msg = EmailMessage()
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = EMAIL_ADDRESS  
    msg['Subject'] = "Script Error Notification"
    msg.set_content(f"An error occurred while running the script:\n\n{error_message}")
    
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.send_message(msg)
        print("Error email sent successfully.")
    except Exception as e:
        print(f"Failed to send error email: {e}")

def send_email(attachment_data, attachment_filename, recipient_email):
    """Sends an email with the processed IF Prep Sheet attached."""
    msg = EmailMessage()
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = recipient_email
    msg['Subject'] = "Processed Instant Fulfillment Sheet"
    msg.set_content("Attached is the updated IF Prep Sheet.")

    try:
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

def send_notification_email(recipient_email, subject, message):
    """Sends a notification email to the user."""
    msg = EmailMessage()
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.set_content(message)
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.send_message(msg)
        print(f"Notification email sent to {recipient_email}.")
    except Exception as e:
        print(f"Failed to send notification email to {recipient_email}: {e}")

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
                if new data was processed, or
        None -- if no new data exists or if an error occurs
    """
    print("Starting conversion...")

    try:
        last_processed_date = get_last_processed_date()
        leads_df["Date"] = pd.to_datetime(leads_df["Date"], errors="coerce")
        
        # Filter rows based on the last processed date
        mask = leads_df["Date"] >= pd.to_datetime(last_processed_date)
        filtered_dates = leads_df.loc[mask, "Date"]

        if filtered_dates.empty:
            print("No new data to process.")
            send_notification_email(
                recipient_email,
                "No new purchases to process",
                "There are no new purchases to process."
            )
            return None

        earliest_date = filtered_dates.min()
        leads_df = leads_df[leads_df["Date"] >= earliest_date]

        REQUIRED_HEADERS = [
            "Order Date", "Supplier / Retailer", "Item Name / Description",
            "Size / Color", "Bundled?", "# Units in Bundle", "# Units Expected",
            "ASIN", "COGS", "Requested List Price", "Seller Notes / Prep Request",
            "Tracking #", "Custom MSKU", "Order #", "UPC #", "FBA or FBM"
        ]

        output_data = []

        for _, row in leads_df.iterrows():
            date_value = row.get("Date", "")
            date_str = "" if pd.isnull(date_value) else date_value.strftime("%Y-%m-%d")

            sale_price_str = str(row.get("Sale Price", "0")).replace("$", "").replace(",", "").strip()
            try:
                sale_price = float(sale_price_str)
                requested_price = round(sale_price * 1.15, 2)
            except ValueError:
                requested_price = ""

            prep_notes = row.get("Prep Notes", "")
            if pd.isna(prep_notes):
                prep_notes = ""

            mapped_row = {
                "Order Date": date_str,
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

        output_df = pd.DataFrame(output_data)
        output_df = output_df.reindex(columns=REQUIRED_HEADERS, fill_value="")
        output_df = output_df.astype(str)

        csv_buffer = StringIO()
        output_df.to_csv(csv_buffer, index=False, header=True, quoting=csv.QUOTE_ALL)
        send_email(csv_buffer.getvalue(), "IF_Prep_Sheet.csv", recipient_email)

        latest_date = str(leads_df["Date"].max().date())
        print("Conversion process complete.")
        return latest_date

    except Exception as e:
        print(f"Error during conversion: {e}")
        return None

def get_users_config():
    """Fetches the user configuration (sheet links and emails) from S3."""
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=CONFIG_S3_BUCKET, Key=USERS_CONFIG_KEY)
        config_data = json.loads(response['Body'].read().decode('utf-8'))
        return config_data.get("users", [])
    except Exception as e:
        print(f"Error fetching users config: {e}")
        return []

def lambda_handler(event, context):
    """AWS Lambda Entry Point."""
    # Set up log capturing by overriding sys.stdout
    original_stdout = sys.stdout
    log_buffer = StringIO()
    sys.stdout = Tee(original_stdout, log_buffer)

    try:
        # 1) Fetch all user records
        users = get_users_config()
        if not users:
            print("No user configurations found.")
        
        # 2) Loop over each user record
        for user in users:
            sheet_link = user.get("sheet")
            recipient_email = user.get("email")

            # 3) Skip any record missing sheet or email
            if not sheet_link or not recipient_email:
                print(f"Skipping user record due to missing sheet or email: {user}")
                continue

            print(f"Processing sheet for: {recipient_email}")
            leads_df = fetch_google_sheet(sheet_link)
            start_conversion(leads_df, recipient_email)
        
        current_date = datetime.now(ZoneInfo("America/New_York")).strftime('%Y-%m-%d')
        print(current_date)
        update_last_processed_date(current_date)
        print(f"Final last processed date updated to: {current_date}")

        return {"statusCode": 200, "body": "Process completed for all sheets."}

    except Exception as e:
        # Include the console log in the error email
        error_message = f"Error in Lambda function: {str(e)}\n\nLogs:\n{log_buffer.getvalue()}"
        print(error_message)
        send_error_email(error_message)
        return {"statusCode": 500, "body": error_message}

    finally:
        # Restore original stdout
        sys.stdout = original_stdout
