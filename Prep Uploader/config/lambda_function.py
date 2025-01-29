import os
import sys
import pandas as pd
import requests
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import formatdate
from dotenv import load_dotenv  # Ensure you have python-dotenv installed
import datetime
import io  # Import io for in-memory file handling

# Load environment variables from .env file
load_dotenv()

# Function to get the current date
def get_current_date():
    """
    Returns the current date in MM/DD/YYYY format.
    Modify the format string as needed.
    """
    return datetime.datetime.now().strftime("%m/%d/%Y")

# Email Configuration
EMAIL_ADDRESS = os.getenv('EMAIL_ADDRESS')  # Sender's email address
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')  # Sender's email password (App Password)
RECEIVER_EMAIL = "tevinz123@gmail.com"  # Replace with your email
SMTP_SERVER = "smtp.gmail.com"  # Gmail SMTP server
SMTP_PORT = 465  # SSL port for Gmail
EMAIL_SUBJECT = f"Processed Instant Fulfillment Template {get_current_date()}"  # Dynamic subject

# HTML email body with clickable link
EMAIL_BODY_HTML = f"""\
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Processed Instant Fulfillment Template</title>
</head>
<body>
    <p>Hello,</p>
    <p>Attached is the updated prep template, ready for upload.</p>
    <p><strong>Steps to upload:</strong></p>
    <ol>
        <li>Go to <a href="https://portal.instant-fulfillment.com/dashboard">Instant Fulfillment Dashboard</a></li>
        <li>Click "Inbound Units" on the left side</li>
        <li>Click "Amazon ASIN"</li>
        <li>Download the file attached to this email and then upload the file in the area on the right side</li>
        <li>Click submit</li>
    </ol>
    <p>Units should be uploaded once you see "Upload Successful" pop up, but to confirm, go to "Unit Tracker" on the left and
    then click "Upload Date" two times to filter by the most recent upload. Here you should see the new inventory you uploaded.</p>
    <p>Best regards,<br>DOT</p>
</body>
</html>
"""

# Plain text email body (for email clients that do not support HTML)
EMAIL_BODY_TEXT = """\
Hello,

Attached is the updated prep template, ready for upload.

Steps to upload:
1) Go to https://portal.instant-fulfillment.com/dashboard
2) Click "Inbound Units" on the left side
3) Click "Amazon ASIN"
4) Download the file attached to this email and then upload the file in the area on the right side
5) Click submit

Units should be uploaded once you see "Upload Successful" pop up, but to confirm, go to "Unit Tracker" on the left and
then click "Upload Date" two times to filter by the most recent upload. Here you should see the new inventory you uploaded.

Best regards,
DOT
"""

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

def send_email(sender_email, sender_password, receiver_email, subject, body_text, body_html, attachment_data, attachment_filename, smtp_server, smtp_port):
    """
    Sends an email with both plain text and HTML content, including an attachment.

    :param sender_email: Sender's email address
    :param sender_password: Sender's email password (App Password)
    :param receiver_email: Receiver's email address
    :param subject: Subject of the email
    :param body_text: Plain text body of the email
    :param body_html: HTML body of the email
    :param attachment_data: The data of the attachment as bytes
    :param attachment_filename: The filename for the attachment
    :param smtp_server: SMTP server address
    :param smtp_port: SMTP server port
    """
    # Create the email message
    msg = EmailMessage()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    msg.set_content(body_text)  # Set the plain text content

    # Add the HTML content as an alternative
    msg.add_alternative(body_html, subtype='html')

    # Add the attachment
    try:
        msg.add_attachment(attachment_data, maintype='application', subtype='octet-stream', filename=attachment_filename)
    except Exception as e:
        print(f"Failed to add attachment: {e}")
        sys.exit(1)

    # Connect to the SMTP server and send the email
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtp_server, smtp_port, context=context) as server:
            server.login(sender_email, sender_password)
            server.send_message(msg)
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")
        sys.exit(1)

def start_conversion(leads_df, prep_sheet_path):
    """
    Converts the leads DataFrame to a format compatible with Instant Fulfillment's import feature.
    Sends the processed data as an email attachment without saving it locally.
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
        total_rows = len(leads_df)
        
        if not marker_rows.empty:
            # Get the first marker row index
            first_marker_idx = marker_rows.index[0]
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
        
        # Convert the DataFrame to CSV in-memory
        csv_buffer = io.StringIO()
        output_df.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode('utf-8')  # Encode to bytes
        
        # Define the attachment filename
        attachment_filename = f"Processed_Instant_Fulfillment_Template_{get_current_date().replace('/', '-')}.csv"
        
        print(f"\nConversion completed successfully. Preparing to send the email with the attachment: {attachment_filename}")
        
        # Send the email with the attachment
        send_email(
            sender_email=EMAIL_ADDRESS,
            sender_password=EMAIL_PASSWORD,
            receiver_email=RECEIVER_EMAIL,
            subject=EMAIL_SUBJECT,
            body_text=EMAIL_BODY_TEXT,
            body_html=EMAIL_BODY_HTML,  # Pass the HTML body
            attachment_data=csv_bytes,
            attachment_filename=attachment_filename,
            smtp_server=SMTP_SERVER,
            smtp_port=SMTP_PORT
        )
    except Exception as e:
        print(f"An error occurred during conversion: {e}")
        sys.exit(1)

def send_email(sender_email, sender_password, receiver_email, subject, body_text, body_html, attachment_data, attachment_filename, smtp_server, smtp_port):
    """
    Sends an email with both plain text and HTML content, including an attachment.

    :param sender_email: Sender's email address
    :param sender_password: Sender's email password (App Password)
    :param receiver_email: Receiver's email address
    :param subject: Subject of the email
    :param body_text: Plain text body of the email
    :param body_html: HTML body of the email
    :param attachment_data: The data of the attachment as bytes
    :param attachment_filename: The filename for the attachment
    :param smtp_server: SMTP server address
    :param smtp_port: SMTP server port
    """
    # Create the email message
    msg = EmailMessage()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    msg.set_content(body_text)  # Set the plain text content

    # Add the HTML content as an alternative
    msg.add_alternative(body_html, subtype='html')

    # Add the attachment
    try:
        msg.add_attachment(attachment_data, maintype='application', subtype='octet-stream', filename=attachment_filename)
    except Exception as e:
        print(f"Failed to add attachment: {e}")
        sys.exit(1)

    # Connect to the SMTP server and send the email
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtp_server, smtp_port, context=context) as server:
            server.login(sender_email, sender_password)
            server.send_message(msg)
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        leads_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vR0V_bwzTiaMDwECn1bEDsyh9m3Wsy7OofffFuyr9zRLIR_E2g3vtwTTHiW9DC0ZUfavnwvQwH0hrQe/pub?gid=0&single=true&output=csv"
        leads_df = fetch_google_sheet(leads_url)
        prep_path = "config/IF_PREP_SHEET.csv"
        if not os.path.exists(prep_path):
            print(f"Prep sheet not found at {prep_path}. Exiting.")
            sys.exit(1)
        start_conversion(leads_df, prep_path)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)