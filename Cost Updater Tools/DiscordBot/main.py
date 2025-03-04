import os
import certifi
import json
import requests
import pandas as pd
from io import StringIO, BytesIO

import boto3
import discord
from discord.ext import commands
from discord import app_commands
from discord.ui import Select, View
from dotenv import load_dotenv

# Point to the certifi certificate bundle (useful on macOS)
os.environ['SSL_CERT_FILE'] = certifi.where()

# Load environment variables
load_dotenv()

# AWS and Discord configuration from environment
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
CONFIG_S3_BUCKET = os.getenv("CONFIG_S3_BUCKET")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

# Ensure AWS credentials are provided
if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    raise Exception("AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in your .env file.")

# Set up Discord bot
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

###########################################
# S3 Uploader Functionality (for /upload)  #
###########################################

def upload_file_to_s3(file_bytes, file_name):
    """
    Uploads file bytes to AWS S3 with the specified file name.
    """
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    try:
        s3_client.put_object(Bucket=CONFIG_S3_BUCKET, Key=file_name, Body=file_bytes)
        return f"Successfully uploaded '{file_name}' to bucket '{CONFIG_S3_BUCKET}'."
    except Exception as e:
        return f"Error uploading '{file_name}' to S3: {e}"

###########################################
# Dropdown UI for Column Mapping         #
###########################################

class ColumnSelect(Select):
    def __init__(self, mapping_type: str, options_list: list):
        self.mapping_type = mapping_type
        options = [discord.SelectOption(label=col, value=col) for col in options_list]
        super().__init__(
            placeholder=f"Select column for {mapping_type}",
            min_values=1,
            max_values=1,
            options=options
        )

    async def callback(self, interaction: discord.Interaction):
        self.view.mapping_result[self.mapping_type] = self.values[0]
        await interaction.response.send_message(
            f"Selected **{self.values[0]}** for **{self.mapping_type}**.", ephemeral=True
        )
        if len(self.view.mapping_result) >= len(self.view.missing_types):
            self.view.stop()

class MappingView(View):
    def __init__(self, missing_types: list, options: list):
        super().__init__(timeout=60)
        self.missing_types = missing_types
        self.mapping_result = {}
        for mapping_type in missing_types:
            self.add_item(ColumnSelect(mapping_type, options))

###########################################
# Slash Commands                          #
###########################################

@bot.tree.command(name="upload", description="Upload a file to S3")
@app_commands.describe(file="The file to upload")
async def slash_upload(interaction: discord.Interaction, file: discord.Attachment):
    """
    Slash command to upload a file to S3.
    The result is sent to the executor via DM, with an ephemeral notification in the channel.
    """
    try:
        file_bytes = await file.read()
        result = upload_file_to_s3(file_bytes, file.filename)
        embed = discord.Embed(
            title="S3 Upload Result",
            description=result,
            color=discord.Color.blue()
        )
        await interaction.response.send_message(embed=embed)
    except Exception as e:
        await interaction.response.send_message(f"Error processing the file: {e}", ephemeral=True)

@bot.tree.command(name="updateaura", description="Update aura CSV with cost data from a Google Sheet")
@app_commands.describe(
    aura_file="The aura CSV file to update",
    google_sheet_url="Optional: Google Sheet URL containing cost data"
)
async def slash_updateaura(interaction: discord.Interaction, aura_file: discord.Attachment, google_sheet_url: str = None):
    """
    Slash command to update an aura CSV file using cost data from a Google Sheet.
    The updated file and a summary are sent to the executor via DM.
    """
    await interaction.response.defer()
    try:
        aura_bytes = await aura_file.read()
        aura_df = pd.read_csv(StringIO(aura_bytes.decode('utf-8')))
    except Exception as e:
        await interaction.followup.send(f"Error reading aura CSV file: {e}", ephemeral=True)
        return

    if not google_sheet_url:
        if os.path.exists("config.json"):
            with open("config.json", "r") as f:
                config = json.load(f)
            google_sheet_url = config.get("google_sheet_url", "").strip()
        if not google_sheet_url:
            await interaction.followup.send("Google Sheet URL not provided and no config.json found.", ephemeral=True)
            return

    try:
        response = requests.get(google_sheet_url)
        response.raise_for_status()
        csv_data = StringIO(response.text)
        sheet_df = pd.read_csv(csv_data, dtype=str)
    except Exception as e:
        await interaction.followup.send(f"Error fetching Google Sheet data: {e}", ephemeral=True)
        return

    # Auto-detect mapping for ASIN and COGS (case-insensitive)
    sheet_columns = list(sheet_df.columns)
    columns_lower = {col.lower(): col for col in sheet_columns}
    mapping = {}
    missing = []
    if "asin" in columns_lower:
        mapping["ASIN"] = columns_lower["asin"]
    else:
        missing.append("ASIN")
    if "cogs" in columns_lower:
        mapping["COGS"] = columns_lower["cogs"]
    else:
        missing.append("COGS")

    if missing:
        view = MappingView(missing, sheet_columns)
        prompt_msg = f"Please select the correct column for **{', '.join(missing)}** from the Google Sheet:"
        await interaction.followup.send(prompt_msg, view=view, ephemeral=True)
        await view.wait()
        if not view.mapping_result or len(view.mapping_result) < len(missing):
            await interaction.followup.send("Column mapping not completed in time. Please try the command again.", ephemeral=True)
            return
        mapping.update(view.mapping_result)

    try:
        sheet_df = sheet_df.rename(columns={mapping["ASIN"]: "ASIN", mapping["COGS"]: "COGS"})
        sheet_df["ASIN"] = sheet_df["ASIN"].astype(str).str.strip()
        sheet_df["COGS"] = (
            sheet_df["COGS"]
            .astype(str)
            .str.replace('$', '', regex=False)
            .str.replace(',', '', regex=False)
        )
        sheet_df["COGS"] = pd.to_numeric(sheet_df["COGS"], errors='coerce')
    except Exception as e:
        await interaction.followup.send(f"Error processing Google Sheet data: {e}", ephemeral=True)
        return

    if "asin" not in aura_df.columns or "cost" not in aura_df.columns:
        await interaction.followup.send("The aura CSV file must have both 'asin' and 'cost' columns.", ephemeral=True)
        return
    aura_df["asin"] = aura_df["asin"].astype(str).str.strip()
    aura_df["cost"] = pd.to_numeric(aura_df["cost"], errors='coerce')

    updated_rows = []
    for idx, row in aura_df.iterrows():
        if pd.isna(row["cost"]):
            match = sheet_df[sheet_df["ASIN"] == row["asin"]]
            if not match.empty:
                new_cost = match.iloc[0]["COGS"]
                if not pd.isna(new_cost):
                    aura_df.at[idx, "cost"] = new_cost
                    updated_rows.append({
                        "index": idx,
                        "asin": row["asin"],
                        "old_cost": row["cost"],
                        "new_cost": new_cost
                    })

    output_buffer = StringIO()
    aura_df.to_csv(output_buffer, index=False)
    output_bytes = output_buffer.getvalue().encode('utf-8')

    embed = discord.Embed(
        title="Aura CSV Update Summary",
        description=f"Updated **{len(updated_rows)}** row(s) in the aura CSV file.",
        color=discord.Color.green()
    )
    file = discord.File(fp=BytesIO(output_bytes), filename="aura_updated.csv")
    await interaction.followup.send(embed=embed, ephemeral=True)
    await interaction.user.send("Aura Updated File", file=discord.File(fp=BytesIO(output_bytes), filename="aura_updated.csv"))

###########################################
# on_ready Event (after command definitions)
###########################################

@bot.event
async def on_ready():
    print(f"Bot is online as {bot.user}")
    guild = discord.Object(id=1287450087852740699)  # Use integer guild ID
    try:
        bot.tree.copy_global_to(guild=guild)
        synced = await bot.tree.sync(guild=guild)
        print(f"Synced {len(synced)} command(s) to guild {guild.id}.")
    except Exception as e:
        print(e)

bot.run(DISCORD_TOKEN)
