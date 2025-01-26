import pandas as pd

# File paths
sb_file = 'sb.xlsx'
il_file = 'il.csv'
output_updated_il_file = 'il_updated.csv'

# Load sb.xlsx and il.csv
sb_df = pd.read_excel(sb_file, engine='openpyxl')
il_df = pd.read_csv(il_file)

# Normalize columns for matching
sb_df['ASIN'] = sb_df['ASIN'].astype(str).str.strip()
sb_df['SKU'] = sb_df['SKU'].astype(str).str.strip()
il_df['ASIN'] = il_df['ASIN'].astype(str).str.strip()
il_df['MSKU'] = il_df['MSKU'].astype(str).str.strip()

# Ensure the 'cost' column in il_df has a compatible dtype
if 'Active Cost/Unit' in il_df.columns:
    il_df['Active Cost/Unit'] = pd.to_numeric(il_df['Active Cost/Unit'], errors='coerce')

# Update costs in il.csv using sb.xlsx values
for index, row in il_df.iterrows():
    match = sb_df[(sb_df['ASIN'] == row['ASIN']) & (sb_df['SKU'] == row['MSKU'])]
    if not match.empty:
        il_df.at[index, 'Active Cost/Unit'] = float(match.iloc[0]['Cost'])  # Explicitly cast to float

# Save the updated DataFrame to a new CSV file
il_df.to_csv(output_updated_il_file, index=False)
print(f"Updated file saved as {output_updated_il_file}")