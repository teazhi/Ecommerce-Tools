import pandas as pd

# File paths
aura_file = 'aura.csv'  # Replace with the correct path if needed
sb_file = 'sb.xlsx'     # Replace with the correct path if needed
output_file = 'aura_new.csv'

# Load aura.csv and sb.xlsx
aura_df = pd.read_csv(aura_file)
sb_df = pd.read_excel(sb_file, engine='openpyxl')

# Normalize ASIN and SKU columns for matching
aura_df['asin'] = aura_df['asin'].astype(str).str.strip()
aura_df['sku'] = aura_df['sku'].astype(str).str.strip()
sb_df['ASIN'] = sb_df['ASIN'].astype(str).str.strip()
sb_df['SKU'] = sb_df['SKU'].astype(str).str.strip()

# Update costs for rows where fulfillment_type is 'fba'
for index, row in aura_df.iterrows():
    if str(row.get('fulfillment_type', '')).strip().lower() == 'fba':
        match = sb_df[(sb_df['ASIN'] == row['asin']) & (sb_df['SKU'] == row['sku'])]
        if not match.empty:
            aura_df.at[index, 'cost'] = match.iloc[0]['Cost']

# Save the updated DataFrame to a new CSV file
aura_df.to_csv(output_file, index=False)

print(f"Updated file saved as {output_file}")