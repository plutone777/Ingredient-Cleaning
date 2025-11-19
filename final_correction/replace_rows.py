import pandas as pd

# -----------------------------
# File paths
# -----------------------------
corrected_file = r"C:\Users\chooi\Downloads\Ingredient Cleaning\excel files\Two Word Rows Corrected.xlsx"
original_file = r"C:\Users\chooi\Downloads\All Rows Cleaned - Updated.xlsx"
output_file = r"C:\Users\chooi\Downloads\Ingredient Cleaning\All Rows Cleaned Final.xlsx"

# Load both files
corrected_df = pd.read_excel(corrected_file)
original_df = pd.read_excel(original_file)

# Ensure index column is numeric
corrected_df['index'] = pd.to_numeric(corrected_df['index'], errors='coerce')
original_df['index'] = pd.to_numeric(original_df['index'], errors='coerce')

corrected_df = corrected_df.set_index('index')
original_df = original_df.set_index('index')

# Only replace these columns
columns_to_replace = [col for col in ["ingredients", "output", "changelog"] if col in original_df.columns]

print(f"Rows in corrected file: {len(corrected_df)}")

# -----------------------------
# Count replaced rows
# -----------------------------
replaced_count = 0

# Loop through corrected rows and compare before/after
for idx, corrected_row in corrected_df[columns_to_replace].iterrows():
    if idx not in original_df.index:
        continue  # skip missing

    original_row = original_df.loc[idx, columns_to_replace]

    # Check if anything is different
    if not original_row.equals(corrected_row):
        replaced_count += 1

# -----------------------------
# Apply updates
# -----------------------------
original_df.update(corrected_df[columns_to_replace])

# Save updated file
original_df.reset_index().to_excel(output_file, index=False)

print(f"Updated file saved to: {output_file}")
print(f"Number of rows replaced: {replaced_count}")
