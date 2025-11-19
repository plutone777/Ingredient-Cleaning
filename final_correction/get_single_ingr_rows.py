import pandas as pd
import re
from collections import defaultdict

# Input and output Excel paths
input_file = r"C:\Users\chooi\Downloads\Ingredient Cleaning\All Rows Cleaned.xlsx"
fuzzy_summary_file = "All Single Ingr Rows - Fuzzy Corrections.xlsx"
fuzzy_full_rows_file = "All Single Ingr Rows - Full Rows.xlsx"

# Read the Excel file
df = pd.read_excel(input_file)

# Dictionary to collect unique fuzzy corrections and their indices
fuzzy_dict = defaultdict(list)

# Set to collect indices of rows that have single-word fuzzy corrections
rows_with_fuzzy = set()

# Iterate over each row
for idx, row in df.iterrows():
    changelog = row.get('changelog', '')
    if not isinstance(changelog, str):
        continue
    
    # Find all one-word fuzzy corrections with %
    matches = re.findall(r'-\s*([\w-]+)\s*→\s*([\w-]+)\s*\((\d{1,3})%\)', changelog)
    
    # Add each match to the dictionary with the row index + 2
    for original, corrected, similarity in matches:
        fuzzy_dict[(original, corrected, int(similarity))].append(idx + 2)
        # Add the original row index to the set
        rows_with_fuzzy.add(idx)

# ----------------------------------------
# Part 1: Prepare fuzzy corrections summary
# ----------------------------------------
exploded_rows = []
for (original, corrected, similarity), indices in fuzzy_dict.items():
    exploded_rows.append({
        'original': original,
        'corrected': corrected,
        'similarity_percent': similarity,
        'original_indexes': ','.join(map(str, indices))  # comma-separated list of indices
    })

fuzzy_summary_df = pd.DataFrame(exploded_rows)
fuzzy_summary_df.to_excel(fuzzy_summary_file, index=False)
print(f"Extracted {len(fuzzy_summary_df)} unique fuzzy corrections. Saved to '{fuzzy_summary_file}'.")

# ----------------------------------------
# Part 2: Save full original rows with fuzzy single-word corrections
# ----------------------------------------
fuzzy_full_rows_df = df.loc[list(rows_with_fuzzy)]  # <-- convert set to list
fuzzy_full_rows_df.to_excel(fuzzy_full_rows_file, index=False)
print(f"Saved {len(fuzzy_full_rows_df)} full rows with fuzzy single-word corrections to '{fuzzy_full_rows_file}'.")

