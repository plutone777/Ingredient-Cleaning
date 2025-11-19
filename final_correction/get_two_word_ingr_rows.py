import pandas as pd
import re
from collections import defaultdict

# Input and output Excel paths
input_file = r"C:\Users\chooi\Downloads\All Rows Cleaned - Updated.xlsx"
fuzzy_summary_file = "All Two-Word Original - Fuzzy Corrections.xlsx"
fuzzy_full_rows_file = "All Two-Word Original - Full Rows.xlsx"

# Read the Excel file
df = pd.read_excel(input_file)

# Dictionary to collect unique fuzzy corrections and their indices
fuzzy_dict = defaultdict(list)

# Set to collect indices of rows that have fuzzy corrections
rows_with_fuzzy = set()

# Iterate over each row
for idx, row in df.iterrows():
    changelog = row.get('changelog', '')
    if not isinstance(changelog, str):
        continue
    
    # Match corrections with similarity %
    matches = re.findall(r'-\s*([\w\s&-]+?)\s*→\s*([\w\s&-]+?)\s*\((\d{1,3})%\)', changelog)
    
    # Keep only if original term has exactly two words
    for original, corrected, similarity in matches:
        if len(original.strip().split()) == 2:
            fuzzy_dict[(original.strip().lower(), corrected.strip().lower(), int(similarity))].append(idx + 2)
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
        'original_indexes': ','.join(map(str, indices))
    })

fuzzy_summary_df = pd.DataFrame(exploded_rows)
fuzzy_summary_df.to_excel(fuzzy_summary_file, index=False)
print(f"Extracted {len(fuzzy_summary_df)} unique fuzzy corrections with two-word originals. Saved to '{fuzzy_summary_file}'.")

# ----------------------------------------
# Part 2: Save full original rows with fuzzy corrections
# ----------------------------------------
fuzzy_full_rows_df = df.loc[list(rows_with_fuzzy)]
fuzzy_full_rows_df.to_excel(fuzzy_full_rows_file, index=False)
print(f"Saved {len(fuzzy_full_rows_df)} full rows with fuzzy corrections to '{fuzzy_full_rows_file}'.")
