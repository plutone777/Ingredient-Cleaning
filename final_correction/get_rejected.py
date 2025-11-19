import pandas as pd
from collections import defaultdict

# Input and output Excel paths
input_file = r"C:\Users\chooi\Downloads\Ingredient Cleaning\All Rows Cleaned.xlsx"
summary_file = "All Rejected Summary.xlsx"
full_rows_file = "All Rejected Rows.xlsx"

# Read the Excel file
df = pd.read_excel(input_file)

# Dictionary to collect unique rejected corrections and their row indices
rejected_dict = defaultdict(list)

# Set to collect row indices that contain rejected corrections
rows_with_rejected = set()

# Iterate over each row
for idx, row in df.iterrows():
    changelog = row.get('changelog', '')
    if not isinstance(changelog, str):
        continue

    # Check each line in changelog
    for line in changelog.splitlines():
        if "(REJECTED)" in line.upper():
            # Split original and corrected by '→'
            if "→" in line:
                parts = line.split("→")
                original = parts[0].strip("- ").strip()
                corrected = parts[1].split("(REJECTED)")[0].strip()
            else:
                original = line
                corrected = ""

            # Store in dictionary
            rejected_dict[(original, corrected)].append(idx + 2)
            rows_with_rejected.add(idx)

# ----------------------------------------
# Part 1: Prepare rejected summary
# ----------------------------------------
exploded_rows = []
for (original, corrected), indices in rejected_dict.items():
    exploded_rows.append({
        'original': original,
        'corrected': corrected,
        'original_indexes': ','.join(map(str, indices))
    })

rejected_summary_df = pd.DataFrame(exploded_rows)
rejected_summary_df.to_excel(summary_file, index=False)
print(f"Extracted {len(rejected_summary_df)} unique rejected corrections. Saved to '{summary_file}'.")

# ----------------------------------------
# Part 2: Save full rows with rejected corrections
# ----------------------------------------
rejected_full_rows_df = df.loc[list(rows_with_rejected)]
rejected_full_rows_df.to_excel(full_rows_file, index=False)
print(f"Saved {len(rejected_full_rows_df)} full rows with rejected corrections to '{full_rows_file}'.")
