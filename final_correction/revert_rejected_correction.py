import pandas as pd
import re
from spelling import tokenize_ingredients, fix_parentheses_commas  # your helper functions

# -----------------------------
# Load files
# -----------------------------
rejected_file = r"C:\Users\chooi\Downloads\Rejected Summary with Flags.xlsx"
all_rows_file = r"C:\Users\chooi\Downloads\Ingredient Cleaning\All Rows Cleaned - Updated.xlsx"
output_file = r"C:\Users\chooi\Downloads\Ingredient Cleaning\Rejected Rows Reverted.xlsx"

rejected_df = pd.read_excel(rejected_file)
all_rows_df = pd.read_excel(all_rows_file)

# -----------------------------
# Robust flag handling
# -----------------------------
# Convert flag to numeric, coerce errors (handles 1, '1', 1.0, etc.)
rejected_df['flag_numeric'] = pd.to_numeric(rejected_df['flag'], errors='coerce')
rows_to_revert = rejected_df[rejected_df['flag_numeric'] == 1]

print(f"Number of flagged rows to revert: {len(rows_to_revert)}")
if len(rows_to_revert) == 0:
    print("No flagged rows found. Please check your 'flag' column in the rejected file.")

# -----------------------------
# Revert changes
# -----------------------------
reverted_count = 0

for _, row in rows_to_revert.iterrows():
    original_term = str(row['original']).strip().lower()
    corrected_term = str(row['corrected']).strip().lower()
    indexes_str = str(row['original_indexes'])
    try:
        indexes = [int(x) for x in re.split(r',\s*', indexes_str)]
    except Exception as e:
        print(f"Skipping row with invalid original_indexes: {indexes_str}")
        continue

    print(f"\nReverting corrected='{corrected_term}' back to original='{original_term}' for indexes: {indexes}")

    for idx in indexes:
        # Match using the 'index' column in all_rows_df
        row_mask = all_rows_df['index'] == idx
        if not row_mask.any():
            print(f"  Skipping index {idx}: not found in all_rows_df['index']")
            continue

        row_pos = all_rows_df.index[row_mask][0]  # positional index
        output_text = str(all_rows_df.at[row_pos, 'output'])
        print(f"  Current output (idx={idx}): {output_text}")

        # Tokenize using your helper
        tokens = tokenize_ingredients(output_text)
        print(f"  Tokens: {tokens}")

        # Revert only exact matches of the corrected term
        reverted = False
        updated_tokens = []
        for t in tokens:
            if t.strip().lower() == corrected_term:
                updated_tokens.append(original_term)
                reverted = True
            else:
                updated_tokens.append(t)

        if reverted:
            print(f"  Reverted '{corrected_term}' to '{original_term}'")
            reverted_count += 1
        else:
            print(f"  No match found for '{corrected_term}' in tokens")

        # Reassemble and fix commas/parentheses
        updated_output = fix_parentheses_commas(", ".join(updated_tokens))
        all_rows_df.at[row_pos, 'output'] = updated_output

        # Update changelog
        log_entry = f"- {corrected_term} → {original_term} (REVERTED)"
        if pd.isna(all_rows_df.at[row_pos, 'changelog']):
            all_rows_df.at[row_pos, 'changelog'] = log_entry
        else:
            all_rows_df.at[row_pos, 'changelog'] += f"\n{log_entry}"  # append with newline

# -----------------------------
# Save reverted file
# -----------------------------
all_rows_df.to_excel(output_file, index=False)
print(f"\nApplied reversions and saved updated output to '{output_file}'")
print(f"Total reverted entries: {reverted_count}")
