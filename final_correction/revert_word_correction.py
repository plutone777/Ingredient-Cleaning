import pandas as pd
import re
from spelling import tokenize_ingredients, fix_parentheses_commas  

# -----------------------------
# Load files
# -----------------------------
mapping_file = r"C:\Users\chooi\Downloads\Two Word with Flags.xlsx"
target_file = r"C:\Users\chooi\Downloads\Ingredient Cleaning\All Two-Word Original - Full Rows.xlsx"
output_file = r"C:\Users\chooi\Downloads\Ingredient Cleaning\Two Word Rows Corrected.xlsx"

mapping_df = pd.read_excel(mapping_file)
target_df = pd.read_excel(target_file)

# -----------------------------
# Robust flag handling
# -----------------------------
# Convert flag to numeric, coerce errors (handles 1, '1', 1.0, etc.)
mapping_df['flag_numeric'] = pd.to_numeric(mapping_df['flag'], errors='coerce')
wrong_corrections = mapping_df[mapping_df['flag_numeric'] == 1]

print(f"Number of flagged rows: {len(wrong_corrections)}")
if len(wrong_corrections) == 0:
    print("No flagged rows found. Please check your 'flag' column in the mapping file.")

# -----------------------------
# Replacement using helper functions
# -----------------------------
for _, row in wrong_corrections.iterrows():
    corrected_term = str(row['corrected']).strip().lower()
    original_term = str(row['original']).strip().lower()
    replacement = str(row['real_correction']).strip() if pd.notna(row['real_correction']) else original_term

    # Parse original_indexes
    indexes_str = str(row['original_indexes'])
    try:
        indexes = [int(x) for x in re.split(r',\s*', indexes_str)]
    except Exception as e:
        print(f"Skipping row with invalid original_indexes: {indexes_str}")
        continue

    print(f"\nProcessing corrected='{corrected_term}' replacement='{replacement}' for indexes: {indexes}")

    for idx in indexes:
        # Match using the 'index' column in target_df
        row_mask = target_df['index'] == idx
        if not row_mask.any():
            print(f"  Skipping index {idx}: not found in target_df['index']")
            continue

        row_pos = target_df.index[row_mask][0]  # positional index
        output_text = str(target_df.at[row_pos, 'output'])
        print(f"  Original output (idx={idx}): {output_text}")

        # Tokenize using your helper
        tokens = tokenize_ingredients(output_text)
        print(f"  Tokens: {tokens}")

        # Replace only exact matches
        replaced = False
        updated_tokens = []
        for t in tokens:
            if t.strip().lower() == corrected_term:
                updated_tokens.append(replacement)
                replaced = True
            else:
                updated_tokens.append(t)

        if replaced:
            print(f"  Replaced '{corrected_term}' with '{replacement}'")
        else:
            print(f"  No match found for '{corrected_term}' in tokens")

        # Reassemble and fix commas/parentheses
        updated_output = fix_parentheses_commas(", ".join(updated_tokens))
        target_df.at[row_pos, 'output'] = updated_output

        # Update changelog
        log_entry = f"- {corrected_term} → {replacement} (CORRECTED)"
        if pd.isna(target_df.at[row_pos, 'changelog']):
            target_df.at[row_pos, 'changelog'] = log_entry
        else:
            target_df.at[row_pos, 'changelog'] += f"\n{log_entry}"  # append with newline

# -----------------------------
# Save corrected file
# -----------------------------
target_df.to_excel(output_file, index=False)
print(f"\nApplied corrections and saved updated output to '{output_file}'")
