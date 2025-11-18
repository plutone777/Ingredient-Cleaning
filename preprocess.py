import pandas as pd
import re
import os
import csv

def get_TAS(raw_file, column_raw, output_csv):
    if os.path.exists(output_csv):
        print(f"File already exists: {output_csv}. Skipping.")
        return pd.read_csv(output_csv)

    df_TAS = pd.read_excel(raw_file)
    df_TAS = df_TAS[[column_raw]].dropna(subset=[column_raw])
    df_TAS.columns = ["Ingredients"]

    df_TAS["Ingredients"] = df_TAS["Ingredients"].astype(str).str.strip().str.rstrip('.')

    df_TAS.index = df_TAS.index + 2

    df_TAS.to_csv(
        output_csv,
        index=True,
        index_label="Index",
        header=True,
        quoting=csv.QUOTE_ALL)
    print(f"Saved raw ingredient data to: {output_csv}")
    return df_TAS


def find_last_punct_outside_parens(text, end_idx):
    paren_depth = 0
    for i in range(end_idx-1, -1, -1):
        if text[i] == ')':
            paren_depth += 1
        elif text[i] == '(':
            paren_depth -= 1
        elif paren_depth == 0 and text[i] in [',', ';', '.']:
            return i
    return -1


# def remove_phrases_before_colon(text):
    removed_parts = []

    while ':' in text:
        colon_idx = text.find(':')
        last_punct_idx = find_last_punct_outside_parens(text, colon_idx)

        if last_punct_idx == -1:
            removed_parts.append(text[:colon_idx + 1].strip())
            text = text[colon_idx + 1:].lstrip()
        else:
            removed_parts.append(text[last_punct_idx + 1:colon_idx + 1].strip())
            text = text[:last_punct_idx + 1] + text[colon_idx + 1:]

    cleaned_text = re.sub(r'\s{2,}', ' ', text).strip()
    removed_text = ", ".join(removed_parts)
    return cleaned_text, removed_text


#=========================================================================================================#

get_TAS(
    raw_file=r"C:\Users\chooi\Downloads\Ingredient and TFI Column for Mariyam.xlsx",
    column_raw="Ingredient (TAS)",
    output_csv="Raws2.csv"
)
