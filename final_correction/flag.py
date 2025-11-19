import pandas as pd
import nltk
from nltk.corpus import words

nltk.download('words')

english_words = set(words.words())

df = pd.read_excel(r"C:\Users\chooi\Downloads\Ingredient Cleaning\All Two-Word Original - Fuzzy Corrections.xlsx")

def flag_two_english(original):
    if not isinstance(original, str):
        return ""
    parts = original.lower().split()
    # Check if exactly 2 words and both are in English dictionary
    if len(parts) == 2 and all(word in english_words for word in parts):
        return 1
    return ""

df['flag'] = df['original'].apply(flag_two_english)

df.to_excel("flagged_two_word_english.xlsx", index=False)

print("Done! 'Flag' column added for two-word English originals.")
