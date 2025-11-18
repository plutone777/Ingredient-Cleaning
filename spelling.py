import pandas as pd
import re
from rapidfuzz import process, fuzz
from tqdm import tqdm  
import inflect
from descriptors import *

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
RAW_FILE = "Raws.csv"
CLEAN_TERMS = "12u12 Cleaned Terms.xlsx"

OUTPUT_CSV = "cleaned_ingredients.csv"
OUTPUT_XLSX = "cleaned_ingredients.xlsx"

ID_COLUMN = "index"
RAW_TEXT_COLUMN = "ingredients"
CLEAN_TERM_COLUMN = "cleaned terms"

FUZZY_THRESHOLD = 87


# ---------------------------------------------------------
# FUNCTIONS
# ---------------------------------------------------------
def load_data(raw_path, clean_path, nrows=None):
    """Load raw CSV and clean Excel"""
    raw_df = pd.read_csv(raw_path, nrows=nrows)
    clean_df = pd.read_excel(clean_path)

    # Normalize column names
    raw_df.columns = [c.strip().lower() for c in raw_df.columns]
    clean_df.columns = [c.strip().lower() for c in clean_df.columns]

    # Validate required columns
    for col, source in [(RAW_TEXT_COLUMN, "raw CSV"), (ID_COLUMN, "raw CSV"), (CLEAN_TERM_COLUMN, "clean Excel")]:
        if col not in (raw_df.columns if "raw" in source else clean_df.columns):
            raise ValueError(f"Column '{col}' not found in {source}.")

    print(f"Loaded {len(raw_df)} rows from raw data.")
    print(f"Loaded {len(clean_df)} clean ingredient terms.")
    return raw_df, clean_df


def tokenize_ingredients(text):
    """
    Split ingredient list into clean tokens, preserving commas inside parentheses.
    """
    if pd.isna(text):
        return []

    text = str(text).strip().lower()

    # Remove symbols
    text = re.sub(r'[\*\+\#]+', '', text)

    # Fix cases like "s, thermophilus" → "s. thermophilus"
    text = re.sub(r'\b([a-z])\s*,\s*([a-z])', r'\1. \2', text)

    # Replace brackets
    text = text.replace('[','(').replace('{', '(')
    text = text.replace(']', ')').replace('}', ')')

    # Depth-aware split
    tokens = []
    current = []
    depth = 0
    for c in text:
        if c == '(':
            depth += 1
            current.append(c)
        elif c == ')':
            depth -= 1
            current.append(c)
        elif c in ',;:' and depth == 0:
            tokens.append("".join(current).strip())
            current = []
        else:
            current.append(c)
    if current:
        tokens.append("".join(current).strip())

    # Clean up tokens
    remove_words = ["contains", "ingredients", "filling"]
    cleaned_tokens = []
    for token in tokens:
        for word in remove_words:
            pattern = rf"^\b{word}\b\s*"
            token = re.sub(pattern, "", token, flags=re.IGNORECASE)
            if token.strip().lower() == word:
                token = ""
        if token.strip():
            cleaned_tokens.append(token.strip())

    return cleaned_tokens


def is_balanced(text):
    """
    Check if () are balanced in the text.
    """
    stack = []
    pairs = {')': '('}
    for char in text:
        if char in '(':
            stack.append(char)
        elif char in ')':
            if not stack or stack[-1] != pairs[char]:
                return False
            stack.pop()
    return len(stack) == 0


def prepare_clean_terms(clean_df):
    """Prepare and deduplicate clean term list for faster matching."""
    terms = (
        clean_df[CLEAN_TERM_COLUMN]
        .dropna()
        .astype(str)
        .str.lower()
        .str.strip()
        .unique()
        .tolist()
    )
    print(f"Using {len(terms)} unique clean terms for fuzzy matching.")
    return terms


def separate_parenthetical(text):
    """
    Separate the main term and any parenthetical note.

    Example:
        "Cocoa Powder (Low Fat)"  -> ("Cocoa Powder", "(Low Fat)")
    """
    match = re.match(r'^(.*?)\s*(\([^)]*\))?\s*$', text)
    if match:
        main = match.group(1).strip()
        paren = match.group(2) or ""
        return main, paren
    return text, ""


def strip_descriptors(ingredient):
    """
    Detect and temporarily remove descriptors,
    to make sure the core ingredient gets checked and corrected.
    Returns (core_ingredient, prefix, suffix)
    Example:
        'natural and organic mango puree' -> ('mango', 'natural and organic ', ' puree')
    """
    ingredient = ingredient.strip().lower()

    prefix = ""
    suffix = ""

    # Remove multiple prefixes iteratively
    prefix_parts = []
    changed = True
    while changed:
        changed = False
        for desc in prefixes:
            if ingredient.startswith(desc):
                prefix_parts.append(desc)
                ingredient = ingredient[len(desc):].strip()
                changed = True
                break 

    prefix = "".join(prefix_parts)

    for desc in suffixes:
        if ingredient.endswith(desc):
            suffix = desc
            ingredient = ingredient[:-len(desc)].strip()
            break  

    return ingredient, prefix, suffix


def dedupe_repeated_words(phrase):
    words = phrase.strip().split()
    if len(words) < 2:
        return phrase
    if words[-1] == words[-2]:
        return " ".join(words[:-1])
    return phrase


# ---------------------------------------------------------
# TO SINGULAR
# ---------------------------------------------------------
p = inflect.engine()

def to_singular(phrase):
    """
    Convert a multi-word phrase to singular form based on rules:
    - If "and" or "&" is present, singularize all words.
    - Otherwise, singularize only the last word.
    Skips scientific names, abbreviations, numbers, short words, and special cases.
    """
    words = phrase.strip().lower().split()
    new_words = []

    singularize_all = "and" in words or "&" in words or "with" in words

    for i, w in enumerate(words):
        # Skip conditions
        if (
            "." in w                     # abbreviations like s. thermophilus
            or len(w) <= 3               # too short
            or re.search(r'\d', w)       # contains numbers
            or re.search(r'[%()]', w)    # contains special chars
            or w.endswith(("us", "philus", "icus", "osis", "ides", "less", "ness", "sses", "ss"))
        ):
            new_words.append(w)
            continue

        if singularize_all or i == len(words) - 1:
            singular = p.singular_noun(w)
            # Only accept if it’s not chopping too much off
            if singular and len(w) - len(singular) <= 3:
                new_words.append(singular)
            else:
                new_words.append(w)
        else:
            new_words.append(w)

    return " ".join(new_words)


# def detect_plural(word):
#     """
#     Detect simple English plurals and return (is_plural, singular_form).
#     Handles regular plurals like 'onions' -> 'onion', 'berries' -> 'berry'.
#     """
#     word = word.strip().lower()
#     if len(word) <= 3:
#         return False, word  # skip short tokens

#     # Regular plural patterns
#     if word.endswith("ies") and len(word) > 4:
#         return True, word[:-3] + "y"    
#     elif word.endswith("oes"):
#         return True, word[:-2]           
#     elif word.endswith(("ses", "sses")):
#         return False, word              
#     elif word.endswith("es"):
#         return True, word[:-1]          
#     elif word.endswith("s") and not word.endswith(("ss", "us")):
#         return True, word[:-1]         
#     else:
#         return False, word


# ---------------------------------------------------------
# FUZZY CORRECTION
# ---------------------------------------------------------
def fuzzy_correct(ingr, clean_terms, threshold=FUZZY_THRESHOLD, is_stripped = False):
    """
    Fuzzy-correct ingredient only if similarity score is high
    The function adjusts the threshold based on:
    - Whether the ingredient is a single word or multiple words
    - The length of the ingredient (in characters)
    - Whether it is a stripped version (prefix/suffix removed)

    """
    ingr_clean = ingr.strip().lower()
    if not ingr_clean:
        return ingr, 0

    adj_threshold = threshold
    words = ingr_clean.split()

    if len(words) == 1:
        if len(ingr_clean) <= 4:
            adj_threshold = 60
        elif len(ingr_clean) <= 7:
            adj_threshold = 85

    if is_stripped:
        adj_threshold = 88

    match, score, _ = process.extractOne(
        ingr_clean,
        clean_terms,
        scorer=fuzz.ratio
    )

    if not match:
        score = 0

    if score >= adj_threshold:
        return match, score
    else:
        # ingr = ingr + "(unchanged)"
        return ingr, score


def reject_fuzzy(orig, corrected, score, fuzzy_threshold=FUZZY_THRESHOLD,
                 upper_check_score=91, word_sim_threshold=75):
    """
    Reject multiword ingredients (≥3 words) whose score is <91
    and the corrected version differs too much at the word level.
    Returns true if rejected.
    """

    orig_s = (orig or "").strip().lower() # original string
    corr_s = (corrected or "").strip().lower() # fuzzy corrected string

    # --- Split into words ---
    orig_words = [w for w in re.split(r'\s+', orig_s) if w]
    corr_words = [w for w in re.split(r'\s+', corr_s) if w]

    # --- If both are short (<3 words), skip ---
    if len(corr_words) < 3 and len(orig_words) < 3:
        return False

    # --- If lengths are not identical, skip ---
    # if not len(corr_words) == len(orig_words):
    #     return False

    # --- Allow subset matches (corrected ⊆ original) ---
    orig_set = set(orig_words)
    corr_set = set(corr_words)
    if corr_set.issubset(orig_set):
        return False

    # --- Only consider borderline fuzzy matches ---
    if score >= fuzzy_threshold and score <= upper_check_score:
        max_len = max(len(orig_words), len(corr_words))
        for i in range(max_len):
            ow = orig_words[i] if i < len(orig_words) else ""
            cw = corr_words[i] if i < len(corr_words) else ""
            if not ow or not cw:
                continue

            if ow == cw:
                continue

            wscore = fuzz.ratio(ow, cw)
            if wscore < word_sim_threshold:
                return True  # reject

    return False


# ---------------------------------------------------------
# FUZZY MATCHING + SINGULARIZATION ON INGREDIENT LIST
# ---------------------------------------------------------
def correct_ingredient_list(ingr_list, clean_terms):
    """
    Correct an entire list of ingredients using plural normalization, descriptor stripping, 
    and fuzzy matching against a cleaned term list.
    Steps:
    1. Separate main ingredient from any parenthetical descriptor.
    2. Normalize plurals to singular form.
    3. Strip common descriptors (prefixes/suffixes) before fuzzy matching.
    4. Apply fuzzy matching on both stripped and full versions, selecting the best match.
    5. Reject low-confidence fuzzy matches for multi-word ingredients based on word-level similarity.
    6. Recursively correct ingredients within brackets (steps 1-5).
    7. Log accepted or rejected corrections.
    8. Reassemble final ingredient string with descriptors and parentheses.
    """
    corrected_list = []
    change_log = []

    for i, ingr in enumerate(ingr_list, start=1):
        raw_ingr = ingr.strip()

        # Separate parentheses
        main_part, paren_part = separate_parenthetical(raw_ingr)

        # Correct parentheses separately
        if paren_part:
            inner_text = paren_part[1:-1].strip()
            if inner_text:
                paren_part = f"({correct_parenthetical(inner_text, clean_terms, change_log)})"

        ingr_for_match = main_part.lower().strip()
        if not ingr_for_match:
            corrected_list.append(raw_ingr)
            continue

        # --- Step 1: plural normalization ---
        # is_plural, singular_form = detect_plural(ingr_for_match)
        # if is_plural and singular_form != ingr_for_match:
        #     change_log.append(f"{i}. {raw_ingr} → {singular_form} (plural normalized)")
        #     ingr_for_match = singular_form

        # --- Step 1: plural normalization ---
        singular_form = to_singular(ingr_for_match)
        if singular_form != ingr_for_match:
            change_log.append(f"- {ingr_for_match} → {singular_form} (singular)")
            ingr_for_match = singular_form
            
        # Strip descriptors
        core_ingr, prefix, suffix = strip_descriptors(ingr_for_match)
        stripped_ingr_for_match = core_ingr

        # --- Step 2: fuzzy correction ---
        match_candidates = []
        is_stripped = False
        if prefix or suffix:
            is_stripped = True

        # a) Match the stripped version
        corrected_stripped, score_stripped = fuzzy_correct(stripped_ingr_for_match, clean_terms, is_stripped=is_stripped)
        match_candidates.append((corrected_stripped, score_stripped, prefix, suffix))

        # b) Match the original version
        corrected_full, score_full = fuzzy_correct(ingr_for_match, clean_terms, is_stripped=is_stripped)
        match_candidates.append((corrected_full, score_full, "", suffix))

        # change_log.append(
        #     f"[DEBUG] stripped='{stripped_ingr_for_match}' → '{corrected_stripped}' ({score_stripped:.0f}%), "
        #     f"full='{ingr_for_match}' → '{corrected_full}' ({score_full:.0f}%)"
        # )

        # Pick the one with the best score
        best_match = max(match_candidates, key=lambda x: x[1])
        corrected_ingr, best_score, use_prefix, use_suffix = best_match

        # --- Step 3: reject low-confidence fuzzy matches ---
        corrected_ingr_final = corrected_ingr
        rejected = False

        # Only run reject_fuzzy if ingredient was NOT stripped
        if not is_stripped:
            if reject_fuzzy(
                ingr_for_match,
                corrected_ingr,
                best_score,
                fuzzy_threshold=FUZZY_THRESHOLD,
                upper_check_score=91,
                word_sim_threshold=75
            ):
                rejected = True
                # Keep final as original, but log rejects
                rejected_candidate = corrected_ingr
                corrected_ingr_final = ingr_for_match
            else:
                rejected_candidate = None
        else:
            rejected_candidate = None

        final_ingr = f"{use_prefix}{corrected_ingr_final}{use_suffix} {paren_part}".strip()
        final_ingr = dedupe_repeated_words(final_ingr)

        # --- Log accepted and rejected corrections ---
        if final_ingr != raw_ingr:
            if rejected:
                change_log.append(
                    f"- {ingr_for_match} {paren_part} → {rejected_candidate} {paren_part} (REJECTED) ({best_score:.0f}%)"
                )
            elif corrected_ingr_final != ingr_for_match:
                change_log.append(
                    f"- {ingr_for_match} {paren_part} → {corrected_ingr_final} {paren_part} ({best_score:.0f}%)"
                )

        # --- Step 4: avoid redundant entries ---
        if final_ingr == raw_ingr:
            final_ingr = raw_ingr  

        corrected_list.append(final_ingr)

    return corrected_list, change_log


def correct_parenthetical(text, clean_terms, change_log):
    """
    Recursively correct ingredients inside parentheses.
    text: string without outer parentheses
    Returns corrected string with parentheses
    """
    if not text:
        return ""

    tokens = tokenize_ingredients(text)
    corrected_tokens = []

    for token in tokens:
        main, paren = separate_parenthetical(token.strip())

        # Correct main part
        corrected_main, log_main = correct_ingredient_list([main], clean_terms)
        if log_main:
            change_log.extend([f"(PAREN) {entry}" for entry in log_main])

        # Correct nested parentheses recursively
        if paren:
            inner_text = paren[1:-1].strip()  # remove surrounding parentheses
            corrected_paren = correct_parenthetical(inner_text, clean_terms, change_log)
            corrected_tokens.append(f"{corrected_main[0]} ({corrected_paren})")
        else:
            corrected_tokens.append(corrected_main[0])

    return ", ".join(corrected_tokens)


# ---------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------
def main_pipeline(raw_df, clean_terms):
    """
    Apply correction to all ingredient lists.
    """
    print("\nRunning correction...\n")

    raw_df["ingr"] = raw_df[RAW_TEXT_COLUMN].apply(tokenize_ingredients)
    corrected_lists = []
    change_logs = []

    for ingr in tqdm(raw_df["ingr"], desc="Processing rows", unit="row"):

        full_text = ", ".join(ingr)
        
        if not is_balanced(full_text):
            corrected_lists.append(ingr)  # keep original list
            change_logs.append([f"UNBALANCED PARENTHESES"])
            continue

        corrected, log = correct_ingredient_list(ingr, clean_terms)
        corrected_lists.append(corrected)
        change_logs.append(log)

    raw_df["corrected ingr"] = corrected_lists
    raw_df["output"] = raw_df["corrected ingr"].apply(lambda lst: ", ".join(lst))

    filtered_logs = []
    for log in change_logs:
        filtered = [entry for entry in log if "(100%)" not in entry]
        filtered_logs.append(filtered)

    raw_df["changelog"] = ["\n".join(flog) for flog in filtered_logs]

    print("\nCorrection completed!\n")
    return raw_df


def save_results(df, csv_path, xlsx_path):
    """Save the cleaned dataset to both CSV and Excel."""
    cols_to_save = [ID_COLUMN, RAW_TEXT_COLUMN, "output", "changelog"]
    df[cols_to_save].to_csv(csv_path, index=False)
    df[cols_to_save].to_excel(xlsx_path, index=False)
    print(f"Results saved to:\n- CSV: {csv_path}\n- Excel: {xlsx_path}")


# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------
def main(start_row=None, end_row=None):
    raw_df, clean_df = load_data(RAW_FILE, CLEAN_TERMS)
    
    if start_row is not None and end_row is not None:
        raw_df = raw_df.iloc[start_row:end_row].reset_index(drop=True)
        print(f"Processing rows {start_row}–{end_row} ({len(raw_df)} total).")

    clean_terms = prepare_clean_terms(clean_df)
    cleaned_df = main_pipeline(raw_df, clean_terms)
    save_results(cleaned_df, OUTPUT_CSV, OUTPUT_XLSX)

if __name__ == "__main__":
    main(start_row=6000, end_row=6500)

