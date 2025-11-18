def find_last_punct_outside_parens(s, end_idx):
    paren_depth = 0
    for i in range(end_idx -1, -1, -1):
        if s[i] == ')':
            paren_depth += 1
        elif s[i] == '(':
            paren_depth -= 1
        elif paren_depth == 0 and s[i] in [',', ';', '.']:
            return i
    return -1


text = "Salt: "
colon_idx = text.find(':')
last_punct_idx = find_last_punct_outside_parens(text, colon_idx)
print(last_punct_idx)