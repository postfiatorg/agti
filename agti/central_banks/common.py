import re

def clean_text(value: bytes | str) -> str:
    """
    If `value` is bytes, decode to str.
    Then replace tabs (\t) and backspaces (\b) with a space,
    and collapse any run of whitespace into a single space.
    """
    # 1. Convert bytes → str
    if isinstance(value, bytes):
        text = value.decode('utf-8', errors='ignore')
    else:
        text = value

    # 2. Replace literal tab and backspace characters with a space
    #    (the pattern [\t\b] matches \t or \b)
    text = re.sub(r'[\t\b]', ' ', text)

    # 3. Collapse any sequence of whitespace (spaces, newlines, etc.) to a single space
    text = ' '.join(text.split())

    return text