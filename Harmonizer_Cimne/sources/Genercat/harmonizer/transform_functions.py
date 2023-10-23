import re


def get_code_ens(text):
    if isinstance(text, str):
        match = re.search(r"\d{4,5}", text)
        if match:
            return match.group(0)
    return ''


