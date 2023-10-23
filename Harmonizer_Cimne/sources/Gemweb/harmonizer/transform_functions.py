import re


def ref_cadastral(value):
    if "Ref. Cadastral: " in value:
        info = value.split("Ref. Cadastral: ")[-1]
        if ";" in info:
            ref = info.split(";")
        else:
            ref = info.split(" ")
        return ";".join(ref)
    else:
        return ""
