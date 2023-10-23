
def reverse_list(l):
    return l[::-1]


def clean_department(department):
    department = department.replace("--", "")
    department = department.replace("(Assignació)", "")
    department = department.replace("(Adscripció)", "")
    department = department.strip()
    return department


def find_type(department):
    if "(Assignació)" in department:
        return "Assignació"
    if "(Adscripció)" in department:
        return "Adscripció"
    return None


