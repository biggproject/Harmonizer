import pandas as pd


def get_municipi(data):
    return data.split("(")[0].strip()


def get_codi_postal(data):
    if len(data.split("(")) > 1:
        cod_p = data.split("(")[1]
        result = cod_p[:-1]
    else:
        result = None
    return result


def remove_nan(data):
    if not isinstance(data, str):
        return ""
    else:
        return data

def remove_nan_list(data):
    if not isinstance(data, list):
        return []
    else:
        return data


def get_department(data):
    return data.split("\\")[0]


# Main gather function
def read_data_from_xlsx(file):
    columns = {'Num ens': 'Num ens', 'Província': 'Província', 'Municipi': 'Municipi', 'Via': 'Via',
               'Núm. via': 'Núm. via', 'Nom': 'Nom','Tipus ús': 'Tipus ús',
               'Nif resp. fiscal efectiu': 'Nif resp. fiscal efectiu',
               'Responsable fiscal efectiu': 'Responsable fiscal efectiu',
               'Sup. const. sobre rasant': 'Sup. const. sobre rasant',
               'Sup. const. sota rasant': 'Sup. const. sota rasant',
               'Sup. construïda total': 'Sup. construïda total', 'Sup. del terreny': 'Sup. del terreny',
               'Component X': 'Component X', 'Component Y': 'Component Y', 'Ref. Cadastral': 'Ref. Cadastral',
               'Classificació del sòl': 'Classificació del sòl'}
    df = pd.read_excel(file, dtype=str)
    df.rename(columns=columns, inplace=True)
    df.set_index("Num ens", inplace=True)

    df['Tipus ús'] = df['Tipus ús'].apply(remove_nan).apply(lambda x: str(x.split(",")) if len(x.split(",")) > 1 else str([x]))

    # df["Codi_postal"] = df["Municipi"].apply(get_codi_postal)
    #df["Municipi"] = df["Municipi"].apply(get_municipi)
    df["Responsable fiscal efectiu"] = df["Responsable fiscal efectiu"].apply(remove_nan).apply(get_department)
    if "Classificació del sòl" in df.columns:
        df["Classificació del sòl"] = df["Classificació del sòl"].apply(remove_nan)
    df["Ref. Cadastral"] = df["Ref. Cadastral"].apply(remove_nan)
    df.reset_index(inplace=True)
    return df.to_dict("records")
