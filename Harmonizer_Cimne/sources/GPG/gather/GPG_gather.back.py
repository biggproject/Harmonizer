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


def remove_semicolon(data):
    result = None
    if data:
        result = str(data)[:-1]
    return result


# Main gather function
def read_data_from_xlsx(file):
    columns = ["Num_Ens_Inventari", "Provincia", "Municipi", "Via", "Num_via",
               "Departament_Assig_Adscrip", "Espai", "Tipus_us",
               "Sup_const_sobre_rasant", "Sup_const_sota rasant",
               "Sup_const_total", "Sup_terreny", "Classificacio_sol",
               "Qualificacio_urbanistica", "Clau_qualificacio_urbanistica",
               "Ref_Cadastral"]
    df = pd.read_excel(file, names=columns, skiprows=list(range(1, 9)))
    df.set_index("Num_Ens_Inventari", inplace=True)
    df_duplicates = df.copy(deep=True)
    df = df[~df.index.duplicated(keep='last')]

    df_join = pd.DataFrame()
    for g, d in df_duplicates.groupby(df_duplicates.index):
        df_join = df_join.append(pd.DataFrame.from_records([{'Tipus_us': d['Tipus_us'].unique().tolist()}], index=[g]))

    df["Tipus_us"].update(df_join['Tipus_us'])

    df["Codi_postal"] = df["Municipi"].apply(get_codi_postal)
    df["Municipi"] = df["Municipi"].apply(get_municipi)
    df["Departament_Assig_Adscrip"] = df["Departament_Assig_Adscrip"].apply(remove_semicolon)
    df["Classificacio_sol"] = df["Classificacio_sol"].apply(remove_semicolon)
    df["Ref_Cadastral"] = df["Ref_Cadastral"].apply(remove_semicolon)
    df.reset_index(inplace=True)
    return df.to_dict("records")
