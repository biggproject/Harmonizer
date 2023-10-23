import pandas as pd


# Main gather function
def read_data_from_xlsx(file):
    columns = ["Unique Code", "Country", "Administration Level 1", "Administration Level 2", "Administration Level 3",
               "Road", "Road Number", "CP", "Longitud", "Latitud", "Departments", "Name", "Use Type",
               "Gross Floor Area Above Ground", "Gross Floor Area Under Ground", "Gross Floor Area", "Land Area",
               "Land Type", "Cadastral References"]
    df = pd.read_excel(file, names=columns)
    df.set_index("Unique Code", inplace=True)
    df = df[~df.index.duplicated(keep='last')]
    df.reset_index(inplace=True)
    return df.to_dict("records")
