import pandas as pd


def get_data(file):
    columns = ["""Departament""", """Entitat / Departament""", """Area""", """File""",
       """ Edifici (Espai)  - Codi Ens (GPG)""",
       """Instal·lació millorada \n(NIVELL 1)""", """Tipus de millora \n(NIVELL 2)""",
       """Tipus de millora \n(NIVELL 3)""", """Tipus de millora \n(NIVELL 4)""",
       """Descripció""",
       """% de la instal·lació millorada / Potencia FV instal·lada [kW] """,
       """Data d'inici\nde l'obra / millora""",
       """Data de finalització de l'obra / millora""",
       """Inversió \n(€) \n(IVA no inclòs)""", """Tipus energia""",
       """Altres observacions""", """Servei Territorial""", """Municipi""", """Tipus centre""",
       """Inversió \n(€) \n(IVA inclòs)"""]
    df = pd.read_excel(file, names=columns)
    return df.to_dict(orient="records")

