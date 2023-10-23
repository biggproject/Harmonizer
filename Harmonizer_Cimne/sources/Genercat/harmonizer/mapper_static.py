import pandas as pd
from neo4j import GraphDatabase
from rdflib import Namespace

import settings
from ontology.namespaces_definition import bigg_enums
from .EEM_mapping import Mapper
from .transform_functions import get_code_ens
from utils.rdf.rdf_functions import generate_rdf
from utils.rdf.save_rdf import save_rdf_with_source
from utils.data_transformations import *

bigg = settings.namespace_mappings['bigg']


def create_measurement(x):
    return ".".join([x1 for x1 in [
        x["""Instal·lació millorada \n(NIVELL 1)"""],
        x["""Tipus de millora \n(NIVELL 2)"""],
        x["""Tipus de millora \n(NIVELL 3)"""],
        x["""Tipus de millora \n(NIVELL 4)"""]
        ] if pd.isna(x1) == False and x1 != 'nan'])


def prepare_data(df):
    df['element_subject'] = df["ce"].apply(id_zfill).apply(construction_element_subject)
    df['measure_subject'] = (df["ce"].apply(id_zfill) + "-" + df["id_"]).apply(eem_subject)
    eem_type_taxonomy = get_taxonomy_mapping(taxonomy_file="sources/Genercat/harmonizer/EEMTypeTaxonomy.xls",
                                             default="Other")
    df['measurement_type'] = df.apply(create_measurement, axis=1).map(eem_type_taxonomy). \
        apply(partial(to_object_property, namespace=bigg_enums))
    df["""Inversió \n(€) \n(IVA no inclòs)"""] = df["""Inversió \n(€) \n(IVA no inclòs)"""].fillna("0")
    df['operation_date'] = (df["""Data de finalització de l'obra / millora"""].astype('datetime64')).dt.strftime("%Y-%m-%dT%H:%M:%SZ")



def harmonize_data(data, **kwargs):
    config = kwargs['config']
    user = kwargs['user']
    namespace = kwargs['namespace']
    df = pd.DataFrame.from_records(data)
    if df.empty:
        return
    df = df.applymap(decode_hbase)
    df['ce'] = df[' Edifici (Espai)  - Codi Ens (GPG)'].apply(get_code_ens).apply(id_zfill)
    # upload only mapped measures
    df = df[df['ce'] != '00000']
    # Get all existing BuildingConstructionElements
    neo = GraphDatabase.driver(**config['neo4j'])
    with neo.session() as s:
        element_id = s.run(f"""
                            MATCH (o: {bigg}__Organization{{userID: "{user}"}})-
                            [:{bigg}__hasSubOrganization *]->()-[:{bigg}__managesBuilding]->()-[:{bigg}__hasSpace]->()
                            -[:{bigg}__isAssociatedWithElement]->(n:{bigg}__BuildingConstructionElement)
                             return n.uri
                            """)
        codi_neo = [get_code_ens(x.value()) for x in element_id]
    df = df[df["ce"].isin(codi_neo)]

    prepare_data(df)
    n = Namespace(namespace)
    mapper = Mapper(config['source'], n)
    g = generate_rdf(mapper.get_mappings("all"), df)
    save_rdf_with_source(g, config['source'], config['neo4j'])
