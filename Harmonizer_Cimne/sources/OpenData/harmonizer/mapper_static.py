import pandas as pd
from neo4j import GraphDatabase
from rdflib import Namespace

from utils.cache import Cache
from sources.OpenData.harmonizer.mapper import Mapper
from utils.data_transformations import *
from utils.rdf.rdf_functions import generate_rdf
from utils.rdf.save_rdf import save_rdf_with_source, link_devices_with_source


def clean_linked_data(df, n):
    df['building_subject'] = df['building_id'].apply(building_subject)
    df['building_uri'] = df['building_subject'].apply(lambda x: n[x])

    # Location
    df['location_subject'] = df['building_id'].apply(location_info_subject)
    df['location_uri'] = df['location_subject'].apply(lambda x: n[x])

    df['hasAddressCity'] = df['poblacio'].map(
        fuzz_location(Cache.municipality_dic_ES, ['ns1:name'], df['poblacio'].unique()))

    df['hasAddressProvince'] = df['nom_provincia'].map(
        fuzz_location(Cache.province_dic_ES, ['ns1:name', 'ns1:officialName'],
                      df['nom_provincia'].dropna().unique()))

    # Cadastral Reference
    df['cadastral_subject'] = df['referencia_cadastral'].apply(cadastral_info_subject)
    df['cadastral_uri'] = df['cadastral_subject'].apply(lambda x: n[x])
    return df


def clean_all_data(df, n):
    # EPC
    df['epc_subject'] = df['num_cas'].apply(epc_subject)

    df['epc_uri'] = df['epc_subject'].apply(lambda x: n[x])

    # EPC Additional Info
    df['additional_epc_subject'] = df['num_cas'].apply(additional_epc_subject)
    df['additional_epc_uri'] = df['additional_epc_subject'].apply(lambda x: n[x])
    df['data_entrada'] = pd.to_datetime(df.data_entrada).dt.tz_localize("Europe/Madrid").apply(lambda x: pd.Timestamp(x).isoformat())
    return df


def harmonize_data(data, **kwargs):
    namespace = kwargs['namespace']
    config = kwargs['config']
    n = Namespace(namespace)

    neo4j_connection = config['neo4j']
    neo = GraphDatabase.driver(**neo4j_connection)
    with neo.session() as session:
        cad_buildings = session.run(
            f"""MATCH (b:bigg__Building)-[:bigg__hasCadastralInfo]-(c:bigg__CadastralInfo),
            (b:bigg__Building)<-[:bigg__managesBuilding|bigg__hasSubOrganization *]-(o:bigg__Organization{{userID:'icaen'}}) 
             RETURN b.bigg__buildingIDFromOrganization as building_id, c.bigg__landCadastralReference as cadastral_ref """).data()
    neo.close()
    df = pd.DataFrame(data)
    df = df.applymap(decode_hbase)

    building_df = pd.DataFrame(cad_buildings)
    building_df = building_df.set_index('cadastral_ref')
    building_df = building_df[building_df.index.duplicated() == False]
    df['building_id'] = df.referencia_cadastral.map(building_df.building_id)

    df = clean_all_data(df, n)

    building_linked = df[~pd.isna(df.building_id)]
    building_unlinked = df[pd.isna(df.building_id)]

    building_linked = clean_linked_data(building_linked, n)
    mapper = Mapper(config['source'], n)

    for linked, _df in [("linked", building_linked), ("unlinked", building_unlinked)]:
        g = generate_rdf(mapper.get_mappings(linked), _df)
        save_rdf_with_source(g, config['source'], neo4j_connection)
