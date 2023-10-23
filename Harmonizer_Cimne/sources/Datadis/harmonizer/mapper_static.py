import pandas as pd
from neo4j import GraphDatabase
from rdflib import Namespace

from utils.cache import Cache
from utils.neo4j import get_cups_id_link
from utils.utils import log_string
from .Datadis_mapping import Mapping
from utils.rdf.rdf_functions import generate_rdf
from utils.rdf.save_rdf import save_rdf_with_source, link_devices_with_source
from utils.data_transformations import *
import settings

bigg = settings.namespace_mappings['bigg']


def prepare_df_clean_all(df, user, neo4j):
    nif_map = get_nif_map(user, neo4j)
    df.loc[:, 'source_id'] = df.nif.map(nif_map)
    df.loc[:, 'device_subject'] = df.cups.apply(partial(device_subject, source="DatadisSource"))
    df.loc[:, 'utility_point_subject'] = df.utility_point_id.apply(delivery_subject)
    df.loc[:, 'device_location_subject'] = df.device_subject.apply(location_info_subject)
    province_dic = Cache.province_dic_ES
    province_fuzz = partial(fuzzy_dictionary_match,
                            map_dict=fuzz_params(province_dic, ['ns1:name']),
                            default=None)
    unique_prov = df['province'].unique()
    province_map = {x: province_fuzz(x) for x in unique_prov}
    df.loc[:, 'hasAddressProvince'] = df.province.map(province_map)

    municipality_dic = Cache.municipality_dic_ES
    for prov_k, prov_uri in province_map.items():
        if prov_uri is None:
            df.loc[df['province'] == prov_k, 'hasAddressCity'] = None
            continue
        grouped = df.groupby("province").get_group(prov_k)
        city_fuzz = partial(fuzzy_dictionary_match,
                            map_dict=fuzz_params(
                                municipality_dic,
                                ['ns1:name'],
                                filter_query=f"""SELECT ?s ?p ?o WHERE {{?s ?p ?o . ?s ns1:parentADM2 <{prov_uri}>}}"""
                            ),
                            default=None)
        unique_city = grouped.municipality.unique()
        city_map = {k: city_fuzz(k) for k in unique_city}
        df.loc[df['province'] == prov_k, 'hasAddressCity'] = grouped.municipality.map(city_map)


def get_nif_map(user, conn):
    neo = GraphDatabase.driver(**conn)
    with neo.session() as session:
        nif_map = session.run(f"""
        Match (n:DatadisSource)<-[:hasSource]-()<-[:bigg__hasSubOrganization*0..]-(bigg__Organization{{userID:"{user}"}})
        RETURN n.username as nif, id(n) as id""").data()
    df = pd.DataFrame.from_records(nif_map)
    df.set_index("nif", inplace=True)
    return df.id.to_dict()


def prepare_df_clean_linked(df):
    df.loc[:, 'location_subject'] = df.NumEns.apply(id_zfill).apply(location_info_subject)
    df.loc[:, 'building_space_subject'] = df.NumEns.apply(id_zfill).apply(building_space_subject)


def harmonize_data(data, **kwargs):
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']
    log_string("creating df", mongo=False)
    df = pd.DataFrame.from_records(data)
    log_string("preparing df", mongo=False)

    df = df.applymap(decode_hbase)

    # get codi_ens from neo4j
    neo = GraphDatabase.driver(**config['neo4j'])
    with neo.session() as ses:
        cups_code = get_cups_id_link(ses, user, settings.namespace_mappings)

    df.loc[:, 'utility_point_id'] = df.cups.str[:20]

    df.loc[:, 'NumEns'] = df.utility_point_id.map(cups_code)  # map cups with first 20 characters

    prepare_df_clean_all(df, user, config['neo4j'])
    linked_supplies = df[df["NumEns"].isna()==False]
    unlinked_supplies = df[df["NumEns"].isna()]

    for linked, df_tmp in [("linked", linked_supplies), ("unlinked", unlinked_supplies)]:
        if linked == "linked":
            prepare_df_clean_linked(df_tmp)
        log_string("maping df", mongo=False)
        n = Namespace(namespace)
        mapping = Mapping(config['source'], n)
        g = generate_rdf(mapping.get_mappings(linked), df_tmp)
        log_string("saving df", mongo=False)
        save_rdf_with_source(g, config['source'], config['neo4j'])
    log_string("linking df", mongo=False)
    link_devices_with_source(df, n, config['neo4j'])
