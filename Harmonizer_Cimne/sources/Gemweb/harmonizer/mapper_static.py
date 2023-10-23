from functools import partial
from urllib.parse import urlparse
import pandas as pd
from neo4j import GraphDatabase
from rdflib import Namespace
import settings
from utils.cache import Cache
from utils.neo4j import get_all_buildings_id_from_datasource
from ontology.namespaces_definition import bigg_enums
from .Gemweb_mapping import Mapping
from utils.rdf.rdf_functions import generate_rdf
from utils.data_transformations import *
from .transform_functions import ref_cadastral

from utils.rdf.save_rdf import save_rdf_with_source, link_devices_with_source

bigg = settings.namespace_mappings['bigg']


def clean_prepare_all_df(df):
    df['device_subject'] = df.dev_gem_id.apply(partial(device_subject, source="GemwebSource"))
    device_type_taxonomy = get_taxonomy_mapping(
        taxonomy_file="sources/Gemweb/harmonizer/DeviceTypeTaxonomy.xls",
        default="")
    df['hasDeviceType'] = df.tipus_submin.map(device_type_taxonomy).\
        apply(partial(to_object_property, namespace=bigg_enums))
    df['utility_point_id'] = df.cups.str[:20]
    df['utility_point'] = df.utility_point_id.apply(delivery_subject)
    utility_type_taxonomy = get_taxonomy_mapping(
        taxonomy_file="sources/Gemweb/harmonizer/UtilityTypeTaxonomy.xls",
        default="")
    df['hasUtilityType'] = df.tipus_submin.map(utility_type_taxonomy). \
        apply(partial(to_object_property, namespace=bigg_enums))
    df['device_location_info'] = df.device_subject.apply(location_info_subject)

    province_dic = Cache.province_dic_ES
    province_fuzz = partial(fuzzy_dictionary_match,
                            map_dict=fuzz_params(province_dic, ['ns1:name']),
                            default=None)
    unique_prov = df['provincia'].unique()
    province_map = {x: province_fuzz(x) for x in unique_prov}
    df.loc[:, 'hasAddressProvince'] = df.provincia.map(province_map)

    municipality_dic = Cache.municipality_dic_ES
    for prov_k, prov_uri in province_map.items():
        if prov_uri is None:
            df.loc[df['provincia'] == prov_k, 'hasAddressCity'] = None
            continue
        grouped = df.groupby("provincia").get_group(prov_k)
        city_fuzz = partial(fuzzy_dictionary_match,
                            map_dict=fuzz_params(
                                municipality_dic,
                                ['ns1:name'],
                                filter_query=f"""SELECT ?s ?p ?o WHERE {{?s ?p ?o . ?s ns1:parentADM2 <{prov_uri}>}}"""
                            ),
                            default=None)
        unique_city = grouped.poblacio.unique()
        city_map = {k: city_fuzz(k) for k in unique_city}
        df.loc[df['provincia'] == prov_k, 'hasAddressCity'] = grouped.poblacio.map(city_map)

    df['contractedPower'] = "P1: "+df.pot_contract_p1.astype(str) + " " + "P2: "+df.pot_contract_p2.astype(str) + " " \
                            + "P3: "+df.pot_contract_p3.astype(str)

def clean_prepare_linked_df(df):
    df['building'] = df.num_ens.apply(building_subject)
    df['location_info'] = df.num_ens.apply(location_info_subject)
    df['cadastral_info'] = df.observacionsbuilding.apply(ref_cadastral).apply(validate_ref_cadastral)

    df['building_space'] = df.num_ens.apply(building_space_subject)
    building_type_taxonomy = get_taxonomy_mapping(
                                     taxonomy_file="sources/Gemweb/harmonizer/BuildingUseTypeTaxonomy.xls",
                                     default="Other")
    df['hasBuildingSpaceUseType'] = df['subtipus'].map(building_type_taxonomy). \
        apply(partial(to_object_property, namespace=bigg_enums))

    df['gross_floor_area'] = df.num_ens.apply(partial(gross_area_subject, a_source="GemwebSource"))
    df['building_element'] = df.num_ens.apply(construction_element_subject)


def harmonize_data(data, **kwargs):
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']

    neo = GraphDatabase.driver(**config['neo4j'])
    n = Namespace(namespace)
    mapping = Mapping(config['source'], n)
    with neo.session() as ses:
        source_id = ses.run(
            f"""Match (o: {bigg}__Organization{{userID: "{user}"}})-[:hasSource]->(s:GemwebSource) 
                return id(s)""")
        source_id = source_id.single().get("id(s)")

    with neo.session() as ses:
        ids = get_all_buildings_id_from_datasource(ses, source_id, settings.namespace_mappings)
    # create num_ens column with parsed values in df
    df = pd.DataFrame.from_records(data)
    df = df.applymap(decode_hbase)
    df.loc[:, "source_id"] = source_id
    clean_prepare_all_df(df)
    df['num_ens'] = df['codi'].apply(id_zfill)
    df_linked = df[df['num_ens'].isin([str(i) for i in ids])]
    clean_prepare_linked_df(df_linked)
    df_unlinked = df[df['num_ens'].isin([str(i) for i in ids]) == False]

    # get all devices with linked buildings
    for linked, df_ in [("linked", df_linked), ('unlinked', df_unlinked)]:
        g = generate_rdf(mapping.get_mappings(linked), df_)
        save_rdf_with_source(g, config['source'], config['neo4j'])
    link_devices_with_source(df, n, config['neo4j'])

