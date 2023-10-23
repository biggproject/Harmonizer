from functools import partial
import pandas as pd
import settings
from rdflib import Namespace
from utils.cache import Cache
from utils.data_transformations import get_taxonomy_mapping, fuzzy_dictionary_match, fuzz_params, decode_hbase
from utils.rdf.rdf_functions import generate_rdf
from utils.rdf.save_rdf import save_rdf_with_source, link_devices_with_source
from utils.utils import log_string
import neo4j

from sources.SIPS.harmonizer.ElecSupply_mapping import Mapper as ElectricMapper
from sources.SIPS.harmonizer.GasSupply_mapping import Mapper as GasMapper

bigg = settings.namespace_mappings['bigg']


def get_cups_ens(config):
    query = """
        Match(n:bigg__Building)-[:bigg__hasSpace]->(bs)-[:bigg__hasUtilityPointOfDelivery]->(u) 
        RETURN u.bigg__pointOfDeliveryIDFromOrganization as cups, n.bigg__buildingIDFromOrganization as ens
            """
    gdb = neo4j.GraphDatabase.driver(**config['neo4j'])
    with gdb.session() as session:
        data = session.run(query).data()
    return data


def clean_all_df(df):
    df_clean = pd.DataFrame()
    df_clean['building_space_uri'] = df.ens.apply(lambda x: "BUILDINGSPACE-" + str(x) if not pd.isna(x) else None)
    df_clean['device_uri'] = "DEVICE-" + "SIPSSource" + "-" + df.cups
    df_clean['location_uri'] = "LOCATION-" + df_clean['device_uri']
    df_clean['device_name'] = df.cups
    df_clean['supply_uri'] = "SUPPLY-" + df.cups.apply(lambda x: x[:20])
    df_clean['supply_name'] = df.cups.apply(lambda x: x[:20])
    df_clean['postal_code'] = df.codigoPostalPS
    prov_codes = get_taxonomy_mapping(taxonomy_file="sources/SIPS/harmonizer/province_codes.xlsx", default=None)
    df_clean['province'] = df.codigoProvinciaPS.astype(int).map(prov_codes)
    province_dic = Cache.province_dic_ES
    province_fuzz = partial(fuzzy_dictionary_match,
                            map_dict=fuzz_params(
                                    province_dic,
                                    ['ns1:name', 'ns1:officialName']),
                            default=None)
    unique_provinces = df_clean['province'].unique()
    province_map = {k: province_fuzz(k) for k in unique_provinces}
    df_clean['province'] = df_clean['province'].map(province_map)
    mun_codes = get_taxonomy_mapping(
        taxonomy_file="sources/SIPS/harmonizer/municipality_codes.xlsx",
        default="None")
    mun_codes = {k: " ".join(v.split(",")[::-1]).strip() for k, v in mun_codes.items()}
    df_clean['municipality'] = df.municipioPS.astype(int).map(mun_codes)
    municipality_dic = Cache.municipality_dic_ES
    for prov, df_group in df_clean.groupby('province'):
        municipality_fuzz = partial(fuzzy_dictionary_match,
                                    map_dict=fuzz_params(
                                        municipality_dic,
                                        ['ns1:name', 'ns1:officialName'],
                                        filter_query=f"SELECT ?s ?p ?o WHERE{{?s ?p ?o . ?s ns1:parentADM2 <{prov}>}}"
                                    ),
                                    default=None)
        unique_city = df_group['municipality'].unique()
        city_map = {k: municipality_fuzz(k) for k in unique_city}
        df_clean.loc[df_clean['province'] == prov, 'municipality'] = df_group.municipality.map(city_map)
    return df_clean


def clean_electric_fields(df, df_clean):
    contracted_powers = ['potenciasContratadasEnWP1', 'potenciasContratadasEnWP2', 'potenciasContratadasEnWP3',
               'potenciasContratadasEnWP4', 'potenciasContratadasEnWP5', 'potenciasContratadasEnWP6']
    df_clean['contracted_power'] = df.apply(lambda x: " ".join([str(int(float(x[c]))) if str(x[c]) != 'nan' else '' for c in contracted_powers]), axis=1)
    df_clean['max_power'] = df.potenciaMaximaBIEW
    tariff_codes = get_taxonomy_mapping(taxonomy_file="sources/SIPS/harmonizer/tariff_codes.xlsx", default=None)
    df_clean['tariff'] = df.codigoTarifaATREnVigor.astype(float).fillna(-1).astype(int).map(tariff_codes)
    voltage_codes = get_taxonomy_mapping(taxonomy_file="sources/SIPS/harmonizer/voltage_codes.xlsx", default=None)
    df_clean['voltage'] = df.codigoTensionV.astype(float).fillna(-1).astype(int).map(voltage_codes)
    property_codes = get_taxonomy_mapping(taxonomy_file="sources/SIPS/harmonizer/property_codes.xlsx", default=None)
    df_clean['property'] = df.codigoPropiedadEquipoMedida.fillna(-1).astype(int).map(property_codes)
    tc_codes = get_taxonomy_mapping(taxonomy_file="sources/SIPS/harmonizer/tc_codes.xlsx", default=None)
    df_clean['tc'] = df.codigoTelegestion.astype(float).fillna(-1).astype(int).map(tc_codes)
    auto_codes = get_taxonomy_mapping(taxonomy_file="sources/SIPS/harmonizer/auto_codes.xlsx", default=None)
    df_clean['auto'] = df.codigoAutoconsumo.astype(float).fillna(-1).astype(int).map(auto_codes)
    return df_clean


def clean_gas_fields(df, df_clean):
    df_clean['address'] = df.tipoViaPS.replace("nan", "") + " " + df.viaPS.replace("nan", "") + " " +\
                          df.numFincaPS.replace("nan", "") + " " + df.portalPS.replace("nan", "") + " " +\
                          df.escaleraPS.replace("nan", "") + " " + df.pisoPS.replace("nan", "") + " " + \
                          df.puertaPS.replace("nan", "")
    toll_codes = get_taxonomy_mapping(taxonomy_file="sources/SIPS/harmonizer/toll_codes.xlsx", default=None)
    df_clean['toll'] = df.codigoPeajeEnVigor.map(toll_codes)
    df_clean['teleMeasurement'] = df.codigoTelemedida
    return df_clean


def harmonize_ps_electric_data(data, **kwargs):
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']
    n = Namespace(namespace)
    mapper = ElectricMapper(config['source'], n)
    neo = neo4j.GraphDatabase.driver(**config['neo4j'])
    with neo.session() as session:
        datasource = session.run(f"""
                 MATCH (o:{bigg}__Organization{{userID:'{user}'}})-[:hasSource]->(s:SIPSSource) return id(s)""").single()
        datasource = datasource['id(s)']
    log_string("preparing df", mongo=False)
    df = pd.DataFrame.from_records(data)
    df = df.applymap(decode_hbase)
    df = df.drop_duplicates(subset=["cups"], keep='last')
    df['ens'] = df.cups.apply(lambda x: x[:20]).map({i['cups']: i['ens'] for i in get_cups_ens(config)})
    df_clean = clean_all_df(df)
    df_clean = clean_electric_fields(df, df_clean)
    log_string("saving df", mongo=False)
    g = generate_rdf(mapper.get_mappings("electric_ps_linked"), df_clean[pd.isnull(df_clean['building_space_uri'])==False])
    save_rdf_with_source(g, config['source'], config['neo4j'])
    g = generate_rdf(mapper.get_mappings("electric_ps_unlinked"), df_clean[pd.isnull(df_clean['building_space_uri'])])
    save_rdf_with_source(g, config['source'], config['neo4j'])
    log_string("linking df", mongo=False)
    df_link = df_clean[["device_uri"]]
    df_link = df_link.rename({'device_uri': 'device_subject'}, axis=1)
    df_link['source_id'] = datasource
    link_devices_with_source(df_link, n, config['neo4j'])


def harmonize_ps_gas_data(data, **kwargs):
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']
    n = Namespace(namespace)
    mapper = GasMapper(config['source'], n)
    neo = neo4j.GraphDatabase.driver(**config['neo4j'])
    with neo.session() as session:
        datasource = session.run(f"""
                 MATCH (o:{bigg}__Organization{{userID:'{user}'}})-[:hasSource]->(s:SIPSSource) return id(s)""").single()
        datasource = datasource['id(s)']
    log_string("preparing df", mongo=False)
    df = pd.DataFrame.from_records(data)
    df = df.applymap(decode_hbase)
    df = df.drop_duplicates(subset=["cups"], keep='last')
    df['ens'] = df.cups.apply(lambda x: x[:20]).map({i['cups']: i['ens'] for i in get_cups_ens(config)})
    df_clean = clean_all_df(df)
    df_clean = clean_gas_fields(df, df_clean)
    log_string("saving df", mongo=False)
    g = generate_rdf(mapper.get_mappings("electric_ps_linked"), df_clean[pd.isnull(df_clean['building_space_uri']) == False])
    save_rdf_with_source(g, config['source'], config['neo4j'])
    g = generate_rdf(mapper.get_mappings("electric_ps_unlinked"), df_clean[pd.isnull(df_clean['building_space_uri'])])
    save_rdf_with_source(g, config['source'], config['neo4j'])

    df_link = df_clean[["device_uri"]]
    df_link = df_link.rename({'device_uri': 'device_subject'}, axis=1)
    df_link['source_id'] = datasource
    link_devices_with_source(df_link, n, config['neo4j'])
