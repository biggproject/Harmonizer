import pandas as pd
from neo4j import GraphDatabase
from rdflib import Namespace, Graph
from slugify import slugify
import ast
import settings
from utils.cache import Cache
from utils.data_transformations import *
from utils.utils import log_string
from ontology.namespaces_definition import Bigg, bigg_enums
from utils.rdf.rdf_functions import generate_rdf
from utils.rdf.save_rdf import save_rdf_with_source
from .IdentificacionEdificio_mapping import Mapper as IdentificacionEdificio_Mapper
from .CondicionesFuncionamientoyOcupacion_mapping import Mapper as CondicionesFuncionamientoyOcupacion_Mapper
from .DatosGeneralesyGeometria_mapping import Mapper as DatosGeneralesyGeometria_Mapper
from .Demanda_mapping import Mapper as Demanda_Mapper
from .Consumo_mapping import Mapper as Consumo_Mapper
from .Emissions_mapping import Mapper as Emissions_Mapper
from .Calification_mapping import Mapper as Calification_Mapper
# get namespaces
bigg = settings.namespace_mappings['bigg']


def clean_CondicionesFuncionamientoyOcupacion_dataframes(df, sources):
    building_organization_code = df.loc[0, 'building_organization_code']
    df_spaces = pd.DataFrame.from_records(ast.literal_eval(df.loc[0, 'Espacio']))
    df_spaces.loc[:, 'building_space_main_subject'] = building_space_subject(building_organization_code)
    df_spaces.loc[:, 'building_space_current_subject'] = df_spaces.apply(lambda x:
                                                                         building_space_subject(
                                                                             f"{building_organization_code}_{x.name}"),
                                                                         axis=1)
    df_spaces.loc[:, 'building_space_current_netfloorarea_subject'] = df_spaces.apply(
        lambda x: net_area_subject(f"{building_organization_code}_{x.name}", a_source="CEEC3X"),
        axis=1)
    return df_spaces


def clean_DatosGeneralesyGeometria_dataframes(df, sources):
    df.loc[:, 'buildingspace_subject'] = df.building_organization_code.apply(building_space_subject)
    df.loc[:, 'netfloor_area_subject'] = df.building_organization_code.apply(partial(net_area_subject, a_source=sources))
    df.loc[:, 'heated_area_subject'] = df.building_organization_code.apply(partial(heated_area_subject, a_source=sources))
    df.loc[:, 'cooling_area_subject'] = df.building_organization_code.apply(partial(cooled_area_subject, a_source=sources))
    df.loc[:, 'heated_area_value'] = f"{float(df.SuperficieHabitable) * float((float(df.PorcentajeSuperficieHabitableCalefactada)/100)):.3f}"
    df.loc[:, 'cooling_area_value'] = f"{float(df.SuperficieHabitable) * float((float(df.PorcentajeSuperficieHabitableRefrigerada)/100)):.3f}"


def clean_IdentificacionEdificio_dataframes(df, source):
    #Building
    df.loc[:, 'building_subject'] = df.building_organization_code.apply(building_subject)
    #Location
    df.loc[:, 'location_subject'] = df.building_organization_code.apply(location_info_subject)
    municipality_dic = Cache.municipality_dic_ES
    municipality_fuzz = partial(fuzzy_dictionary_match,
                                map_dict=fuzz_params(
                                    municipality_dic,
                                    ['ns1:name']
                                ),
                                default=None
                                )
    df.loc[:, 'hasAddressCity'] = df['Municipio'].apply(municipality_fuzz)
    province_dic = Cache.province_dic_ES
    province_fuzz = partial(fuzzy_dictionary_match,
                            map_dict=fuzz_params(
                                province_dic,
                                ['ns1:name']
                            ),
                            default=None
                            )
    df.loc[:, 'hasAddressProvince'] = df['Provincia'].apply(province_fuzz)
    # -> hasAddressClimateZone
    # CadastralInfo
    df.loc[:, 'cadastral_subject'] = df.ReferenciaCatastral.apply(cadastral_info_subject)
    # BuildingSpace
    df.loc[:, 'buildingspace_subject'] = df.building_organization_code.apply(building_space_subject)
    building_type_taxonomy = get_taxonomy_mapping(
        taxonomy_file="sources/CEEC3X/harmonizer/BuildingUseTypeTaxonomy.xls",
        default=to_object_property("Other", namespace=bigg_enums))
    for k, v in building_type_taxonomy.items():
        building_type_taxonomy[k] = to_object_property(v, namespace=bigg_enums)
    df.loc[:, 'hasBuildingSpaceUseType'] = df['TipoDeEdificio'].map(building_type_taxonomy)
    # EnergyPerformanceCertificate
    df.loc[:, 'epc_subject'] = df['certificate_unique_code'].apply(epc_subject)
    # EnergyPerformanceCertificateAdditionalInfo
    df.loc[:, 'epc_additional_subject'] = df['epc_subject'] + "-additional"


def clean_Demanda_dataframes(df, sources):
    df.loc[:, 'epc_subject'] = df['certificate_unique_code'].apply(epc_subject)
    try:
        df.loc[:, 'Calefaccion'] = df['EdificioObjeto'].apply(lambda x: eval(x)['Calefaccion'])
    except:
        pass
    try:
        df.loc[:, 'Refrigeracion'] = df['EdificioObjeto'].apply(lambda x: eval(x)['Refrigeracion'])
    except:
        pass

def clean_Consumo_dataframes(df, sources):
    df.loc[:, 'epc_subject'] = df['certificate_unique_code'].apply(epc_subject)
    try:
        df.loc[:, 'annualPrimaryEnergyConsumption'] = df["EnergiaPrimariaNoRenovable"].apply(lambda x:eval(x)['Global'])
    except:
        pass
    try:
        df.loc[:, 'annualFinalEnergyConsumption'] = df["EnergiaFinalVectores"].\
            apply(lambda x: sum([float(v['Global']) for k, v in eval(x).items()]))
    except:
        pass

def clean_Emissions_dataframes(df, sources):
    df.loc[:, 'epc_subject'] = df['certificate_unique_code'].apply(epc_subject)
    try:
        df.loc[:, 'annualC02Emissions'] = df['TotalConsumoElectrico'].astype(float) + df['TotalConsumoOtros'].astype(float)
    except:
        pass
    try:
        df.loc[:, 'annualLightingCO2Emissions'] = df['Iluminacion'].astype(float) * df['area'].astype(float)
    except:
        pass
    try:
        df.loc[:, 'annualHeatingCO2Emissions'] = df['Calefaccion'].astype(float) * df['area'].astype(float)
    except:
        pass
    try:
        df.loc[:, 'annualHotWaterCO2Emissions'] = df['ACS'].astype(float) * df['area'].astype(float)
    except:
        pass
    try:
        df.loc[:, 'annualCoolingCO2Emissions'] = df['Refrigeracion'].astype(float) * df['area'].astype(float)
    except:
        pass

def clean_Calification_dataframes(df, sources):
    df.loc[:, 'epc_subject'] = df['certificate_unique_code'].apply(epc_subject)
    try:
        df.loc[:, 'heatingEnergyDemandClass'] = df["Demanda"].apply(lambda x:eval(x)['Calefaccion'])
    except:
        pass
    try:
        df.loc[:, 'coolingEnergyDemandClass'] = df["Demanda"].apply(lambda x:eval(x)['Refrigeracion'])
    except:
        pass
    try:
        df.loc[:, 'lightingCO2EmissionsClass'] = df["EmisionesCO2"].apply(lambda x:eval(x)['Iluminacion'])
    except:
        pass
    try:
        df.loc[:, 'heatingCO2EmissionsClass'] = df["EmisionesCO2"].apply(lambda x:eval(x)['Calefaccion'])
    except:
        pass
    try:
        df.loc[:, 'hotWaterCO2EmissionsClass'] = df["EmisionesCO2"].apply(lambda x:eval(x)['ACS'])
    except:
        pass
    try:
        df.loc[:, 'coolingCO2EmissionsClass'] = df["EmisionesCO2"].apply(lambda x:eval(x)['Refrigeracion'])
    except:
        pass
    try:
        df.loc[:, 'C02EmissionsClass'] = df["EmisionesCO2"].apply(lambda x:eval(x)['Global'])
    except:
        pass

def harmonize_IdentificacionEdificio(data, **kwargs):
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']
    n = Namespace(namespace)
    log_string("preparing df", mongo=False)
    df = pd.DataFrame.from_records(data)
    df = df.applymap(decode_hbase)
    clean_IdentificacionEdificio_dataframes(df, config['source'])
    mapper = IdentificacionEdificio_Mapper(config['source'], n)
    log_string("generating rdf", mongo=False)
    g = generate_rdf(mapper.get_mappings("all"), df)
    log_string("saving", mongo=False)
    save_rdf_with_source(g, config['source'], config['neo4j'])


def harmonize_DatosGeneralesyGeometria(data, **kwargs):
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']
    n = Namespace(namespace)
    log_string("preparing df", mongo=False)
    df = pd.DataFrame.from_records(data)
    df = df.applymap(decode_hbase)
    clean_DatosGeneralesyGeometria_dataframes(df, config['source'])
    mapper = DatosGeneralesyGeometria_Mapper(config['source'], n)
    log_string("generating rdf", mongo=False)
    g = generate_rdf(mapper.get_mappings("all"), df)
    log_string("saving", mongo=False)
    save_rdf_with_source(g, config['source'], config['neo4j'])


def harmonize_CondicionesFuncionamientoyOcupacion(data, **kwargs):
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']
    n = Namespace(namespace)
    log_string("preparing df", mongo=False)
    df = pd.DataFrame.from_records(data)
    df = df.applymap(decode_hbase)
    df = clean_CondicionesFuncionamientoyOcupacion_dataframes(df, config['source'])
    mapper = CondicionesFuncionamientoyOcupacion_Mapper(config['source'], n)
    log_string("generating rdf", mongo=False)
    g = generate_rdf(mapper.get_mappings("all"), df)
    log_string("saving", mongo=False)
    save_rdf_with_source(g, config['source'], config['neo4j'])


def harmonize_Demanda(data, **kwargs):
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']
    n = Namespace(namespace)
    log_string("preparing df", mongo=False)
    df = pd.DataFrame.from_records(data)
    df = df.applymap(decode_hbase)
    clean_Demanda_dataframes(df, config['source'])
    mapper = Demanda_Mapper(config['source'], n)
    log_string("generating rdf", mongo=False)
    g = generate_rdf(mapper.get_mappings("all"), df)
    log_string("saving", mongo=False)
    save_rdf_with_source(g, config['source'], config['neo4j'])

def harmonize_Consumo(data, **kwargs):
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']
    n = Namespace(namespace)
    log_string("preparing df", mongo=False)
    df = pd.DataFrame.from_records(data)
    df = df.applymap(decode_hbase)
    clean_Consumo_dataframes(df, config['source'])
    mapper = Consumo_Mapper(config['source'], n)
    log_string("generating rdf", mongo=False)
    g = generate_rdf(mapper.get_mappings("all"), df)
    log_string("saving", mongo=False)
    save_rdf_with_source(g, config['source'], config['neo4j'])

def harmonize_Emissions(data, **kwargs):
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']
    n = Namespace(namespace)
    log_string("preparing df", mongo=False)
    df = pd.DataFrame.from_records(data)
    df = df.applymap(decode_hbase)
    clean_Emissions_dataframes(df, config['source'])
    mapper = Emissions_Mapper(config['source'], n)
    log_string("generating rdf", mongo=False)
    g = generate_rdf(mapper.get_mappings("all"), df)
    log_string("saving", mongo=False)
    save_rdf_with_source(g, config['source'], config['neo4j'])


def harmonize_Calificacion(data, **kwargs):
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']
    n = Namespace(namespace)
    log_string("preparing df", mongo=False)
    df = pd.DataFrame.from_records(data)
    df = df.applymap(decode_hbase)
    clean_Calification_dataframes(df, config['source'])
    mapper = Calification_Mapper(config['source'], n)
    log_string("generating rdf", mongo=False)
    g = generate_rdf(mapper.get_mappings("all"), df)
    log_string("saving", mongo=False)
    save_rdf_with_source(g, config['source'], config['neo4j'])