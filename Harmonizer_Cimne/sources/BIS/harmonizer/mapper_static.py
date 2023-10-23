from neo4j import GraphDatabase
from rdflib import Namespace, Graph
from slugify import slugify

import settings
from utils.cache import Cache
from utils.data_transformations import *
from utils.utils import log_string
from ontology.namespaces_definition import Bigg, bigg_enums
from utils.rdf.rdf_functions import generate_rdf
from utils.rdf.save_rdf import save_rdf_with_source
from .BIS_mapping import Mapper

# get namespaces
bigg = settings.namespace_mappings['bigg']


def clean_dataframe(df, source):
    df['department_organization'] = df["Departments"].apply(lambda x: ";".join([slugify(x1) for x1 in x.split(";")]))
    df['building_organization'] = df['Unique Code'].apply(building_department_subject)
    df['building'] = df['Unique Code'].apply(building_subject)
    df['location_info'] = df['Unique Code'].apply(location_info_subject)
    country_dic = Cache.country_dic
    country_fuzz = partial(fuzzy_dictionary_match,
                            map_dict=fuzz_params(country_dic, ['ns1:countryCode']),
                            default=None
                            )
    unique_country = df['Country'].unique()
    country_map = {k: country_fuzz(k) for k in unique_country}
    df['hasAddressCountry'] = df['Country'].map(country_map)
    province_dic = Cache.province_dic_ES
    municipality_dic = Cache.municipality_dic_ES
    province_fuzz = partial(fuzzy_dictionary_match,
                            map_dict=fuzz_params(
                                province_dic,
                                ['ns1:name']
                            ),
                            default=None
                            )
    unique_prov = df['Administration Level 2'].unique()
    prov_map = {k: province_fuzz(k) for k in unique_prov}
    df.loc[:, 'hasAddressProvince'] = df['Administration Level 2'].map(prov_map)
    municipality_fuzz = partial(fuzzy_dictionary_match,
                            map_dict=fuzz_params(
                                municipality_dic,
                                ['ns1:name']
                            ),
                            default=None
                            )
    unique_city = df['Administration Level 3'].unique()
    city_map = {k: municipality_fuzz(k) for k in unique_city}
    df.loc[:, 'hasAddressCity'] = df['Administration Level 3'].map(city_map)
    df['Cadastral References'] = df['Cadastral References'].apply(validate_ref_cadastral)
    df['building_space'] = df['Unique Code'].apply(building_space_subject)
    building_type_taxonomy = get_taxonomy_mapping(
        taxonomy_file="sources/BIS/harmonizer/BuildingUseTypeTaxonomyInfraestructures.xls",
        default="Other")
    df['hasBuildingSpaceUseType'] = df['Use Type'].map(building_type_taxonomy). \
        apply(partial(to_object_property, namespace=bigg_enums))
    df['gross_floor_area'] = df['Unique Code'].apply(partial(gross_area_subject, a_source=source))
    df['gross_floor_area_above_ground'] = df['Unique Code'].apply(partial(gross_area_subject_above, a_source=source))
    df['gross_floor_area_under_ground'] = df['Unique Code'].apply(partial(gross_area_subject_under, a_source=source))

    df['building_element'] = df['Unique Code'].apply(construction_element_subject)


def harmonize_data(data, **kwargs):
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']
    n = Namespace(namespace)
    mapper = Mapper(config['source'], n)
    df = pd.DataFrame.from_records(data)
    log_string("preparing df", mongo=False)
    df = df.applymap(decode_hbase)
    clean_dataframe(df, config['source'])
    log_string("generating rdf", mongo=False)
    g = generate_rdf(mapper.get_mappings("all"), df)
    log_string("saving", mongo=False)
    save_rdf_with_source(g, config['source'], config['neo4j'])
