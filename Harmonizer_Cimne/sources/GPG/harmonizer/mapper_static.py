import ast
from neo4j import GraphDatabase
from rdflib import Namespace, Graph
from slugify import slugify

import settings
from utils.cache import Cache
from utils.data_transformations import *
from ontology.namespaces_definition import Bigg, bigg_enums
from utils.rdf.rdf_functions import generate_rdf
from utils.rdf.save_rdf import save_rdf_with_source, update_relationships
from utils.utils import log_string
from .GPG_mapping import Mapper
from .transform_functions import *

# get namespaces
bigg = settings.namespace_mappings['bigg']

def _organization_map(org_list, orgs, default):
    resp = {}
    resp_bool = {}
    for xx in org_list:
        query = slugify(xx)
        match, score = process.extractOne(query, orgs.keys())
        if score > 90:
            resp[xx] = orgs[match]
            resp_bool[xx] = True
        else:
            resp[xx] = default
            resp_bool[xx] = False
    return resp, resp_bool


def _get_fuzz_params(user_id, neo4j_conn):
    # Get all existing Organizations typed department
    neo = GraphDatabase.driver(**neo4j_conn)
    with neo.session() as s:
        organization_names = s.run(f"""
         MATCH 
         (m:{bigg}__Organization {{userID: "{user_id}"}})-[:{bigg}__hasSubOrganization *]->
         (n:{bigg}__Organization{{{bigg}__organizationDivisionType: "Department"}})
         RETURN n.uri as uri, n.{bigg}__organizationName as name
         """)
        dep_uri = {slugify(x.get("name")): x.value().split("#")[1] for x in organization_names}
    return {"orgs": dep_uri, "default": "no-trobat"}


def fuzz_departments(df, user_id, neo4j):
    fparams = _get_fuzz_params(user_id, neo4j)
    org_list = df['Responsable fiscal efectiu'].unique()
    org_map, bool_map = _organization_map(org_list, **fparams)
    df['department_organization'] = df['Responsable fiscal efectiu'].map(org_map)
    df['department_organization_main'] = df['Responsable fiscal efectiu'].map(bool_map)


def clean_dataframe(df, source):
    df['building_organization'] = df['Num ens'].apply(id_zfill).apply(building_department_subject)
    df['building'] = df['Num ens'].apply(id_zfill).apply(building_subject)
    df['buildingIDFromOrganization'] = df['Num ens'].apply(id_zfill)
    df['location_info'] = df['Num ens'].apply(id_zfill).apply(location_info_subject)
    province_dic = Cache.province_dic_ES
    province_fuzz = partial(fuzzy_dictionary_match,
                            map_dict=fuzz_params(province_dic, ['ns1:name']),
                            default=None
                            )
    unique_prov = df['Província'].unique()
    prov_map = {k: province_fuzz(k) for k in unique_prov}
    df['hasAddressProvince'] = df['Província'].map(prov_map)

    municipality_dic = Cache.municipality_dic_ES
    for prov, df_group in df.groupby('hasAddressProvince'):
        municipality_fuzz = partial(fuzzy_dictionary_match,
                                    map_dict=fuzz_params(
                                        municipality_dic,
                                        ['ns1:name'],
                                        filter_query=f"SELECT ?s ?p ?o WHERE{{?s ?p ?o . ?s ns1:parentADM2 <{prov}>}}"
                                        ),
                                    default=None
                                    )
        unique_city = df_group.Municipi.unique()
        city_map = {k: municipality_fuzz(k) for k in unique_city}
        df.loc[df['hasAddressProvince']==prov, 'hasAddressCity'] = df_group.Municipi.map(city_map)

    df['cadastral_info'] = df['Ref. Cadastral'].apply(validate_ref_cadastral)
    df['building_space'] = df['Num ens'].apply(id_zfill).apply(building_space_subject)

    building_type_taxonomy = get_taxonomy_mapping(
        taxonomy_file="sources/GPG/harmonizer/BuildingUseTypeTaxonomy.xls",
        default="Other")
    df.loc[df['Tipus ús']=="nan", 'Tipus ús'] = '["Desconegut"]'

    df['hasBuildingSpaceUseType'] = df['Tipus ús'].apply(lambda x: eval(x)[0]).map(building_type_taxonomy). \
        apply(partial(to_object_property, namespace=bigg_enums))
    df['gross_floor_area'] = df['Num ens'].apply(id_zfill).\
        apply(partial(gross_area_subject, a_source=source))
    df['gross_floor_area_above_ground'] = df['Num ens'].\
        apply(id_zfill).apply(partial(gross_area_subject_above, a_source=source))
    df['gross_floor_area_under_ground'] = df['Num ens'].\
        apply(id_zfill).apply(partial(gross_area_subject_under, a_source=source))

    df['building_element'] = df['Num ens'].apply(id_zfill).apply(construction_element_subject)
    try:
        df.loc[df['Codi_postal'] > 'nan', 'Codi_postal'] = ''
        df['Codi_postal'] = df['Codi_postal'].apply(id_zfill)
    except:
        df['Codi_postal'] = ""
    # remove strange buildings
    # df = df[~((df.hasBuildingSpaceUseType == rdflib.URIRef('http://bigg-project.eu/ontology#Other')) & (
    # df.Sup_const_total.astype(float) == 0))]

def harmonize_data(data, **kwargs):
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']
    organizations = kwargs['organizations'] if 'organizations' in kwargs else False
    n = Namespace(namespace)
    mapper = Mapper(config['source'], n)
    df = pd.DataFrame.from_records(data)
    log_string("preparing df", mongo=False)
    df = df.applymap(decode_hbase)
    clean_dataframe(df, config['source'])
    log_string("generating rdf", mongo=False)
    if organizations:
        log_string("maching organizations", mongo=False)
        fuzz_departments(df, user, config['neo4j'])
        dep = df[df.department_organization_main==True]
        main = df[df.department_organization_main==False]
        g = generate_rdf(mapper.get_mappings("main_org"), main)
        g += generate_rdf(mapper.get_mappings("dep_org"), dep)
        update_relationships(g, mapper.get_update_relationships("main_org"), config['neo4j'])
    else:
        g = generate_rdf(mapper.get_mappings("buildings"), df)
    log_string("saving", mongo=False)
    save_rdf_with_source(g, config['source'], config['neo4j'])
