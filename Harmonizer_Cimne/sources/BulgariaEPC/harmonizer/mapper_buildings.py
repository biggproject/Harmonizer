from rdflib import Namespace
from sources.BulgariaEPC.harmonizer.Mapper import Mapper
from utils.data_transformations import *
from utils.rdf.rdf_functions import generate_rdf
from utils.rdf.save_rdf import save_rdf_with_source, link_devices_with_source


def clean_dataframe_building_info(df_orig, source):
    df = df_orig.copy(deep=True)
    df['subject'] = df['building_id'] + '-' + df['epc_id'].str.strip().str.replace(" ", "")
    df['epc_before_subject'] = df['subject'].apply(epc_subject)
    df['epc_date'] = pd.to_datetime(df['epc_id'].str.strip().str.
                                    replace(" ", "").str.split("/").apply(lambda x: x[1]), format="%d.%m.%Y")
    df['epc_date'] = df['epc_date'].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["energy_class_"] = df["energy_class_before"].str.strip()
    df['building_subject'] = df['building_id']

    df['building_space_subject'] = df['building_id'].str.split("-").apply(lambda x: "-".join(x[1:])).\
        apply(building_space_subject)
    df['gross_floor_area_subject'] = df['building_id'].str.split("-").apply(lambda x: "-".join(x[1:]))\
        .apply(partial(gross_area_subject, a_source=source))
    return df


def prepare_all(df, user, config):
    df_building = clean_dataframe_building_info(df, config['source'])
    return df_building


def harmonize_static(data, **kwargs):
    """
    This function will harmonize only the building information.
    :param data: The json list with the data
    :param kwargs: the set of parameters for the harmonizer (user, namespace and config)
    :return:
    """
    namespace = kwargs['namespace']
    user = kwargs['user']
    n = Namespace(namespace)
    config = kwargs['config']
    df = pd.DataFrame.from_records(data)
    df = df.applymap(decode_hbase)
    df_building = prepare_all(df, user, config)
    mapper = Mapper(config['source'], n)
    g_building = generate_rdf(mapper.get_mappings("building_info"), df_building)
    save_rdf_with_source(g_building, config['source'], config['neo4j'])
    print("harmonized_static")

