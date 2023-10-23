import hashlib
from datetime import datetime

import pandas as pd
from neo4j import GraphDatabase
from rdflib import Namespace

import settings
from utils.data_transformations import *
from utils.hbase import save_to_hbase
from utils.neo4j import create_co2_component, get_co2_from_datasource
from ontology.namespaces_definition import units, bigg_enums
from slugify import slugify


def harmonize_data_ts(data, **kwargs):
    # Variables
    namespace = kwargs['namespace']
    co2_uid = kwargs['co2_uid']
    measured_property = kwargs['measured_property']
    co2_property = kwargs['co2_property']
    co2_property_unit = kwargs['co2_property_unit']
    unit = kwargs['unit']
    user = kwargs['user']

    co2_uri = co2_subject(co2_uid)
    date_ini = kwargs['date_ini']
    date_end = kwargs['date_end']
    config = kwargs['config']
    # Database connections

    hbase_conn2 = config['hbase_store_harmonized_data']
    neo4j_connection = config['neo4j']
    neo = GraphDatabase.driver(**neo4j_connection)

    # Init dataframe
    df_full = pd.DataFrame(data={"data": pd.date_range(datetime(2020, 1, 1, 0), datetime(2020, 12, 31, 23), freq="H",
                                                       tz="UTC").values})
    df = pd.DataFrame.from_records(data)
    df = df.applymap(decode_hbase)
    df.index = df.pos.astype(int).map(df_full.data)
    df.sort_index(inplace=True)

    df['map'] = df.index.strftime("%m-%d-%H")
    ranged = pd.date_range(date_ini, date_end, freq="H", tz="UTC")

    co2_df = pd.DataFrame(index=ranged)
    co2_df['map'] = co2_df.index.strftime("%m-%d-%H")

    co2_df['value'] = co2_df.map.map({k['map']: k['value'] for k in df.to_dict(orient="records")})
    co2_df = co2_df.dropna()
    if co2_df.empty:
        return
    co2_df['start'] = co2_df.index.astype('int') / 10 ** 9
    co2_df['start'] = co2_df['start'].astype('int')
    co2_df['end'] = co2_df['start'] + 3600
    co2_df['isReal'] = True

    co2_df['bucket'] = (co2_df['start'] // settings.ts_buckets) % settings.buckets

    co2_df = co2_df[['bucket', 'start', 'end', 'value', 'isReal']]

    co2_df.sort_index(inplace=True)

    dt_ini = co2_df.iloc[0].name.tz_convert("UTC").tz_convert(None)
    dt_end = co2_df.iloc[-1].name.tz_convert("UTC").tz_convert(None)

    with neo.session() as session:
        n = Namespace(namespace)
        devices_neo = list(get_co2_from_datasource(session, str(n[co2_uri]), settings.namespace_mappings))
    for device in devices_neo:
        co2_uri = device['d'].get("uri")
        prop = str(measured_property).split("#")[1]

        co2_list_id = co2_list_subject("CO2EmissionsSource", co2_uid, prop, "RAW", "PT1H")
        co2_list_uri = str(n[co2_list_id])
        measurement_id = hashlib.sha256(co2_list_uri.encode("utf-8"))
        measurement_id = measurement_id.hexdigest()
        measurement_uri = str(n[measurement_id])
        with neo.session() as session:
            create_co2_component(session=session, co2_factor_list_uri=co2_list_uri, property_uri=measured_property,
                                 estimation_method_uri=bigg_enums["TrustedModel"], is_regular=True, is_cumulative=False, is_on_change=False,
                                 freq="PT1H", agg_func="SUM",  dt_ini=dt_ini, dt_end=dt_end, measurement_uri=measurement_uri,
                                 co2_uri=co2_uri, related_prop=co2_property, related_unit=co2_property_unit,
                                 unit=unit, ns_mappings=settings.namespace_mappings)
        co2_df['listKey'] = measurement_id
        device_table = f"harmonized_online_{prop}_100_SUM_PT1H_{user}"

        save_to_hbase(co2_df.to_dict(orient="records"),
                      device_table,
                      hbase_conn2,
                      [("info", ['end', 'isReal']), ("v", ['value'])],
                      row_fields=['bucket', 'listKey', 'start'])
        period_table = f"harmonized_batch_{prop}_100_SUM_PT1H_{user}"
        save_to_hbase(co2_df.to_dict(orient="records"),
                      period_table, hbase_conn2,
                      [("info", ['end', 'isReal']), ("v", ['value'])],
                      row_fields=['bucket', 'start', 'listKey'])
