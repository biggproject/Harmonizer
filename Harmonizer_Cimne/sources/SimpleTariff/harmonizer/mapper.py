import hashlib
from datetime import datetime

import pandas as pd
from neo4j import GraphDatabase
from rdflib import Namespace

import settings
from utils.data_transformations import *
from utils.hbase import save_to_hbase
from utils.neo4j import get_tariff_from_datasource, create_tariff_component
from ontology.namespaces_definition import units, bigg_enums
from slugify import slugify


def harmonize_data_ts(data, **kwargs):
    # Variables
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']
    tariff_uid = kwargs['tariff']
    measured_property = kwargs['measured_property']
    priced_property = kwargs['priced_property']
    priced_property_unit = kwargs['priced_property_unit']
    currency_unit = kwargs['currency_unit']

    tariff_uri = tariff_subject("SimpleTariffSource", user, tariff_uid)
    date_ini = kwargs['date_ini']
    date_end = kwargs['date_end']
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

    tariff_df = pd.DataFrame(index=ranged)
    tariff_df['map'] = tariff_df.index.strftime("%m-%d-%H")

    tariff_df['value'] = tariff_df.map.map({k['map']: k['value'] for k in df.to_dict(orient="records")})
    tariff_df = tariff_df.dropna()
    if tariff_df.empty:
        return
    tariff_df['start'] = tariff_df.index.astype('int') / 10 ** 9
    tariff_df['start'] = tariff_df['start'].astype('int')
    tariff_df['end'] = tariff_df['start'] + 3600
    tariff_df['isReal'] = True

    tariff_df['bucket'] = (tariff_df['start'] // settings.ts_buckets) % settings.buckets

    tariff_df = tariff_df[['bucket', 'start', 'end', 'value', 'isReal']]

    tariff_df.sort_index(inplace=True)

    dt_ini = tariff_df.iloc[0].name.tz_convert("UTC").tz_convert(None)
    dt_end = tariff_df.iloc[-1].name.tz_convert("UTC").tz_convert(None)

    with neo.session() as session:
        n = Namespace(namespace)
        devices_neo = list(get_tariff_from_datasource(session, user, str(n[tariff_uri]), "SimpleTariffSource",
                                                          settings.namespace_mappings))
    for device in devices_neo:
        tariff_uri = device['d'].get("uri")
        prop = str(measured_property).split("#")[1]
        tariff_comp_id = tariff_component_subject("SimpleTariffSource", tariff_uid, prop, "RAW",
                                             "PT1H")
        tariff_comp_uri = str(n[tariff_comp_id])
        measurement_id = hashlib.sha256(tariff_comp_uri.encode("utf-8"))
        measurement_id = measurement_id.hexdigest()
        measurement_uri = str(n[measurement_id])
        with neo.session() as session:
            create_tariff_component(session=session, tariff_component_uri=tariff_comp_uri, property_uri=measured_property,
                                    estimation_method_uri=bigg_enums["TrustedModel"], is_regular=True, is_cumulative=False, is_on_change=False,
                                    freq="PT1H", agg_func="SUM",  dt_ini=dt_ini, dt_end=dt_end, measurement_uri=measurement_uri,
                                    tariff_uri=tariff_uri, priced_property=priced_property, unit_uri=priced_property_unit,
                                    currency_unit=currency_unit, ns_mappings=settings.namespace_mappings)
        tariff_df['listKey'] = measurement_id
        device_table = f"harmonized_online_{prop}_100_SUM_PT1H_{user}"

        save_to_hbase(tariff_df.to_dict(orient="records"),
                      device_table,
                      hbase_conn2,
                      [("info", ['end', 'isReal']), ("v", ['value'])],
                      row_fields=['bucket', 'listKey', 'start'])
        period_table = f"harmonized_batch_{prop}_100_SUM_PT1H_{user}"
        save_to_hbase(tariff_df.to_dict(orient="records"),
                      period_table, hbase_conn2,
                      [("info", ['end', 'isReal']), ("v", ['value'])],
                      row_fields=['bucket', 'start', 'listKey'])
