import hashlib

import pandas as pd
from neo4j import GraphDatabase
from rdflib import Namespace
from utils.data_transformations import decode_hbase, sensor_subject
from utils.hbase import save_to_hbase
from utils.neo4j import create_sensor, get_device_from_datasource

import settings
from ontology.namespaces_definition import bigg_enums, units


def clean_dataframe(df):
    df_clean = pd.DataFrame()
    df_clean['cups'] = df.cups
    df_clean['ts'] = pd.to_datetime(df.fechaInicioMesConsumo.
                                       apply(lambda x: f"{x}T00:00.000")).dt.tz_localize(
        "Europe/Madrid").dt.tz_convert("UTC")
    df_clean['start'] = df_clean['ts']
    to_shift = df.fechaInicioMesConsumo == df.fechaFinMesConsumo.shift(1)
    df_clean['start'][to_shift] = df_clean['start'][to_shift] + pd.DateOffset(days=1)
    df_clean['end'] = pd.to_datetime(df.fechaFinMesConsumo.
                                     apply(lambda x: f"{x}T23:59.000")).dt.tz_localize(
        "Europe/Madrid").dt.tz_convert("UTC")
    df_clean['start'] = (df_clean['start'].astype(int) / 10 ** 9).astype(int)
    df_clean['end'] = (df_clean['end'].astype(int) / 10 ** 9).astype(int)
    df_clean["bucket"] = (df_clean.start // settings.ts_buckets) % settings.buckets
    # if the end date is the same as the start date of next row, we add 1 day to start date

    df_clean['isReal'] = True
    return df_clean


def store_ts(df_clean, sensor_type, namespace, user, neo4j_connection, hbase_conn2):
    for cups, df_cups in df_clean.groupby("cups"):
        df_cups.set_index("ts", inplace=True)
        df_cups.sort_index(inplace=True)
        dt_ini = df_cups.iloc[0].name
        dt_end = df_cups.iloc[-1].name
        neo = GraphDatabase.driver(**neo4j_connection)
        with neo.session() as session:
            device_neo = list(get_device_from_datasource(session, user, cups, "SIPSSource",
                                                         settings.namespace_mappings))
        for d_neo in device_neo:
            device_uri = d_neo["d"].get("uri")
            sensor_id = sensor_subject("SIPSSource", cups, "EnergyConsumptionGridElectricity", "RAW", "")
            sensor_uri = str(namespace[sensor_id])
            measurement_id = hashlib.sha256(sensor_uri.encode("utf-8"))
            measurement_id = measurement_id.hexdigest()
            measurement_uri = str(namespace[measurement_id])
            with neo.session() as session:
                create_sensor(session, device_uri, sensor_uri, units["KiloW-HR"],
                              bigg_enums[sensor_type], bigg_enums.Naive,
                              measurement_uri, False,
                              False, False, "", "SUM", dt_ini, dt_end, settings.namespace_mappings)

            df_cups['listKey'] = measurement_id
            device_table = f"harmonized_online_{sensor_type}_000_SUM__{user}"
            save_to_hbase(df_cups.to_dict(orient="records"), device_table, hbase_conn2,
                          [("info", ['end', 'isReal']), ("v", ['value'])],
                          row_fields=['bucket', 'listKey', 'start'])
            period_table = f"harmonized_batch_{sensor_type}_000_SUM__{user}"
            save_to_hbase(df_cups.to_dict(orient="records"), period_table, hbase_conn2,
                          [("info", ['end', 'isReal']), ("v", ['value'])],
                          row_fields=['bucket', 'start', 'listKey'])
        neo.close()


def harmonize_ts_gas_data(data, **kwargs):
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']
    hbase_conn2 = config['hbase_store_harmonized_data']
    neo4j_connection = config['neo4j']
    n = Namespace(namespace)
    df = pd.DataFrame.from_records(data)
    df = df.applymap(decode_hbase)
    df_clean = clean_dataframe(df)
    columns = [x for x in df.columns if "consumoEnWh" in x]
    df_clean['value'] = df[columns].astype(float).sum(axis=1)
    store_ts(df_clean, "EnergyConsumptionGas", n, user, neo4j_connection, hbase_conn2)


def harmonize_ts_electric_data(data, **kwargs):
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']
    hbase_conn2 = config['hbase_store_harmonized_data']
    neo4j_connection = config['neo4j']
    n = Namespace(namespace)
    df = pd.DataFrame.from_records(data)
    df = df.applymap(decode_hbase)
    df_clean = clean_dataframe(df)
    columns = [x for x in df.columns if "consumoEnergiaActivaEnWhP" in x]
    df_clean['value'] = df[columns].astype(float).sum(axis=1)
    store_ts(df_clean, "EnergyConsumptionGridElectricity", n, user, neo4j_connection, hbase_conn2)
