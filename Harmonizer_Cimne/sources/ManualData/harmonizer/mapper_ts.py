import hashlib

import neo4j
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
    df_clean['device'] = df.namespace + df.building_id
    df_clean['ts'] = pd.to_datetime(df['Start Date (YYYY-MM-DD)']).dt.tz_localize("UTC")
    df_clean['start'] = (df_clean['ts'].astype(int) / 10 ** 9).astype(int)
    df_clean['end'] = (pd.to_datetime(df['End Date (YYYY-MM-DD)']).dt.tz_localize("UTC").astype(int) / 10 ** 9).astype(int)
    df_clean["bucket"] = (df_clean.start // settings.ts_buckets) % settings.buckets
    df_clean['isReal'] = True
    return df_clean


def store_ts(df_clean, namespace, user, neo4j_connection, hbase_conn2):
    for device, df_cups in df_clean.groupby("device"):
        df_cups.set_index("ts", inplace=True)
        df_cups.sort_index(inplace=True)
        dt_ini = df_cups.iloc[0].name
        dt_end = df_cups.iloc[-1].name
        neo = GraphDatabase.driver(**neo4j_connection)
        with neo.session() as session:
            device_info = mes_prop = list(session.run(f"""
                Match(n:bigg__Device) where n.uri = "{device}" 
                Match(n)-[:bigg__hasDeviceType]->()<-[:bigg__relatedDeviceType]-()-[:bigg__relatedMeasuredProperty]->(p) 
                return n, p
            """))
        for info in device_info:
            device_uri = info.get('n').get("uri")
            device_id = info.get('n').get("uri").split('#')[1]
            property = info.get('p').get('uri').split("#")[1]
            sensor_id = sensor_subject("ManualSource", device_id, "EnergyConsumptionGridElectricity", "RAW", "")
            sensor_uri = str(namespace[sensor_id])
            measurement_id = hashlib.sha256(sensor_uri.encode("utf-8"))
            measurement_id = measurement_id.hexdigest()
            measurement_uri = str(namespace[measurement_id])
            with neo.session() as session:
                create_sensor(session, device_uri, sensor_uri, units["KiloW-HR"],
                              bigg_enums[property], bigg_enums.Naive,
                              measurement_uri, False,
                              False, False, "", "SUM", dt_ini, dt_end, settings.namespace_mappings)
            df_cups['listKey'] = measurement_id
            device_table = f"harmonized_online_{property}_000_SUM__{user}"
            save_to_hbase(df_cups.to_dict(orient="records"), device_table, hbase_conn2,
                          [("info", ['end', 'isReal']), ("v", ['value'])],
                          row_fields=['bucket', 'listKey', 'start'])
            period_table = f"harmonized_batch_{property}_000_SUM__{user}"
            save_to_hbase(df_cups.to_dict(orient="records"), period_table, hbase_conn2,
                          [("info", ['end', 'isReal']), ("v", ['value'])],
                          row_fields=['bucket', 'start', 'listKey'])
        neo.close()


def harmonize_ts_data(data, **kwargs):
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']
    hbase_conn2 = config['hbase_store_harmonized_data']
    neo4j_connection = config['neo4j']
    n = Namespace(namespace)
    df = pd.DataFrame.from_records(data)
    df = df.applymap(decode_hbase)
    df_clean = clean_dataframe(df)
    df_clean['value'] = df['Net Value(kWh)'].astype(float)
    store_ts(df_clean, n, user, neo4j_connection, hbase_conn2)


