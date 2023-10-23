import hashlib
import pandas as pd
from neo4j import GraphDatabase
from rdflib import Namespace
from datetime import timedelta

import settings
from utils.hbase import save_to_hbase
from utils.data_transformations import *
from utils.neo4j import create_sensor, get_weather_stations_by_location
from utils.rdf.rdf_functions import get_namespace_subject
from ontology.namespaces_definition import bigg_enums, units

time_to_timedelta = {
    "PT1H": timedelta(hours=1),
    "PT15M": timedelta(minutes=15)
}

# keys_remap = {'apparentTemperature': 'apparentTemperature', 'cloudCover': 'cloudCover', 'dewPoint': 'dewPoint',
#               'humidity': 'humidity', 'icon': 'icon', 'ozone': 'ozone', 'precipIntensity': 'precipIntensity',
#               'precipProbability': 'precipProbability', 'precipType': 'precipType', 'pressure': 'pressure',
#               'summary': 'summary', 'temperature': 'temperature', 'uvIndex': 'uvIndex', 'visibility': 'visibility',
#               'windBearing': 'windBearing', 'windGust': 'windGust', 'windSpeed': 'windSpeed',
#               'precipAccumulation': 'precipAccumulation'}

keys_remap = {
    'temperature': {"property": bigg_enums.Temperature, "units": units.DEG_C, "aggregation": "AVG"},
    'humidity': {"property": bigg_enums.HumidityRatio, "units": units.PERCENT, "aggregation": "AVG"}
}


def harmonize_data(data, **kwargs):
    freq = kwargs['freq']
    namespace = kwargs['namespace']
    config = kwargs['config']

    hbase_conn2 = config['hbase_store_harmonized_data']
    neo4j_connection = config['neo4j']

    n = Namespace(namespace)
    df = pd.DataFrame.from_records(data)
    df = df.applymap(decode_hbase)
    df['timestamp'] = pd.to_datetime(df['ts'])
    df["start"] = (pd.to_datetime(df['ts']).astype("int") / 10**9).astype("int")
    df["bucket"] = ((df['start'] // settings.ts_buckets) % settings.buckets).astype("int")
    df['end'] = ((pd.to_datetime(df['ts']) + time_to_timedelta[freq]).astype(int) / 10**9).astype("int")
    df['isReal'] = False
    for k, v in keys_remap.items():
        if k in df.columns:
            df["value"] = df[k]
            for station_id, data_group in df.groupby("station_id"):
                lat, long = station_id.split("~")
                neo = GraphDatabase.driver(**neo4j_connection)
                with neo.session() as session:
                    ws_neo = list(get_weather_stations_by_location(session, lat, long, settings.namespace_mappings))
                for ws_n in ws_neo:
                    data_group.set_index("timestamp", inplace=True)
                    data_group.sort_index(inplace=True)
                    # find device with ID imported from source
                    dt_ini = data_group.iloc[0].name
                    dt_end = data_group.iloc[-1].name
                    device_uri = ws_n["d"].get("uri")
                    prop = get_namespace_subject(v['property'])[1]
                    sensor_id = sensor_subject("weather", "("+station_id+")", prop, "RAW", freq)
                    sensor_uri = str(n[sensor_id])
                    measurement_id = hashlib.sha256(sensor_uri.encode("utf-8"))
                    measurement_id = measurement_id.hexdigest()
                    measurement_uri = str(n[measurement_id])
                    with neo.session() as session:
                        create_sensor(session, device_uri, sensor_uri, v['units'],
                                      v['property'], bigg_enums.TrustedModel,
                                      measurement_uri,
                                      True, False, False, freq, v['aggregation'], dt_ini, dt_end,
                                      settings.namespace_mappings)
                    data_group['listKey'] = measurement_id
                    station_table = f"harmonized_online_{prop}_100_AVG_{freq}_public"
                    save_to_hbase(data_group.to_dict(orient="records"), station_table, hbase_conn2,
                                  [("info", ['end', 'isReal']), ("v", ["value"])],
                                  row_fields=['bucket', 'listKey', 'start'])
                    period_table = f"harmonized_batch_{prop}_100_AVG_{freq}_public"
                    save_to_hbase(data_group.to_dict(orient="records"), period_table, hbase_conn2,
                                  [("info", ['end', 'isReal']), ("v", ["value"])],
                                  row_fields=['bucket', 'start', 'listKey'])
                    neo.close()
