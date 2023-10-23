import argparse

from sources.OpenData.DadesObertes.CEEE.client import CEEE
from utils.hbase import save_to_hbase
from utils.kafka import save_to_kafka
from utils.nomenclature import raw_nomenclature, RAW_MODE
from utils.utils import log_string
import utils
from datetime import datetime, timedelta


def gather_data(config, settings, args):
    limit = 1000
    offset = 0
    opendata_import = (
        utils.mongo.mongo_connection(config['mongo_db'])
        [config['data_sources'][config['source']]['log_executions']])
    try:
        device = opendata_import.find({"_id": args.user}).limit(1)[0]
    except IndexError:
        device = {"_id": args.user, "last_cert": datetime(1900, 1, 1)}
    start_date = device['last_cert']
    while True:
        log_string(f"Gather data limit={limit}, offset={offset}")
        try:
            df = CEEE().query(limit=limit, offset=offset * limit,
                              where=f"data_entrada>='{start_date}'", order='data_entrada')
            save_data(data=df.to_dict(orient="records"), data_type='EnergyPerformanceCertificate',
                      row_keys=['referencia_cadastral', 'num_cas'], column_map=[("info", "all")],
                      config=config, settings=settings, args=args)
            end_date = df.data_entrada.max()
            opendata_import.update_one({"_id": args.user}, {"$set": {"last_cert": end_date.split('T')[0]}},
                                       upsert=True)
            if len(df.index) == limit:
                offset += 1
            else:
                break
        except Exception as ex:
            log_string(f"Error during the gathering process: {ex}")
            break


def save_data(data, data_type, row_keys, column_map, config, settings, args):
    if args.store == "kafka":
        try:
            k_topic = config["kafka"]["topic"]
            kafka_message = {
                "namespace": args.namespace,
                "user": args.user,
                "collection_type": data_type,
                "source": config['source'],
                "row_keys": row_keys,
                "column_map": column_map,
                "data": data
            }
            save_to_kafka(topic=k_topic, info_document=kafka_message,
                          config=config['kafka']['connection'], batch=settings.kafka_message_size)

        except Exception as e:
            log_string(f"error when sending message: {e}")

    elif args.store == "hbase":

        try:
            h_table_name = raw_nomenclature(mode=RAW_MODE.STATIC, data_type=data_type, frequency="", user=args.user,
                                            source=config['source'])
            save_to_hbase(data, h_table_name, config['hbase_store_raw_data'], column_map,
                          row_fields=row_keys)
        except Exception as e:
            log_string(f"Error saving datadis supplies to HBASE: {e}")
    else:
        log_string(f"store {config['store']} is not supported")


def gather(arguments, settings, config):
    ap = argparse.ArgumentParser(description='Gathering data from OpenData')
    ap.add_argument("-st", "--store", required=True, help="Where to store the data", choices=["kafka", "hbase"])
    ap.add_argument("--user", "-u", help="The user importing the data", required=True)
    ap.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    args = ap.parse_args(arguments)

    gather_data(config=config, settings=settings, args=args)
