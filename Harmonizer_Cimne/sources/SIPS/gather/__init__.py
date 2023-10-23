import argparse
import utils
from datetime import datetime
import os
import pandas as pd


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
                "data": data
            }
            utils.kafka.save_to_kafka(topic=k_topic, info_document=kafka_message,
                                      config=config['kafka']['connection'], batch=settings.kafka_message_size)

        except Exception as e:
            utils.utils.log_string(f"error when sending message: {e}")

    elif args.store == "hbase":

        try:
            h_table_name = f"{config['data_sources'][config['source']]['hbase_table']}_ts_{data_type}_invoices_{args.user}"  # todo : change

            utils.hbase.save_to_hbase(data, h_table_name, config['hbase_store_raw_data'], column_map,
                                      row_fields=row_keys)
        except Exception as e:
            utils.utils.log_string(f"Error saving datadis supplies to HBASE: {e}")
    else:
        utils.utils.log_string(f"store {config['store']} is not supported")


def gather(arguments, config=None, settings=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("-st", "--store", required=True, help="Where to store the data", choices=["kafka", "hbase"])
    ap.add_argument("--user", "-u", help="The user importing the data", required=True)
    ap.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    ap.add_argument("-d", "--directory", required=True, help="Excel file path to parse")
    ap.add_argument("-tycons", "--type_consumption", choices=["electricity", "gas"], required=True)
    ap.add_argument("-tydata", "--type_data", help="select if you want import PS info o consumptions",
                    choices=["ps_info", "consumption"], required=True)
    args = ap.parse_args(arguments)

    mongo_logger = utils.mongo.mongo_logger
    mongo_logger.create(config['mongo_db'], config['data_sources'][config['source']]['log'], 'gather', user=args.user,
                        log_exec=datetime.utcnow())
    # Files should be placed in data folder under subfolder indicating ['ps_info', 'consumption'] and another subfolder
    # indicating ['electricity', 'gas']
    for file in os.listdir(f'{args.directory}/{args.type_data}/{args.type_consumption}'):
        if file.endswith('.csv'):
            df = pd.read_csv(f'{args.directory}/{args.type_data}/{args.type_consumption}/{file}', sep=',')
            if args.type_data == "ps_info":
                row_keys = ['cups']
            elif args.type_data == "consumption":
                row_keys = ["cups", "fechaInicioMesConsumo"]
            else:
                raise Exception("error bad arguments")

            save_data(data=df.to_dict(orient='records'), data_type=f"{args.type_data}-{args.type_consumption}",
                      row_keys=row_keys,
                      column_map=[("info", "all")], config=config, settings=settings, args=args)


