import argparse
from datetime import datetime
import utils
from .GPG_gather import read_data_from_xlsx

def gather(arguments, config=None, settings=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--file", required=True, help="Excel file path to parse")
    ap.add_argument("--user", "-u", help="The user importing the data", required=True)
    ap.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    ap.add_argument("-st", "--store", required=True, help="Where to store the data", choices=["kafka", "hbase"])
    args = ap.parse_args(arguments)
    mongo_logger = utils.mongo.mongo_logger
    mongo_logger.create(config['mongo_db'], config['data_sources'][config['source']]['log'], 'gather', user=args.user,
                        log_exec=datetime.utcnow())
    try:
        gpg_list = read_data_from_xlsx(file=args.file)
    except Exception as e:
        gpg_list = []
        utils.utils.log_string(f"could not parse file: {e}")
        exit(1)

    if args.store == "kafka":
        try:
            utils.utils.log_string(f"saving to kafka", mongo=False)
            kafka_message = {
                "namespace": args.namespace,
                "user": args.user,
                "collection_type": "buildings",
                "source": config['source'],
                "row_keys": ["Num ens"],
                "logger": mongo_logger.export_log(),
                "data": gpg_list
            }
            k_harmonize_topic = config["kafka"]["topic"]
            utils.kafka.save_to_kafka(topic=k_harmonize_topic, info_document=kafka_message,
                                      config=config['kafka']['connection'], batch=settings.kafka_message_size)
        except Exception as e:
            utils.utils.log_string(f"error when sending message: {e}")
    elif args.store == "hbase":
        utils.utils.log_string(f"saving to hbase", mongo=False)
        try:
            h_table_name = f"raw_GPG_static_buildings__{args.user}"
            utils.hbase.save_to_hbase(gpg_list, h_table_name, config['hbase_store_raw_data'], [("info", "all")],
                                      row_fields=["Num ens"])
        except Exception as e:
            utils.utils.log_string(f"error saving to hbase: {e}")
    else:
        utils.utils.log_string(f"store {args.store} is not supported")
