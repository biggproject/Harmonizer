import argparse
import hashlib
from datetime import datetime
from .genercat_gather import get_data
import utils


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
        data = get_data(args.file)
        file_id = hashlib.md5(bytes(args.file.split("/")[-1], encoding="utf-8")).hexdigest()
        for i, datat in enumerate(data):
            datat['id_'] = f"{file_id}~{i}"
    except Exception as e:
        data = []
        utils.utils.log_string(f"error parsing the file: {e}")
        exit(1)
    if args.store == "kafka":
        utils.utils.log_string(f"saving to kafka", mongo=False)
        try:
            kafka_message = {
                "namespace": args.namespace,
                "user": args.user,
                "collection_type": "eem",
                "source": config['source'],
                "row_keys": ["id_"],
                "logger": mongo_logger.export_log(),
                "data": data
            }
            k_topic = config["kafka"]["topic"]
            utils.kafka.save_to_kafka(topic=k_topic, info_document=kafka_message,
                                      config=config['kafka']['connection'], batch=settings.kafka_message_size)
        except Exception as e:
            utils.utils.log_string(f"error when sending message: {e}")
    elif args.store == "hbase":
        utils.utils.log_string(f"saving to hbase", mongo=False)
        try:
            h_table_name = f"raw_{config['source']}_static_eem__{args.user}"
            utils.hbase.save_to_hbase(data, h_table_name, config['hbase_store_raw_data'], [("info", "all")], row_fields=["id_"])
        except Exception as e:
            utils.utils.log_string(f"error saving to hbase: {e}")
    else:
        utils.utils.log_string(f"store {args.store} is not supported")

