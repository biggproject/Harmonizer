import argparse

import pandas as pd
import utils
import os
from datetime import datetime
from sources import ManagedFile, ManagedFolder


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
        raise Exception("NOT IMPLEMENTED")
        # try:
        #     h_table_name = f"raw_BulgariaEPC_static_{data_type}__bulgaria"
        #
        #     utils.hbase.save_to_hbase(data, h_table_name, config['hbase_store_raw_data'], column_map,
        #                               row_fields=row_keys)
        # except Exception as e:
        #     utils.utils.log_string(f"Error saving datadis supplies to HBASE: {e}")
    else:
        utils.utils.log_string(f"store {config['store']} is not supported")


def gather(arguments, config=None, settings=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("-st", "--store", required=True, help="Where to store the data", choices=["kafka", "hbase"])
    ap.add_argument("-a", "--auto", action="store_true",
                    help="Wether to use the automatic file managment based on UI or not")
    ap.add_argument("-f", "--file", required=True, help="Folder with Excel EPC file path to parse")
    ap.add_argument("-b", "--buildingId", default='', help="Folder with Excel EPC file path to parse")
    ap.add_argument("-n", "--namespace", default='', help="Folder with Excel EPC file path to parse")
    ap.add_argument("-u", "--user", default='', help="Folder with Excel EPC file path to parse")
    ap.add_argument("-p", "--pod", default='', help="k8s pod partial name")
    ap.add_argument("-pns", "--pod_ns", default='', help="k8s pod namespace")

    args = ap.parse_args(arguments)
    mongo_logger = utils.mongo.mongo_logger
    mongo_logger.create(config['mongo_db'], config['data_sources'][config['source']]['log'], 'gather', user=args.user,
                        log_exec=datetime.utcnow())

    m_folder = ManagedFolder(config=config, deployment=args.pod, namespace=args.pod_ns, auto=args.auto, dest_path="..")
    for file in os.listdir(m_folder.get_not_processed_path(args.file)):
        if file.endswith('.xlsx'):
            managed_file = ManagedFile(args.file, file, m_folder)
            managed_file.set_status(ManagedFile.PROCESSING)
            df = pd.read_excel(managed_file.get_local_file())
            args.namespace, args.buildingId, args.user = managed_file.get_info(args.namespace, args.buildingId, args.user)
            df['building_id'] = args.buildingId
            df['namespace'] = args.namespace
            save_data(data=df.to_dict(orient="records"), data_type="consumption",
                      row_keys=['namespace', "building_id", "Start Date (YYYY-MM-DD)"],
                      column_map=[("info", "all")], config=config, settings=settings, args=args)
            managed_file.set_status(ManagedFile.PROCESSED)




    """
import argparse
import pandas as pd
import utils
import os
from datetime import datetime
from sources import ManagedFile, ManagedFolder
import settings
config = utils.utils.read_config(settings.conf_file)
config['source'] = 'ManualData'
ap = argparse.ArgumentParser()
ap.add_argument("-st", "--store", required=True, help="Where to store the data", choices=["kafka", "hbase"])
ap.add_argument("-a", "--auto", action="store_true", help="Wether to use the automatic file managment based on UI or not")
ap.add_argument("-f", "--file", required=False, help="Folder with Excel EPC file path to parse")
ap.add_argument("-b", "--buildingId", default='', help="Folder with Excel EPC file path to parse")
ap.add_argument("-n", "--namespace", default='', help="Folder with Excel EPC file path to parse")
ap.add_argument("-u", "--user", default='', help="Folder with Excel EPC file path to parse")
ap.add_argument("-p", "--pod", default='', help="k8s pod partial name")
ap.add_argument("-pns", "--pod_ns", default='', help="k8s pod namespace")
args = ap.parse_args(['-a', '-f', "/mnt/data/manual_data", '-st', 'kafka', '-p', 'webapp-entrack', '-pns', 'dev-ui'])
m_folder = ManagedFolder(config=config, deployment="webapp-entrack", namespace="dev-ui", auto=True, dest_path="..")
for file in os.listdir(m_folder.get_not_processed_path(args.file)):
        if file.endswith('.xlsx'):
            managed_file = ManagedFile(args.file, file, m_folder)
            managed_file.set_status(ManagedFile.PROCESSING)
            print(args.namespace, args.buildingId, args.user)
            df = pd.read_excel(managed_file.get_local_file())
            args.namespace, args.buildingId, args.user = managed_file.get_info(args.namespace, args.buildingId, args.user)
            print(args.namespace, args.buildingId, args.user)
            managed_file.set_status(ManagedFile.PROCESSED)
    """
