import argparse
import os
import pickle
from tempfile import NamedTemporaryFile

import numpy as np
import pandas as pd

from utils.hbase import save_to_hbase
from utils.hdfs import generate_input_tsv, put_file_to_hdfs, remove_file_from_hdfs, remove_file
from utils.kafka import save_to_kafka
from utils.mongo import mongo_connection
from utils.utils import log_string
from .ixon_mrjob import MRIxonJob


def save_devices(data, data_type, row_keys, column_map, config, settings, args):
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
            h_table_name = f"{config['data_sources'][config['source']]['hbase_table']}_{data_type}_{args.type}__{args.user}"
            save_to_hbase(data, h_table_name, config['hbase_store_raw_data'], column_map,
                          row_fields=row_keys)
        except Exception as e:
            log_string(f"Error saving datadis supplies to HBASE: {e}")
    else:
        log_string(f"store {config['store']} is not supported")


def gather_devices(config, settings, args):
    for file in os.listdir(f"data/{config['source']}"):
        if file.endswith('.xlsx'):
            df = pd.read_excel(f"data/{config['source']}/{file}")
            df['Object ID'] = df['Object ID'].fillna(0).astype(np.int64)
            df['Object ID'] = df['Object ID'].astype(str)
            save_devices(data=df.to_dict(orient="records"), data_type='static',
                         row_keys=['Description', 'BACnet Type', 'Object ID'], column_map=[("info", "all")],
                         config=config, settings=settings, args=args)


def gather_ts(config, settings, args):
    # generate config file
    job_config = config.copy()
    job_config.update({"store": args.store, "user": args.user, "namespace": args.namespace, "type": args.type,
                       "kafka_message_size": settings.kafka_message_size})
    config_file = NamedTemporaryFile(delete=False, prefix='config_job_', suffix='.pickle')
    config_file.write(pickle.dumps(job_config))
    config_file.close()

    # Connect to MongoDB
    db_ixon_users = mongo_connection(config['mongo_db'])['ixon_users']

    # Generate TSV File
    tmp_path = generate_input_tsv(list(db_ixon_users.find({})), ["email", "password", "api_application", "description"])

    hdfs_out_path = put_file_to_hdfs(source_file_path=tmp_path, destination_file_path="/tmp/ixon_tmp/")

    # MapReduce Config

    MOUNTS = 'YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/dev/net/tun:/dev/net/tun:rw'
    IMAGE = 'YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=docker.tech.beegroup-cimne.com/mr/mr-ixon:development'
    RUNTYPE = 'YARN_CONTAINER_RUNTIME_TYPE=docker'

    mr_job = MRIxonJob(args=[
        '-r', 'hadoop', 'hdfs://%s' % hdfs_out_path,
        '--file', 'utils#utils',
        '--file', 'sources/Ixon/gather/vpn_files/vpn_template_0.ovpn',
        '--file', 'sources/Ixon/gather/vpn_files/vpn_template_1.ovpn',
        '--file', 'sources/Ixon/gather/vpn_files/vpn_template_2.ovpn',
        '--file', 'sources/Ixon/gather/vpn_files/vpn_template_3.ovpn',
        '--file', 'sources/Ixon/gather/vpn_files/vpn_template_4.ovpn',
        '--file', 'sources/Ixon/gather/vpn_files/vpn_template_5.ovpn',
        '--file', f'{config_file.name}',
        '--jobconf', 'mapreduce.map..env={},{},{}'.format(MOUNTS, IMAGE, RUNTYPE),  # PRIVILEGED, DISABLE),
        '--jobconf', 'mapreduce.reduce..env={},{},{}'.format(MOUNTS, IMAGE, RUNTYPE),  # PRIVILEGED, DISABLE),
        '--jobconf', 'mapreduce.job.name=importing_tool_gather_ixon',
        '--jobconf', 'mapreduce.job.reduces=5'
    ])
    try:
        with mr_job.make_runner() as runner:
            runner.run()
        # Remove generated files
        remove_file(tmp_path)
        remove_file_from_hdfs(hdfs_out_path)
        remove_file(config_file.name)
    except Exception as ex:
        log_string(f"error in map_reduce: {ex}")
        # Remove generated files
        remove_file(tmp_path)
        remove_file_from_hdfs(hdfs_out_path)
        remove_file(config_file.name)


def gather(arguments, config=None, settings=None):
    ap = argparse.ArgumentParser(description='Gathering data from Nedgia')
    ap.add_argument("-st", "--store", required=True, help="Where to store the data", choices=["kafka", "hbase"])
    ap.add_argument("--user", "-u", help="The user importing the data", required=True)
    ap.add_argument("--type", "-t", help="Gather data", choices=['devices', 'ts'], required=True)
    ap.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    ap.add_argument("-f", "--file", required=True, help="Excel file path to parse")
    args = ap.parse_args(arguments)

    if args.type == 'devices':
        gather_devices(config=config, settings=settings, args=args)
    elif args.type == 'ts':
        gather_ts(config=config, settings=settings, args=args)
