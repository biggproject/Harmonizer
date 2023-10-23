from tempfile import NamedTemporaryFile

from pyhive import hive
import argparse
import pickle
import json
import os
import sys

from sources.Gemweb import Plugin

sys.path.append(os.getcwd())
from utils import *
from Gemweb.gemweb_gather_mr import Gemweb_gather
import datetime

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-freq", "--frequency", required=True, help="frequency of data to import: one of {}".format(
        list(Gemweb_gather.frequencies.keys()) + ["all"]))
    ap.add_argument("-l", "--limit", required=False, help="The limit of packages to run at one execution")
    ap.add_argument("-d", "--device", required=False, help="The device to start obtaining data")
    ap.add_argument("-df", "--date_from", required=False, help="The date from when to obtain data")
    ap.add_argument("-dt", "--date_to", required=False, help="The date to when to obtain data")
    ap.add_argument("-u", "--user", required=False, help="The gemweb user to download data from")
    ap.add_argument("-r", "--reduces", required=False, help="The number of reduces")
    args = vars(ap.parse_args())

    # parameters validation
    if args['frequency'] not in list(Gemweb_gather.frequencies.keys()) + ["all"]:
        raise NotImplemented(f"The frequency {args['frequency']} is not implemented")
    frequency = args['frequency']

    if args['limit']:
        if args['limit'] < 0:
            raise ValueError("limit: must be positive")
        limit = args['limit']
    else:
        limit = None

    if args["device"]:
        device = args['device']
    else:
        device = None

    if args['date_from']:
        try:
            datetime.datetime.strptime(args['date_from'], "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"date_from: {e}")
        date_from = args['date_from']
    else:
        date_from = None

    if args['date_to']:
        try:
            datetime.datetime.strptime(args['date_to'], "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"date_to: {e}")
        date_to = args['date_to']
    else:
        date_to = datetime.datetime.utcnow().date().strftime("%Y-%m-%d")

    if args['user']:
        user = args['user']
    else:
        user = 'all'

    if args['reduces']:
        if args['reduces'] < 0:
            raise ValueError("reduces: must be positive")
        reduces = args['reduces']
    else:
        reduces = 8

    # read config file
    with open("./config.json") as config_f:
        config = json.load(config_f)

    mongo = connection_mongo(config['mongo_db'])
    data_source = config['data_sources'][config['source']]

    # start data obtention the connections
    if user != 'all':
        connections = mongo[data_source['info']].find({"user": user})
    else:
        connections = mongo[data_source['info']].find({})

    for connection in connections:
        # connection = mongo[data_source['info']].find_one({})
        # create supplies hdfs file to perform mapreduce
        hbase_table = f"raw_data:gemweb_supplies_{connection['user']}"
        version = connection['timeseries']['version'] + 1 if 'timeseries' in connection and 'version' in connection[
            'timeseries'] else 0
        hdfs_file = f"supplies_{connection['user']}"
        create_table_hbase = f"""CREATE EXTERNAL TABLE {hdfs_file}(id string, value string)
                                STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
                                WITH SERDEPROPERTIES (
                                    'hbase.table.name' = '{hbase_table}',
                                    "hbase.columns.mapping" = ":key,info:cups"
                                )"""

        save_id_to_file = f"""INSERT OVERWRITE DIRECTORY '/tmp/{hdfs_file}/' SELECT id FROM {hdfs_file}"""
        if device:
            save_id_to_file = f"{save_id_to_file} WHERE id > '{device}'"
        if limit:
            save_id_to_file = f"{save_id_to_file} LIMIT {limit}"

        remove_hbase_table = f"""DROP TABLE {hdfs_file}"""
        cursor = hive.Connection("master1.internal", 10000, database="bigg").cursor()
        cursor.execute(create_table_hbase)
        cursor.execute(save_id_to_file)
        cursor.execute(remove_hbase_table)
        cursor.close()

        log = {
            "user": connection['user'],
            "frequency": frequency,
            "launched": datetime.datetime.utcnow(),
            "devices": {}
        }

        report_id = mongo[data_source['log']].insert_one(log)

        job_config = dict()
        job_config['connection'] = connection.copy()
        job_config['config'] = dict()
        job_config['config']['data_source'] = data_source
        job_config['config']['mongo_connection'] = config['mongo_db']
        job_config['config']['hbase_connection'] = config['hbase']
        job_config['job']['date_from'] = date_from
        job_config['job']['date_to'] = date_to
        job_config['job']['freq'] = frequency
        job_config['job']['report'] = report_id.inserted_id
        job_config['job']['version'] = version

        f = NamedTemporaryFile(delete=False, suffix='.json')
        f.write(pickle.dumps(job_config))
        f.close()

        MOUNTS = 'YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/hadoop_stack:/hadoop_stack:ro'
        IMAGE = 'YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=beerepo.tech.beegroup-cimne.com:5000/python3-mr'
        RUNTYPE = 'YARN_CONTAINER_RUNTIME_TYPE=docker'
        mr_job = Gemweb_gather(args=[
            '-r', 'hadoop', 'hdfs://{}'.format(f"/tmp/{hdfs_file}/"), '--file', f.name,
            '--file', 'utils.py#utils.py',
            '--jobconf', f'mapreduce.map.env={MOUNTS},{IMAGE},{RUNTYPE}',
            '--jobconf', f'mapreduce.reduce.env={MOUNTS},{IMAGE},{RUNTYPE}',
            '--jobconf', f'mapreduce.job.name=gemweb_import',
            '--jobconf', f'mapreduce.job.reduces={reduces}',
        ])

        with mr_job.make_runner() as runner:
            runner.run()

        up_conn = {"$set": {'timeseries.version': version}}
        mongo[data_source['info']].update_one({"_id": connection['_id']}, up_conn)
