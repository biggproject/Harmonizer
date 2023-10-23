import argparse
from datetime import datetime
import pandas as pd
from neo4j import GraphDatabase
from utils.neo4j import get_all_linked_weather_stations
from utils.mongo import mongo_logger
import pickle
from tempfile import NamedTemporaryFile
from utils.hdfs import put_file_to_hdfs, remove_file, remove_file_from_hdfs, generate_input_tsv
from utils.utils import log_string
from .weather_gather_mr import WeatherMRJob


def get_weather_stations(neo4j, ns_mappings):
    driver = GraphDatabase.driver(**neo4j)
    with driver.session() as session:
        location = get_all_linked_weather_stations(session, ns_mappings)

        df_loc = pd.DataFrame.from_records(location)
        df_loc.latitude = df_loc.latitude.apply(lambda x: f"{float(x):.3f}")
        df_loc.longitude = df_loc.longitude.apply(lambda x: f"{float(x):.3f}")
        df_loc.drop_duplicates(inplace=True)
    return df_loc


def get_timeseries_data(config, settings):

    # generate config file
    mongo_logger.create(config['mongo_db'], config['data_sources'][config['source']]['log'], 'gather',
                        log_exec=datetime.utcnow())
    job_config = config.copy()
    job_config.update({"kafka_message_size": settings.kafka_message_size, "mongo_logger": mongo_logger.export_log()})
    config_file = NamedTemporaryFile(delete=False, prefix='config_job_', suffix='.pickle')
    config_file.write(pickle.dumps(job_config))
    config_file.close()

    # Get all CP to generate the MR input file
    stations = get_weather_stations(config['neo4j'], settings.namespace_mappings)
    local_input = generate_input_tsv(stations.to_dict(orient="records"), ["latitude", "longitude"])
    input_mr = put_file_to_hdfs(source_file_path=local_input, destination_file_path='/tmp/weather_tmp/')
    remove_file(local_input)
    # Map Reduce
    MOUNTS = 'YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/hadoop_stack:/hadoop_stack:ro'
    IMAGE = 'YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=docker.tech.beegroup-cimne.com/mr/mr-weather'
    RUNTYPE = 'YARN_CONTAINER_RUNTIME_TYPE=docker'

    weather_job = WeatherMRJob(args=[
        '-r', 'hadoop', 'hdfs://{}'.format(input_mr),
        '--file', config_file.name,
        '--file', 'utils#utils',
        '--jobconf', f'mapreduce.map.env={MOUNTS},{IMAGE},{RUNTYPE}',
        '--jobconf', f'mapreduce.reduce.env={MOUNTS},{IMAGE},{RUNTYPE}',
        '--jobconf', f"mapreduce.job.name=weather_import",
        '--jobconf', f'mapreduce.job.maps=8'
    ])
    try:
        with weather_job.make_runner() as runner:
            runner.run()
        remove_file_from_hdfs(input_mr)
        remove_file(config_file.name)
    except Exception as e:
        log_string(f"error in map_reduce: {e}", mongo=False)
        remove_file_from_hdfs(input_mr)
        remove_file(config_file.name)


def gather(arguments, config=None, settings=None):
    ap = argparse.ArgumentParser(description='Gathering weather data from CAMS, DARKSKY and METEOGALICIA')
    args = ap.parse_args(arguments)
    get_timeseries_data(config, settings)
