import argparse
import pickle
from tempfile import NamedTemporaryFile
import utils
from utils.utils import log_string
from .datadis_utils import get_users, decrypt_passwords
from .datadis_gather_mr import DatadisMRJob


def get_timeseries_data(store, policy, config, settings):
    # generate config file
    job_config = config.copy()
    job_config.update({"store": store, "kafka_message_size": settings.kafka_message_size, "policy": policy})
    config_file = NamedTemporaryFile(delete=False, prefix='config_job_', suffix='.pickle')
    config_file.write(pickle.dumps(job_config))
    config_file.close()

    # Get Users to generate the MR input file
    users = get_users(config['neo4j'])
    users = decrypt_passwords(users, settings)
    local_input = utils.hdfs.generate_input_tsv(users, ["username", "password", "user", "namespace"])
    input_mr = utils.hdfs.put_file_to_hdfs(source_file_path=local_input, destination_file_path='/tmp/datadis_tmp/')
    utils.hdfs.remove_file(local_input)

    # Map Reduce
    MOUNTS = 'YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/hadoop_stack:/hadoop_stack:ro'
    IMAGE = 'YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=docker.tech.beegroup-cimne.com/jobs/importing_tool'
    RUNTYPE = 'YARN_CONTAINER_RUNTIME_TYPE=docker'

    datadis_job = DatadisMRJob(args=[
        '-r', 'hadoop', 'hdfs://{}'.format(input_mr),
        '--file', config_file.name,
        '--jobconf', f'mapreduce.map.env={MOUNTS},{IMAGE},{RUNTYPE}',
        '--jobconf', f'mapreduce.reduce.env={MOUNTS},{IMAGE},{RUNTYPE}',
        '--jobconf', f"mapreduce.job.name=datadis_import",
        '--jobconf', f'mapreduce.job.reduces=8'
    ])
    try:
        with datadis_job.make_runner() as runner:
            runner.run()
        utils.hdfs.remove_file_from_hdfs(input_mr)
        utils.hdfs.remove_file(config_file.name)
    except Exception as e:
        log_string(f"error in map_reduce: {e}")
        utils.hdfs.remove_file_from_hdfs(input_mr)
        utils.hdfs.remove_file(config_file.name)


def gather(arguments, config=None, settings=None):
    ap = argparse.ArgumentParser(description='Gathering data from Datadis')
    ap.add_argument("-st", "--store", required=True, help="Where to store the data", choices=["kafka", "hbase"])
    ap.add_argument("-p", "--policy", required=True, help="The policy for updating data.", choices=["last", "repair"])
    args = ap.parse_args(arguments)
    get_timeseries_data(args.store, args.policy, config, settings)
