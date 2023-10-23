import argparse
import re
import utils
from utils.cache import Cache
from utils.utils import log_string
from .mapper_static import harmonize_data


def harmonize_command_line(arguments, config=None, settings=None):
    ap = argparse.ArgumentParser(description='Mapping of opendata certificates data to neo4j.')
    ap.add_argument("--user", "-u", help="The user importing the data", required=True)
    ap.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    args = ap.parse_args(arguments)

    hbase_conn = config['hbase_store_raw_data']
    hbase_table = f"raw_OpenData_static_EnergyPerformanceCertificate__{args.user}"
    i = 0
    Cache.load_cache()

    for data in utils.hbase.get_hbase_data_batch(hbase_conn, hbase_table, batch_size=1000):
        dic_list = []
        log_string("parsing hbase", mongo=False)
        for key_info, x in data:
            item = dict()
            ref, n_cas = key_info.decode().split("~")
            for k, v in x.items():
                k1 = re.sub("^info:", "", k.decode())
                item[k1] = v
            item.update({"num_cas": n_cas})
            item.update({"referencia_cadastral": ref})
            dic_list.append(item)
        log_string("parsed. Mapping...", mongo=False)
        i += len(dic_list)
        log_string(i, mongo=False)
        harmonize_data(dic_list, namespace=args.namespace, user=args.user, config=config)
