import argparse
import re
import utils
from utils.cache import Cache
from utils.utils import log_string
from .mapper_static import harmonize_data


def harmonize_command_line(arguments, config=None, settings=None):
    ap = argparse.ArgumentParser(description='Mapping of EEM data to neo4j.')
    ap.add_argument("--user", "-u", help="The user importing the data", required=True)
    ap.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    args = ap.parse_args(arguments)

    hbase_conn = config['hbase_store_raw_data']
    hbase_table = f"raw_Genercat_static_eem__{args.user}"
    i = 0
    Cache.load_cache()
    for data in utils.hbase.get_hbase_data_batch(hbase_conn, hbase_table, batch_size=100):
        dic_list = []
        log_string("parsing hbase", mongo=False)
        for id_, x in data:
            item = dict()
            for k, v in x.items():
                k1 = re.sub("^info:", "", k.decode())
                item[k1] = v
            item.update({"id_": id_})
            dic_list.append(item)
        i += len(dic_list)
        log_string(i, mongo=False)
        harmonize_data(dic_list, namespace=args.namespace, user=args.user, config=config)
