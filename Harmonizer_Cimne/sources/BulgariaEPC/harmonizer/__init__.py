import argparse
import re
import utils
from utils.cache import Cache


def harmonize_command_line(arguments, config=None, settings=None):
    ap = argparse.ArgumentParser(description='Mapping of GPG data to neo4j.')
    # Configure args
    args = ap.parse_args(arguments)
    hbase_conn = config['hbase_store_raw_data']
    # SET HBASE TABLE TO READ
    i = 0
    Cache.load_cache()
    for data in utils.hbase.get_hbase_data_batch(hbase_conn, hbase_table, batch_size=1000):
        dic_list = []
        print("parsing hbase")
        for u_c, x in data:
            item = dict()
            for k, v in x.items():
                k1 = re.sub("^info:", "", k.decode())
                item[k1] = v
            # SET FIELDS BASED ON KEY
            dic_list.append(item)
        print("parsed. Mapping...")
        i += len(dic_list)
        print(i)
        # CALL HARMONIZER WITH DATA
