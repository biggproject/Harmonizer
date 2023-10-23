import argparse
import re
import utils
from utils.cache import Cache
from utils.nomenclature import RAW_MODE
from .mapper_buildings import harmonize_all


def harmonize_command_line(arguments, config=None, settings=None):
    ap = argparse.ArgumentParser(description='Mapping of Bulgaria summary data to neo4j.')
    ap.add_argument("--user", "-u", help="The user importing the data", required=True)
    ap.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    args = ap.parse_args(arguments)

    hbase_conn = config['hbase_store_raw_data']
    Cache.load_cache()

    hbase_table = utils.nomenclature.raw_nomenclature("Bulgaria", RAW_MODE.STATIC, data_type="BuildingInfo",
                                                      user=args.user)
    i = 0
    for data in utils.hbase.get_hbase_data_batch(hbase_conn, hbase_table, batch_size=150):
        dic_list = []
        print("parsing hbase")
        for u_c, x in data:
            item = dict()
            for k, v in x.items():
                k1 = re.sub("^info:", "", k.decode())
                item[k1] = v
            filename, id = u_c.decode().split("~")
            item.update({"filename": filename})
            item.update({"id": id})
            dic_list.append(item)
        print(f"parsed. Mapping...{i}")
        i += len(dic_list)
        harmonize_all(dic_list, namespace=args.namespace, user=args.user, config=config)

