import argparse
import re
import pandas as pd
import utils
from utils.cache import Cache
from utils.utils import log_string
from datetime import datetime
from .mapper import harmonize_data_ts


def harmonize_command_line(arguments, config=None, settings=None):
    ap = argparse.ArgumentParser(description='Mapping of Tariff data to neo4j.')
    ap.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    ap.add_argument("--user", "-u", help="The user importing the data", required=True)
    ap.add_argument("--measured_property", "-mp", help="The type of the data measured", required=True)
    ap.add_argument("--co2_property", "-p", help="The type of the data that the price relates to", required=True)
    ap.add_argument("--co2_property_unit", "-pu", help="The unit of the related type", required=True)
    ap.add_argument("--unit", "-unit", help="The currency unit", required=True)
    args = ap.parse_args(arguments)

    hbase_conn = config['hbase_store_raw_data']
    i = 0
    hbase_table = f"raw_co2emissions_ts_{args.co2_property.split('#')[1]}_PT1H_{args.user}"
    Cache.load_cache()
    # TODO: Vigilar perque només funciona amb 1 key, però com que la UI ho fa diferent de moment ho deixem aixi
    for data in utils.hbase.get_hbase_data_batch(hbase_conn, hbase_table, batch_size=100000):
        dic_list = []
        for key, data1 in data:
            item = dict()
            unique_id, pos = key.decode().split("~")
            co2_uid, ts_ini, ts_end = unique_id.split("-")
            for k, v in data1.items():
                k1 = re.sub("^info:", "", k.decode())
                item[k1] = v
            dic_list.append(item)
        if len(dic_list) <= 0:
            continue
        i += len(dic_list)
        log_string(i, mongo=False)
        harmonize_data_ts(dic_list, namespace=args.namespace, config=config, co2_uid=co2_uid,
                          date_ini=datetime.strptime(ts_ini, "%Y%m%d"), date_end=datetime.strptime(ts_end, "%Y%m%d"),
                          measured_property=args.measured_property, co2_property=args.co2_property,
                          co2_property_unit=args.co2_property_unit, unit=args.unit, user=args.user)

