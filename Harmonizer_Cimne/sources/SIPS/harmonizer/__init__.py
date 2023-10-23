import argparse
import re
import utils
from utils.cache import Cache

from .mapper_static import harmonize_ps_electric_data, harmonize_ps_gas_data
from .mapper_ts import harmonize_ts_electric_data, harmonize_ts_gas_data


def harmonize_command_line(arguments, config=None, settings=None):
    ap = argparse.ArgumentParser(description='Mapping of GPG data to neo4j.')
    ap.add_argument("--user", "-u", help="The user importing the data", required=True)
    ap.add_argument("-tycons", "--type_consumption", choices=["electricity", "gas"], required=True)
    ap.add_argument("-tydata", "--type_data", help="select if you want import PS info o consumptions",
                    choices=["ps_info", "consumption"], required=True)
    args = ap.parse_args(arguments)
    hbase_conn = config['hbase_store_raw_data']
    type_part = 'ts' if args.type_data == 'consumption' else 'static'
    type_element = 'Invoices' if args.type_data == 'consumption' else 'Supplies'
    cons_element = 'electric' if args.type_consumption == 'electricity' else 'gas'
    hbase_table = f"raw_SIPS_{type_part}_{cons_element}{type_element}__{args.user}"
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
            if args.data_type == 'ps_info':
                cups = u_c.decode()
                item.update({"cups": cups})
            elif args.data_type == 'consumption':
                cups, ts_ini = u_c.decode().split("~")
                item.update({"cups": cups})
                item.update({"fechaInicioMesConsumo": ts_ini})
            dic_list.append(item)
        print("parsed. Mapping...")
        i += len(dic_list)
        print(i)
        if args.type_consumption == "ps_info" and args.type_data == "electricity":
            harmonize_ps_electric_data(dic_list, namespace=args.namespace, user=args.user, config=config)
        elif args.type_consumption == "ps_info" and args.type_data == "gas":
            harmonize_ps_gas_data(dic_list, namespace=args.namespace, user=args.user, config=config)
        elif args.type_consumption == "consumptions" and args.type_data == "electricity":
            harmonize_ts_electric_data(dic_list, namespace=args.namespace, user=args.user, config=config)
        elif args.type_consumption == "consumptions" and args.type_data == "gas":
            harmonize_ts_gas_data(dic_list, namespace=args.namespace, user=args.user, config=config)
