import pandas as pd

from utils.cache import Cache
from utils.utils import log_string
import argparse
import re
import utils
from .mapper_static import harmonize_data


def harmonize_command_line(arguments, config=None, settings=None):
    ap = argparse.ArgumentParser(description='Mapping of Gemweb data to neo4j.')
    ap.add_argument("--user", "-u", help="The user importing the data", required=True)
    ap.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    args = ap.parse_args(arguments)

    hbase_conn = config['hbase_store_raw_data']
    building_table = f"raw_Gemweb_static_buildings__{args.user}"
    building_list = []
    Cache.load_cache()
    for data in utils.hbase.get_hbase_data_batch(hbase_conn, building_table):
        for n_ens, x in data:
            item = dict()
            for k, v in x.items():
                k1 = re.sub("^info:", "", k.decode())
                item[k1] = v
            item.update({"build_gem_id": n_ens.decode()})
            building_list.append(item)
        log_string("parsed. Mapping...", mongo=False)

    building_df = pd.DataFrame.from_records(building_list)
    building_df.set_index("build_gem_id", inplace=True)

    supp_table = f"raw_Gemweb_static_supplies__{args.user}"
    i = 0
    for data in utils.hbase.get_hbase_data_batch(hbase_conn, supp_table, batch_size=1000):
        supp_list = []
        for n_ens, x in data:
            item = dict()
            for k, v in x.items():
                k1 = re.sub("^info:", "", k.decode())
                item[k1] = v
            item.update({"dev_gem_id": n_ens.decode()})
            supp_list.append(item)
        log_string("parsed. Mapping...", mongo=False)
        i = i + len(supp_list)
        log_string(i, mongo=False)
        supplies_df = pd.DataFrame.from_records(supp_list)
        supplies_df['id_centres_consum'] = supplies_df['id_centres_consum'].apply(lambda idc: idc.decode())
        df = supplies_df.join(building_df, on='id_centres_consum', lsuffix="supply", rsuffix="building", how="left")
        harmonize_data(df.to_dict(orient="records"), namespace=args.namespace, user=args.user, config=config)

# if __name__ == "__main__":
#     pass
    # parser = argparse.ArgumentParser(description='Mapping of Gemweb data to neo4j.')
    # main_org_params = parser.add_argument_group("Organization",
    #                                             "Set the main organization information for importing the data")
    # main_org_params.add_argument("--user", "-u", help="The main organization name", required=True)
    # main_org_params.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    # main_org_params.add_argument("--type", "-t", help="The type to import [static] or [ts]", required=True)
    #
    # if os.getenv("PYCHARM_HOSTED"):
    #     args_t = ["-n", "http://icaen.cat#", "-u", "icaen", "-t", "ts"]
    #     args = parser.parse_args(args_t)
    # else:
    #     args = parser.parse_args()
    # # read config file
    # config = read_config(settings.conf_file)
    #
    #
    # hbase_conn = config['hbase_store_raw_data']
    #
    # if args.type == "static":
    #     # get supplies from HBASE
    #     supplies_table = f"{source}_supplies_{args.user}".lower()
    #     supplies_list = []
    #     for data in get_hbase_data_batch(hbase_conn, supplies_table, batch_size=100000):
    #         for gem_id, data1 in data:
    #             item = dict()
    #             for k, v in data1.items():
    #                 k1 = re.sub("^info:", "", k.decode())
    #                 item[k1] = v
    #             item.update({"dev_gem_id": gem_id})
    #             supplies_list.append(item)
    #     supplies_df = pd.DataFrame.from_records(supplies_list)
    #     df = supplies_df.join(building_df, on='id_centres_consum', lsuffix="supply", rsuffix="building")

    #     # get buildings from HBASE
    #     building_table = f"{source}_buildings_{args.user}".lower()
    #     building_list = []
    #     for data in get_hbase_data_batch(hbase_conn, building_table, batch_size=100000):
    #         for gem_id, data1 in data:
    #             item = dict()
    #             for k, v in data1.items():
    #                 k1 = re.sub("^info:", "", k.decode())
    #                 item[k1] = v
    #             item.update({"build_gem_id": gem_id})
    #             building_list.append(item)
    #     building_df = pd.DataFrame.from_records(building_list)
    #     building_df.set_index("build_gem_id", inplace=True)
    #
    #     # Join dataframes by link
    #     df = supplies_df.join(building_df, on='id_centres_consum', lsuffix="supply", rsuffix="building")
    #     map_data_static(df.to_dict(orient="records"), namespace=args.namespace,
    #                     user=args.user, config=config)
    #
    # elif args.type == "ts":
    #     hbase = happybase.Connection(**hbase_conn)
    #     ts_tables = [x for x in hbase.tables() if re.match(rf"{source}_data_.*", x.decode())]
    #     for h_table_name in ts_tables:
    #         freq = h_table_name.decode().split("_")[2]
    #         for data in get_hbase_data_batch(hbase_conn, h_table_name, batch_size=100000):
    #             data_list = []
    #             for key, row in data:
    #                 item = dict()
    #                 for k, v in row.items():
    #                     k1 = re.sub("^info:", "", k.decode())
    #                     k1 = re.sub("^v:", "", k1)
    #                     item[k1] = v
    #                 gem_id, ts = key.decode().split("~")
    #                 item.update({"gem_id": gem_id.encode("utf-8")})
    #                 item.update({"measurement_ini": ts.encode("utf-8")})
    #                 data_list.append(item)
    #             if len(data_list) <= 0:
    #                 continue
    #             map_data_ts(data_list, freq=freq, namespace=args.namespace, user=args.user, config=config)
    # else:
    #     raise(NotImplementedError("invalid type: [static, ts]"))
