import argparse
import hashlib
import os
import re

import openpyxl
import pandas as pd

import utils
from utils.cache import Cache
from utils.nomenclature import RAW_MODE

from sources.Bulgaria.harmonizer.mapper_buildings import harmonize_static, harmonize_kpi, harmonize_eem_kpi, harmonize_ts

energy_groups = [
    [r".*Hard.*", r".*Gas.*", r".*Others.*", r".*Heat.*", r".*Electricity.*"],  # liquid
    [r".*Liquid.*", r".*Gas.*", r".*Others.*", r".*Heat.*", r".*Electricity.*"],  # hard
    [r".*Liquid.*", r".*Hard.*", r".*Others.*", r".*Heat.*", r".*Electricity.*"],  # gas
    [r".*Liquid.*", r".*Hard.*", r".*Gas.*", r".*Heat.*", r".*Electricity.*"],  # others
    [r".*Liquid.*", r".*Hard.*", r".*Gas.*", r".*Others.*", r".*Electricity.*"],  # heat
    [r".*Liquid.*", r".*Hard.*", r".*Gas.*", r".*Others.*", r".*Heat.*"]  # electricity

]


def gather_data(config, settings, args):
    for file in os.listdir(args.file):
        if file.endswith('.xlsx') and not file.startswith("~$"):
            path = f"{args.file}/{file}"
            df = pd.read_excel(path, header=list(range(4)), sheet_name="List_residential_pilots")
            df.columns = ["_".join([f for f in c if not re.match("Unnamed:.*", f)]) for c in df.columns]
            df['filename'] = hashlib.md5(file.encode()).hexdigest()
            df['id'] = df.index
            df = df.iloc[:600]
            save_data(data=df.to_dict(orient='records'), data_type="BuildingInfo",
                      row_keys=["filename", "id"],
                      column_map=[("info", "all")], config=config, settings=settings, args=args)
            for s in range(0, 2):
                save_data(data=df.to_dict(orient='records'), data_type=f"BuildingInfo_KPI_{s}",
                          row_keys=["filename", "id"],
                          column_map=[("info", "all")], config=config, settings=settings, args=args)
            for s in range(0, 25):
                save_data(data=df.to_dict(orient='records'), data_type=f"EEM_KPI_{s}",
                          row_keys=["filename", "id"],
                          column_map=[("info", "all")], config=config, settings=settings, args=args)
            for g in energy_groups:
                df_source = df.copy(deep=True)
                df_source = df_source[[x for x in df_source.columns if not any([True if re.search(x1, x) else False
                                                                                for x1 in g])]]
                save_data(data=df_source.to_dict(orient='records'), data_type="BuildingEnergyConsumption",
                          row_keys=["filename", "id"],
                          column_map=[("info", "all")], config=config, settings=settings, args=args)


def save_data(data, data_type, row_keys, column_map, config, settings, args):
    if args.store == "direct":
        Cache.load_cache()
        params = {
                "namespace": args.namespace,
                "user": args.user,
                "config": config,
                "collection_type": data_type
             }
        if data_type == 'BuildingInfo':
            return harmonize_static(data, **params)
        if re.search(r"BuildingInfo_KPI_.*", data_type):
            return harmonize_kpi(data, split=int(data_type.split("_")[2]), **params)
        if re.search(r"EEM_KPI_.*", data_type):
            return harmonize_eem_kpi(data, split=int(data_type.split("_")[2]), **params)
        if data_type == 'BuildingEnergyConsumption':
            return harmonize_ts(data, **params)

    elif args.store == "kafka":
        try:
            k_topic = config["kafka"]["topic"]
            kafka_message = {
                "namespace": args.namespace,
                "user": args.user,
                "collection_type": data_type,
                "source": config['source'],
                "row_keys": row_keys,
                "data": data
            }
            utils.kafka.save_to_kafka(topic=k_topic, info_document=kafka_message,
                                      config=config['kafka']['connection'], batch=settings.kafka_message_size)

        except Exception as e:
            utils.utils.log_string(f"error when sending message: {e}")

    elif args.store == "hbase":

        try:
            h_table_name = utils.nomenclature.raw_nomenclature("Bulgaria", RAW_MODE.STATIC, data_type=data_type,
                                                               frequency="", user=args.user)

            utils.hbase.save_to_hbase(data, h_table_name, config['hbase_store_raw_data'], column_map,
                                      row_fields=row_keys)
        except Exception as e:
            utils.utils.log_string(f"Error saving datadis supplies to HBASE: {e}")
    else:
        utils.utils.log_string(f"store {config['store']} is not supported")


def gather(arguments, settings, config):
    ap = argparse.ArgumentParser(description='Gathering data from Bulgaria')
    ap.add_argument("-st", "--store", required=True, help="Where to store the data", choices=["direct", "kafka", "hbase"])
    ap.add_argument("--user", "-u", help="The user importing the data", required=True)
    ap.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    ap.add_argument("-f", "--file", help="Excel file path to parse", required=True)
    args = ap.parse_args(arguments)

    gather_data(config=config, settings=settings, args=args)
