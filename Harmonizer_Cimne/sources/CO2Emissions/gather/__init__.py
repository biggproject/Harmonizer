import argparse
from datetime import datetime

import pandas as pd

import utils


def gather(arguments, config=None, settings=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--file", required=True, help="Excel file path to parse")
    ap.add_argument("--user", "-u", help="The user importing the data", required=True)
    ap.add_argument("--date_ini", "-di", help="The data where the co2 emissions starts", required=True)
    ap.add_argument("--date_end", "-de", help="The data where the co2 emissions ends", required=True)
    ap.add_argument("--co2_uid", "-co2", help="The id to identify the co2 emissions", required=True)
    ap.add_argument("--measured_property", "-mp", help="the type of the timeseries", required=True)
    ap.add_argument("--co2_property", "-cp", help="The property that the co2 is related", required=True)
    ap.add_argument("--co2_property_unit", "-cpu", help="the unit of the co2 related property", required=True)
    ap.add_argument("--unit", "-unit", help="the co2 unit", required=True)
    ap.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    ap.add_argument("-st", "--store", required=True, help="Where to store the data", choices=["kafka", "hbase"])
    args = ap.parse_args(arguments)
    mongo_logger = utils.mongo.mongo_logger
    mongo_logger.create(config['mongo_db'], config['data_sources'][config['source']]['log'], 'gather', user="",
                        log_exec=datetime.utcnow())

    """
python3 -m gather -so CO2Emissions -f data/CO2Emissions/EMISSIONS_FACT_ELECSP_test01.xlsx -u icaen -di 2015-01-01 -de 2030-01-01 --co2_uid cataloniaElectric -mp "http://bigg-project.eu/ontology#CO2Emissions" -cp "http://bigg-project.eu/ontology#EnergyConsumptionGridElectricity" -cpu "http://qudt.org/vocab/unit/KiloW-HR" -unit "http://bigg-project.eu/ontology#KiloGM-CO2" -n "https://icaen.cat#" -st kafka 
python3 -m gather -so CO2Emissions -f data/CO2Emissions/EMISSIONS_FACT_GASNAT_test01.xlsx -u icaen -di 2015-01-01 -de 2030-01-01 --co2_uid cataloniaGas -mp "http://bigg-project.eu/ontology#CO2Emissions" -cp "http://bigg-project.eu/ontology#EnergyConsumptionGas" -cpu "http://qudt.org/vocab/unit/KiloW-HR" -unit "http://bigg-project.eu/ontology#KiloGM-CO2" -n "https://icaen.cat#" -st kafka 
    """
    try:
        tariff = pd.read_excel(args.file, engine="openpyxl")
    except Exception as e:
        tariff = []
        utils.utils.log_string(f"could not parse file: {e}")
        exit(1)

    if len(tariff) != 8784:
        mongo_logger.log_string(f"the file is not correct")
        exit(0)
    date_ini = datetime.fromisoformat(args.date_ini)
    date_end = datetime.fromisoformat(args.date_end)
    tariff.loc[:, 'row_index'] = tariff.apply(lambda x:
                                              f"{args.co2_uid}-{date_ini.strftime('%Y%m%d')}-{date_end.strftime('%Y%m%d')}~{x.name}",
                                              axis=1)
    tariff.loc[:, 'pos'] = tariff.index
    if args.store == "kafka":
        try:
            utils.utils.log_string(f"saving to kafka", mongo=False)
            kafka_message = {
                "namespace": args.namespace,
                "collection_type": "co2_ts",
                "source": config['source'],
                "user": args.user,
                "row_keys": ["row_index"],
                "date_ini": date_ini,
                "date_end": date_end,
                "co2_uid": args.co2_uid,
                "measured_property": args.measured_property,
                "co2_property": args.co2_property,
                "co2_property_unit": args.co2_property_unit,
                "unit": args.unit,
                "logger": mongo_logger.export_log(),
                "data": tariff.to_dict(orient="records")
            }
            k_harmonize_topic = config["kafka"]["topic"]
            utils.kafka.save_to_kafka(topic=k_harmonize_topic, info_document=kafka_message,
                                      config=config['kafka']['connection'], batch=settings.kafka_message_size)
        except Exception as e:
            utils.utils.log_string(f"error when sending message: {e}")
    elif args.store == "hbase":
        utils.utils.log_string(f"saving to hbase", mongo=False)
        prop = str(args.measured_property).split('#')[1]
        try:
            h_table_name = f"raw_co2emissions_{prop}_co2_PT1H_{args.user}"
            utils.hbase.save_to_hbase(tariff.to_dict(orient="records"), h_table_name, config['hbase_store_raw_data'],
                                      [("info", "all")],
                                      row_fields=["row_index"])
        except Exception as e:
            utils.utils.log_string(f"error saving to hbase: {e}")
    else:
        utils.utils.log_string(f"store {args.store} is not supported")
