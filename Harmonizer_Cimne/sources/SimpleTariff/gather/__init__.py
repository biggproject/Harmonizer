import argparse
from datetime import datetime

import pandas as pd

import utils


def gather(arguments, config=None, settings=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--file", required=True, help="Excel file path to parse")
    ap.add_argument("--user", "-u", help="The user importing the data", required=True)
    ap.add_argument("--date_ini", "-di", help="The data where the tariff starts", required=True)
    ap.add_argument("--date_end", "-de", help="The data where the tariff ends", required=True)
    ap.add_argument("--tariff_uid", "-tar", help="The id to identify the tariff", required=True)
    ap.add_argument("--measured_property", "-mp", help="the type of the timeseries", required=True)
    ap.add_argument("--priced_property", "-pp", help="The property that the price is related", required=True)
    ap.add_argument("--priced_property_unit", "-ppu", help="the unit of the priced property", required=True)
    ap.add_argument("--currency_unit", "-cu", help="the currency unit", required=True)
    ap.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    ap.add_argument("-st", "--store", required=True, help="Where to store the data", choices=["kafka", "hbase"])
    args = ap.parse_args(arguments)
    mongo_logger = utils.mongo.mongo_logger
    mongo_logger.create(config['mongo_db'], config['data_sources'][config['source']]['log'], 'gather', user=args.user,
                        log_exec=datetime.utcnow())

    """
python3 -m gather -so SimpleTariff -f data/Tariff/Tariff_ELEC_test01.xlsx -u icaen -di 2015-01-01 -de 2030-01-01 -tar electricdefault -mp "http://bigg-project.eu/ontology#Price.EnergyPriceGridElectricity" -pp "http://bigg-project.eu/ontology#EnergyConsumptionGridElectricity" -ppu "http://qudt.org/vocab/unit/KiloW-HR" -cu "http://qudt.org/vocab/unit/Euro" -n "https://icaen.cat#" -st kafka
python3 -m gather -so SimpleTariff -f data/Tariff/Tariff_GASNAT_test01.xlsx -u icaen -di 2015-01-01 -de 2030-01-01 -tar gasdefault -mp "http://bigg-project.eu/ontology#Price.EnergyPriceGas" -pp "http://bigg-project.eu/ontology#EnergyConsumptionGas" -ppu "http://qudt.org/vocab/unit/KiloW-HR" -cu "http://qudt.org/vocab/unit/Euro" -n "https://icaen.cat#" -st kafka 
 
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
                                              f"{args.tariff_uid}-{date_ini.strftime('%Y%m%d')}-{date_end.strftime('%Y%m%d')}~{x.name}",
                                              axis=1)
    tariff.loc[:, 'pos'] = tariff.index
    if args.store == "kafka":
        try:
            utils.utils.log_string(f"saving to kafka", mongo=False)
            kafka_message = {
                "namespace": args.namespace,
                "user": args.user,
                "collection_type": "tariff_ts",
                "source": config['source'],
                "row_keys": ["row_index"],
                "date_ini": date_ini,
                "date_end": date_end,
                "tariff": args.tariff_uid,
                "measured_property": args.measured_property,
                "priced_property": args.priced_property,
                "priced_property_unit": args.priced_property_unit,
                "currency_unit": args.currency_unit,
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
            h_table_name = f"raw_simpletariff_ts_{prop}_PT1H_{args.user}"
            utils.hbase.save_to_hbase(tariff.to_dict(orient="records"), h_table_name, config['hbase_store_raw_data'],
                                      [("info", "all")],
                                      row_fields=["row_index"])
        except Exception as e:
            utils.utils.log_string(f"error saving to hbase: {e}")
    else:
        utils.utils.log_string(f"store {args.store} is not supported")
