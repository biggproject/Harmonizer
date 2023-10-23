import argparse
from datetime import datetime
import pandas as pd
# import gemweb
from .gemweb_gather import get_data
import utils

data_types = {
    "entities": {
        "name": "entities",
        #"endpoint": gemweb.ENDPOINTS.GET_INVENTORY,
        "category": "entitats"
    },
    "buildings": {
        "name": "buildings",
        #"endpoint": gemweb.ENDPOINTS.GET_INVENTORY,
        "category": "centres_consum"
    },
    "supplies": {
        "name": "supplies",
        #"endpoint": gemweb.ENDPOINTS.GET_INVENTORY,
        "category": "subministraments"
    },
    "solarpv": {
        "name": "solarpv",
        #"endpoint": gemweb.ENDPOINTS.GET_INVENTORY,
        "category": "instalacions_solars"
    },
    # "invoices": {
    #     "name": "invoices",
    #     "endpoint": gemweb.ENDPOINTS.GET_INVENTORY,
    #     "category": "factures"
    # }
}


def gather(arguments, config=None, settings=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("-st", "--store", required=True, help="Where to store the data", choices=["kafka", "hbase"])
    args = ap.parse_args(arguments)
    # Connection to neo4j to get all GemwebSources user,passwords and main org
    # TODO: Query Neo4j to get connection data
    gemweb_connections = [{"username": "icaen_api", "password": "", "namespace": "https://icaen.cat#",
                           "user": "icaen"}]
    mongo_logger = utils.mongo.mongo_logger
    for connection in gemweb_connections:
        mongo_logger.create(config['mongo_db'], config['data_sources'][config['source']]['log'], 'gather',
                            user=connection['user'], datasource_user=connection['username'],
                            log_exec=datetime.utcnow())
        try:
            # TODO: LOGIN TO GEMWEB
            gemweb = config['data_sources'][config['source']]['special_gemweb_data']
            # gemweb.gemweb.connection(connection['username'], connection['password'], timezone="UTC")
        except Exception as e:
            utils.utils.log_string(f"log in to gemweb API error: {e}")
            continue
        data = {}
        for t in data_types:
            try:
                data[t] = get_data(gemweb, data_types[t])
            except Exception as e:
                utils.utils.log_string(f"gemweb data from entity {t} obtained error: {e}")
        if args.store == "kafka":
            k_topic = config["kafka"]["topic"]
            for d_t in data:
                try:
                    kafka_message = {
                        "namespace": connection["namespace"],
                        "user": connection["user"],
                        "collection_type": d_t,
                        "source": config['source'],
                        "row_keys": ["id"],
                        "logger": mongo_logger.export_log(),
                        "data": data[d_t]
                    }
                    utils.kafka.save_to_kafka(topic=k_topic, info_document=kafka_message,
                                              config=config['kafka']['connection'], batch=settings.kafka_message_size)
                except Exception as e:
                    utils.utils.log_string(f"error when sending data: {e}")
            if any(['buildings' not in data, 'supplies' not in data]):
                continue
            try:
                supplies_df = pd.DataFrame.from_records(data['supplies'])
                buildings_df = pd.DataFrame.from_records(data['buildings'])
                buildings_df.set_index("id", inplace=True)
                df = supplies_df.join(buildings_df, on='id_centres_consum', lsuffix="supply", rsuffix="building")
                df.rename(columns={"id": "dev_gem_id"}, inplace=True)
            except Exception as e:
                utils.utils.log_string(f"error joining dataframes: {e}")
                continue
            try:
                data = df.to_dict(orient="records")
                kafka_message = {
                    "namespace": connection["namespace"],
                    "user": connection["user"],
                    "collection_type": "harmonize",
                    "source": config['source'],
                    "row_keys": ["id"],
                    "logger": mongo_logger.export_log(),
                    "data": data
                }
                utils.kafka.save_to_kafka(topic=k_topic, info_document=kafka_message,
                                          config=config['kafka']['connection'], batch=settings.kafka_message_size)
            except Exception as e:
                utils.utils.log_string(f"error when sending message: {e}")
