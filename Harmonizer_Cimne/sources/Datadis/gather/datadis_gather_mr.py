from datetime import datetime, timedelta
import glob
import pickle
import bson
import sys
from abc import ABC
import pandas as pd
import pytz
import utils
from mrjob.job import MRJob
from beedis import ENDPOINTS, datadis
from dateutil.relativedelta import relativedelta
from neo4j import GraphDatabase

TZ = pytz.timezone("Europe/Madrid")


class LoginException(Exception):
    pass


class GetDataException(Exception):
    pass


def login(username, password):
    try:
        datadis.connection(username=username, password=password, timeout=100)
    except Exception as e:
        raise LoginException(f"{e}")


def save_datadis_data(data, credentials, data_type, row_keys, column_map, config):
    if config['store'] == "kafka":
        utils.utils.log_string(f"saving to kafka", mongo=False)
        try:
            k_topic = config["kafka"]["topic"]
            kafka_message = {
                "namespace": credentials["namespace"],
                "user": credentials["user"],
                "collection_type": data_type,
                "source": config['source'],
                "row_keys": row_keys,
                "logger": utils.mongo.mongo_logger.export_log(),
                "data": data
            }
            utils.kafka.save_to_kafka(topic=k_topic, info_document=kafka_message,
                                      config=config['kafka']['connection'], batch=config["kafka_message_size"])

        except Exception as e:
            utils.utils.log_string(f"error when sending message: {e}")
    elif config['store'] == "hbase":
        utils.utils.log_string(f"saving to hbase", mongo=False)
        try:
            type_d = "static" if data_type == "supplies" else "ts"
            data_type = "supplies_" if data_type == "supplies" else data_type
            h_table_name = f"raw_{config['source']}_{type_d}_{data_type}_{credentials['user']}"
            utils.hbase.save_to_hbase(data, h_table_name, config['hbase_store_raw_data'], column_map,
                                      row_fields=row_keys)
        except Exception as e:
            utils.utils.log_string(f"Error saving datadis supplies to HBASE: {e}")
    else:
        utils.utils.log_string(f"store {config['store']} is not supported")


# only use with less than 1 year
def get_1h_in_period(init_time, end_time):
    init_time = TZ.localize(init_time)
    end_time = TZ.localize(end_time)
    return (end_time - init_time).days * 24 + (end_time - init_time).seconds // 3600


def get_15min_in_period(init_time, end_time):
    init_time = TZ.localize(init_time)
    end_time = TZ.localize(end_time)
    return (end_time - init_time).days * 96 + (end_time - init_time).seconds // 900


def get_1m_in_period(init_time, end_time):
    r = relativedelta(end_time, init_time - relativedelta(months=1))
    return r.months + (12 * r.years)


def parse_max_power_chunk(max_power):
    if len(max_power) <= 0:
        return list()
    try:
        df_consumption = pd.DataFrame(max_power)
        # Cast datetime64[ns] to timestamp (int64)
        df_consumption.set_index('datetime', inplace=True)
        df_consumption.sort_index(inplace=True)

        for c in [x for x in df_consumption.columns if x.startswith("datetime_")]:
            df_consumption[c] = df_consumption[c].astype('int64') // 10 ** 9

        df_consumption['timestamp'] = df_consumption.index.astype('int64') // 10 ** 9
        m_power = df_consumption.to_dict(orient='records')
        for x in m_power:
            for c in [_ for _ in x.keys() if _.startswith("datetime_")]:
                if x[c] == -9223372037:
                    period = c.split("_")[2]
                    x.pop(f"datetime_period_{period}")
                    x.pop(f"maxPower_period_{period}")

        return m_power
    except Exception as e:
        utils.utils.log_string(f"Received an exception when parsing max power data: {e}")
        return list()


def parse_consumption_chunk(consumption):
    if len(consumption) <= 0:
        return list()
    try:
        df_consumption = pd.DataFrame(consumption)
        df_consumption.index = df_consumption['datetime']
        df_consumption.sort_index(inplace=True)
        # Cast datetime64[ns] to timestamp (int64)
        df_consumption['timestamp'] = df_consumption['datetime'].astype('int64') // 10 ** 9
        return df_consumption.to_dict(orient='records')
    except Exception as e:
        utils.utils.log_string(f"Received and exception when parsing consumption data: {e}")
        return list()


data_types_dict = {
    "EnergyConsumptionGridElectricity_PT1H": {
        "mongo_collection": "data_1h",
        "type_data": "timeseries",
        "freq_rec": relativedelta(day=31, hour=23, minute=59, second=59),
        "measurement_type": "0",
        "endpoint": ENDPOINTS.GET_CONSUMPTION,
        "params": ["cups", "distributor_code", "start_date", "end_date", "measurement_type", "point_type"],
        "elements_in_period": get_1h_in_period,
        "parser": parse_consumption_chunk,
    },
    "EnergyConsumptionGridElectricity_PT15M": {
        "mongo_collection": "data_15m",
        "type_data": "timeseries",
        "freq_rec": relativedelta(day=31, hour=23, minute=59, second=59),
        "measurement_type": "1",
        "endpoint": ENDPOINTS.GET_CONSUMPTION,
        "params": ["cups", "distributor_code", "start_date", "end_date", "measurement_type", "point_type"],
        "elements_in_period": get_15min_in_period,
        "parser": parse_consumption_chunk,
    },
    "Power_P1M": {
        "mongo_collection": "max_power",
        "type_data": "timeseries",
        "freq_rec": relativedelta(months=5) + relativedelta(day=31, hour=23, minute=59, second=59),
        "endpoint": ENDPOINTS.GET_MAX_POWER,
        "params": ["cups", "distributor_code", "start_date", "end_date"],
        "elements_in_period": get_1m_in_period,
        "parser": parse_max_power_chunk,
    },
    # "contracts": {
    #     "freq_rec": "static",
    #     "endpoint": ENDPOINTS.GET_CONTRACT,
    #     "params": ["cups", "distributor_code"]
    # }
}


def parse_arguments(row, type_params, date_ini, date_end):
    arguments = {}
    for a in type_params['params']:
        if a == "cups":
            arguments["cups"] = row["cups"]
        elif a == "distributor_code":
            arguments["distributor_code"] = row['distributorCode']
        elif a == "start_date":
            arguments["start_date"] = date_ini
        elif a == "end_date":
            arguments["end_date"] = date_end
        elif a == "measurement_type":
            arguments["measurement_type"] = type_params['measurement_type']
        elif a == "point_type":
            arguments["point_type"] = str(row["pointType"])
    return arguments


def download_chunk(supply, type_params, credentials, status):
    try:
        date_ini_req = status['date_ini_block'].date()
        date_end_req = status['date_end_block'].date()
        utils.utils.log_string(f"{credentials['username']} with {supply['cups']} is obtaining from {date_ini_req} to {date_end_req}", mongo=False)

        kwargs = parse_arguments(supply, type_params, date_ini_req, date_end_req)
        consumption = datadis.datadis_query(type_params['endpoint'], **kwargs)
        if not consumption:
            raise GetDataException(f"No data could be found")
        return consumption
    except Exception as e:
        utils.utils.log_string(f"Error obtaining {credentials['username']} device {supply['cups']} from {date_ini_req} to {date_end_req}: {e}")
        return list()

class DatadisMRJob(MRJob, ABC):
    def __read_config__(self):
        fn = glob.glob('*.pickle')
        self.config = pickle.load(open(fn[0], 'rb'))

    def mapper_init(self):
        self.__read_config__()

    def mapper(self, _, line):
        credentials = {k: v for k, v in zip(["username", "password", "user", "namespace"], line.split('\t'))}
        utils.mongo.mongo_logger.create(self.config['mongo_db'],
                                        self.config['data_sources'][self.config['source']]['log'],
                                        'gather', user=credentials["user"], datasource_user=credentials["username"],
                                        log_exec=datetime.utcnow())
        try:
            utils.utils.log_string(f"User: {credentials['username']}", mongo=False)
            login(credentials["username"], credentials["password"])

            # Obtain supplies from the user logged
            supplies = datadis.datadis_query(ENDPOINTS.GET_SUPPLIES)
            if not supplies:
                utils.utils.log_string(f"The user {credentials['username']} has 0 supplies")
                return
            utils.utils.log_string(f"The user {credentials['username']} has {len(supplies)}", mongo=False)
            for supply in supplies:
                supply.update({"nif": credentials['username']})
            save_datadis_data(supplies, credentials, "supplies", ["cups"], [("info", "all")], self.config)
            log_exported = utils.mongo.mongo_logger.export_log()
            log_exported['log_id'] = str(log_exported['log_id'])
            # TODO: remove this after fast load
            driver = GraphDatabase.driver(**self.config['neo4j'])
            with driver.session() as session:
                supplies_linked = [v['d.bigg__deviceName'] for v in session.run(f"""
                    Match(s:DatadisSource{{username: "{credentials['username']}"}}) 
                    Match(d:bigg__Device{{source:"DatadisSource"}})<-[:bigg__isObservedByDevice]-(bs) 
                    where (d)-[:importedFromSource]->(s)  
                    return d.bigg__deviceName""").data()]
            supplies = [x for x in supplies if x["cups"] in supplies_linked]
            utils.utils.log_string(f"The user {credentials['username']} has {len(supplies)} after filter", mongo=False)
            for i, supply in enumerate(supplies):
                key = i % 8  # num reducers
                value = {"supply": supply, "credentials": credentials, "logger": log_exported}
                yield key, value

        except LoginException as e:
            utils.utils.log_string(f"Error in login to datadis for user {credentials['username']}: {e}")
        except Exception as e:
            utils.utils.log_string(f"Received and exception: {e}")

    def reducer_init(self):
        self.__read_config__()

    def reducer(self, key, values):
        # Loop supplies
        values = list(values)
        for info in values:
            utils.utils.log_string(f"Gathering: {info['supply']['cups']}\n")
            supply = info['supply']
            credentials = info['credentials']
            import_log = info['logger']
            import_log['log_id'] = bson.objectid.ObjectId(import_log['log_id'])
            utils.mongo.mongo_logger.import_log(import_log, "gather")
            datadis_devices = (
                utils.mongo.mongo_connection(self.config['mongo_db'])
                [self.config['data_sources'][self.config['source']]['log_devices']])
            # get the highest page document log
            try:
                device = datadis_devices.find_one({"_id": supply['cups']})
            except IndexError:
                device = None
            if not device:
                # if there is no log document create a new one
                device = {
                    "_id": supply['cups']
                }
            # create the data chunks we will gather the information for timeseries
            has_init_date = True
            try:
                date_ini = datetime.strptime(supply['validDateFrom'][:-2], '%Y/%m').date()
            except ValueError:
                has_init_date = False
                date_ini = datetime(2018, 1, 1)
            # get last minute of current month
            now = datetime.today().date() + relativedelta(day=31, hour=23, minute=59, second=59)
            try:
                date_end = datetime.strptime(supply['validDateTo'][:-2], '%Y/%m') + \
                           relativedelta(day=31, hour=23, minute=59, second=59)
                if date_end <= now:
                    date_end = date_end
                    finished = True
                else:
                    raise Exception()
            except Exception as e:
                date_end = now
                finished = False

            for t, type_params in [(x, y) for x, y in data_types_dict.items() if y["type_data"] == "timeseries"]:
                if type_params["mongo_collection"] not in device:
                    device[type_params["mongo_collection"]] = {}
                loop_date_ini = date_ini
                while loop_date_ini < date_end:
                    current_ini = loop_date_ini
                    current_end = current_ini + type_params['freq_rec']
                    k = "~".join([current_ini.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")])
                    date_ini_block = current_ini
                    date_end_block = current_end
                    if k not in device[type_params["mongo_collection"]]:
                        device[type_params["mongo_collection"]].update({
                            k: {
                                "has_ini_date": has_init_date,
                                "date_ini_block": date_ini_block,
                                "date_end_block": date_end_block,
                                "values": 0,
                                "total": type_params['elements_in_period'](date_ini_block, date_end_block +
                                                                           relativedelta(seconds=1)),
                                "retries": 6,
                            }
                        })
                    loop_date_ini = current_end + relativedelta(seconds=1)
            try:
                login(username=credentials['username'], password=credentials['password'])
            except LoginException as e:
                utils.utils.log_string(f"Error in login to datadis for user {credentials['username']} IN REDUCER: {e}")
            for data_type, type_params in data_types_dict.items():
                if type_params['type_data'] == "timeseries":
                    if self.config['policy'] == "last":
                        try:
                            # get last chunk
                            status = list(device[type_params["mongo_collection"]].values())[-1]
                        except IndexError as e:
                            continue
                        # check if chunk is in current time
                        if not(status['date_ini_block'] <= datetime.today()
                               <= status['date_end_block']):
                            continue
                        data = download_chunk(supply, type_params, credentials, status)
                        self.increment_counter('gathered', 'device', 1)
                        data_df = type_params['parser'](data)
                        if len(data_df) > 0:
                            save_datadis_data(data_df, credentials, data_type,
                                              ["cups", "timestamp"], [("info", "all")], self.config)
                            status['date_min'] = pd.to_datetime(data_df[0]['timestamp'], unit="s").tz_localize(pytz.UTC)
                            status['date_max'] = pd.to_datetime(data_df[-1]['timestamp'], unit="s"). \
                                tz_localize(pytz.UTC)
                        # store status info
                        status['values'] = len(data_df)
                        self.increment_counter('gathered', 'device', 1)
                    if self.config['policy'] == "repair":
                        # get all incomplete chunks
                        status_list = [x for x in device[type_params["mongo_collection"]].values()
                                       if x['values'] < x['total'] and x['retries'] > 0]
                        for status in status_list:
                            data = download_chunk(supply, type_params, credentials, status)
                            self.increment_counter('gathered', 'device', 1)
                            data_df = type_params['parser'](data)
                            if len(data_df) > 0:
                                save_datadis_data(data_df, credentials, data_type,
                                                  ["cups", "timestamp"], [("info", "all")], self.config)
                                status['date_min'] = pd.to_datetime(data_df[0]['timestamp'], unit="s").tz_localize(
                                    pytz.UTC)
                                status['date_max'] = pd.to_datetime(data_df[-1]['timestamp'], unit="s"). \
                                    tz_localize(pytz.UTC)
                            else:
                                # store status info
                                status['retries'] -= 1
                            status['values'] = len(data_df)
            datadis_devices.replace_one({"_id": device['_id']}, device, upsert=True)
            self.increment_counter('gathered', 'device', 1)


if __name__ == '__main__':
    DatadisMRJob.run()
