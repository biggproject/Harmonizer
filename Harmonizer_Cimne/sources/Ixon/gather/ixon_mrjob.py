import datetime
import glob
import pickle
import subprocess
import time

import BAC0
import psutil
import requests
from mrjob.job import MRJob
from mrjob.step import MRStep

from utils.hbase import save_to_hbase
#from utils.ixon import Ixon
from utils.kafka import save_to_kafka
from utils.mongo import mongo_connection
from utils.utils import log_string

NUM_VPNS_CONFIG = 5
NETWORK_INTERFACE = 'tap0'

vpn_dict = {'0': '10.187.10.1', '1': '10.187.10.15', '2': '10.187.10.12', '3': '10.187.10.13', '4': '10.187.10.14',
            '5': '10.187.13.10'}

DEBUG = False


# TODO: Init mongo_logger
def save_data(data, data_type, row_keys, column_map, config):
    if config['store'] == "kafka":
        log_string(f"saving to kafka", mongo=False)
        try:
            k_topic = config["kafka"]["topic"]
            kafka_message = {
                "namespace": config['namespace'],
                "collection_type": data_type,
                "source": config['source'],
                "row_keys": row_keys,
                "user": config['user'],
                "column_map": column_map,
                # "logger": mongo_logger.export_log(),
                # TODO: kafka topic erroni, agafa la configuracio que no es
                "data": data
            }
            save_to_kafka(topic=k_topic, info_document=kafka_message,
                          config=config['kafka']['connection'], batch=config["kafka_message_size"])

        except Exception as e:
            log_string(f"error when sending message: {e}")
    elif config['store'] == "hbase":
        log_string(f"saving to hbase", mongo=False)
        try:
            h_table_name = f"raw_{config['source']}_ts_Meter_PT15M_{config['user']}"
            save_to_hbase(data, h_table_name, config['hbase_store_raw_data'], column_map,
                          row_fields=row_keys)
        except Exception as e:
            log_string(f"Error saving datadis supplies to HBASE: {e}")
    else:
        log_string(f"store {config['store']} is not supported")


class MRIxonJob(MRJob):
    def __read_config__(self):
        fn = glob.glob('*.pickle')
        self.config = pickle.load(open(fn[0], 'rb'))

    def mapper_get_available_agents(self, _, line):
        # line : email password application_id
        l = line.split('\t')

        ixon_conn = Ixon(l[2])  # api application
        ixon_conn.generate_token(l[0], l[1])  # user, password
        ixon_conn.get_companies()
        ixon_conn.get_agents()

        for index, agent in enumerate(ixon_conn.agents):
            dict_result = {"token": ixon_conn.token, "api_application": l[2], "company": ixon_conn.companies[0]}
            dict_result.update({"agent": agent['publicId']})
            dict_result.update({"company_label": l[3]})
            yield index % NUM_VPNS_CONFIG, dict_result

    def mapper_generate_network_config(self, key, line):

        try:
            headers = {
                "Api-Version": '2',
                "Api-Application": line['api_application'],
                'Authorization': f"Bearer {line['token']}",
                "Api-Company": line['company']['publicId']
            }

            # Get network configuration
            res = requests.get(
                url=f"https://portal.ixon.cloud/api/agents/{line['agent']}?fields=activeVpnSession.vpnAddress,config.routerLan.*,devices.*,devices.dataProtocol.*,deviceId,description",
                headers=headers,
                timeout=(5, 10))

            # Store data
            res = res.json()

            if 'data' in res:
                data = res['data']

                if 'deviceId' in data:
                    db = mongo_connection(self.connection)
                    ixon_devices = db['ixon_devices']
                    current_buildings = list(ixon_devices.distinct('building_id'))

                    if data['deviceId'] in current_buildings:
                        if data['activeVpnSession'] and data['config'] and data['config']['routerLan']:
                            values = {"company_label": line['company_label'],
                                      'ip_vpn': data['activeVpnSession']['vpnAddress'],
                                      'network': data['config']['routerLan']['network'],
                                      'network_mask': data['config']['routerLan']['netMask'],
                                      'deviceId': data['deviceId'],
                                      'description': data['description']}
                            yield key, values

        except Exception as ex:
            print(str(ex))

    def reducer_generate_vpn(self, key, values):

        db = mongo_connection(self.connection)

        ixon_devices = db['ixon_devices']
        ixon_logs = db['ixon_logs']
        network_usage = db['network_usage']

        for value in values:  # buildings

            building_devices = list(ixon_devices.find({'building_id': value['deviceId']}, {'_id': 0}))

            if building_devices:
                log_string(f"{building_devices[0]['building_name']}\n", mongo=False)

                # Generate VPN Config
                with open(f'vpn_template_{key}.ovpn', 'r') as file:
                    f = open(value['deviceId'].split('-')[1].strip() + '.ovpn', 'w')
                    content = file.read()
                    content += "\nroute {0} {1} {2}".format(value['network'], value['network_mask'], value['ip_vpn'])
                    f.write(content)
                    f.close()

                # Connect to VPN
                openvpn = subprocess.Popen(["sudo", "openvpn", f.name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                interfaces = subprocess.run(["hostname", "-I"], stdout=subprocess.PIPE)
                interfaces_list = interfaces.stdout.decode(encoding="utf-8").split(" ")

                log_string(f"{interfaces_list}\n", mongo=False)

                # Waiting to VPN connection
                time_out = 4  # seconds
                init_time = time.time()
                waiting_time = 0.2

                connected = vpn_dict[str(key)] in interfaces_list

                while not connected:
                    time.sleep(waiting_time)
                    interfaces = subprocess.run(["hostname", "-I"], stdout=subprocess.PIPE)
                    interfaces_list = interfaces.stdout.decode(encoding="utf-8").split(" ")
                    connected = vpn_dict[str(key)] in interfaces_list
                    if time.time() - init_time > time_out:
                        break

                if not connected:
                    if not DEBUG:
                        ixon_logs.insert_one(
                            {'building_id': value['deviceId'], "building_name": building_devices[0]['building_name'],
                             'building_internal_id': building_devices[0]['building_internal_id'],
                             "info": "VPN Connection: Time out exceeded.",
                             "date": datetime.datetime.utcnow(), "successful": False})
                    try:
                        subprocess.call(["sudo", "pkill", "openvpn"])
                    except Exception as ex:
                        log_string(ex, mongo=False)
                    continue

                vpn_ip = vpn_dict[str(key)]
                log_string(str(vpn_ip), mongo=False)

                # Recover Data
                # Open BACnet Connection
                results = []
                devices_logs = []
                current_time = time.time()
                aux = False

                while not aux and time.time() - current_time < 3:
                    try:
                        log_string(f"{vpn_ip},{value['ip_vpn']}\n", mongo=False)
                        bacnet = BAC0.lite(ip=vpn_ip + '/16', bbmdAddress=value['ip_vpn'] + ':47808', bbmdTTL=900)
                        aux = True
                    except Exception as ex:
                        bacnet.disconnect()
                        log_string(ex, mongo=False)
                        log_string("%s " % str(building_devices[0]['building_name']) + "\n", mongo=False)
                        time.sleep(0.2)

                if not aux:

                    try:
                        netio = psutil.net_io_counters(pernic=True)
                        if not DEBUG:
                            network_usage.insert_one(
                                {"from": 'infraestructures.cat', "building": value['deviceId'],
                                 'building_internal_id': building_devices[0]['building_internal_id'],
                                 "timestamp": datetime.datetime.utcnow(),
                                 "bytes_sent": netio[NETWORK_INTERFACE].bytes_sent,
                                 "bytes_recv": netio[NETWORK_INTERFACE].bytes_recv})
                    except Exception as ex:
                        log_string(ex, mongo=False)

                    subprocess.call(["sudo", "pkill", "openvpn"])
                    bacnet.disconnect()
                    if not DEBUG:
                        ixon_logs.insert_one(
                            {'building_id': value['deviceId'], "building_name": building_devices[0]['building_name'],
                             'building_internal_id': building_devices[0]['building_internal_id'],
                             "info": "BACnet Connection: Time out exceeded.",
                             "date": datetime.datetime.utcnow(), "successful": False})
                    continue

                # Recover data for each device
                for device in building_devices:
                    try:
                        device_value = bacnet.read(
                            f"{device['bacnet_device_ip']} {device['type']} {device['object_id']} presentValue")

                        results.append({"building": device['building_id'], "device": device['name'],
                                        'building_internal_id': device['building_internal_id'],
                                        "timestamp": datetime.datetime.utcnow().timestamp(), "value": device_value,
                                        "type": device['type'], "description": device['description'],
                                        "object_id": device['object_id']})

                        devices_logs.append({'device_name': device['name'],
                                             'device_id': device['object_id'], 'device_type': device['type'],
                                             'info': 'OK',
                                             'successful': True})
                    except Exception as ex:
                        devices_logs.append({'device_name': device['name'],
                                             'device_id': device['object_id'], 'device_type': device['type'],
                                             'info': f"Device {device['name']} fail.",
                                             'successful': False})

                try:
                    netio = psutil.net_io_counters(pernic=True)
                    if not DEBUG:
                        network_usage.insert_one(
                            {"from": 'infraestructures.cat', "building": value['deviceId'],
                             'building_internal_id': building_devices[0]['building_internal_id'],
                             "bytes_sent": netio[NETWORK_INTERFACE].bytes_sent,
                             "timestamp": datetime.datetime.utcnow(),
                             "bytes_recv": netio[NETWORK_INTERFACE].bytes_recv})
                except Exception as ex:
                    log_string(ex, mongo=False)

                # End Connections (Bacnet and VPN)
                bacnet.disconnect()
                subprocess.call(["sudo", "pkill", "openvpn"])

                if not DEBUG:
                    ixon_logs.insert_one(
                        {'building_id': value['deviceId'], "building_name": building_devices[0]['building_name'],
                         'building_internal_id': building_devices[0]['building_internal_id'],
                         "devices_logs": devices_logs,
                         "info": "OK",
                         "date": datetime.datetime.utcnow(), "successful": True})

                if results and not DEBUG:
                    try:
                        save_data(data=results, data_type='ts',
                                  row_keys=['building', 'device', 'timestamp'], column_map=[("v", ["value"]),
                                                                                            ("info",
                                                                                             ["type", "description",
                                                                                              'object_id',
                                                                                              'building_internal_id'])],
                                  config=self.config)

                    except Exception as ex:
                        log_string(ex, mongo=False)

    def reducer_init_databases(self):
        # Read and save MongoDB config
        self.__read_config__()
        self.connection = self.config['mongo_db']
        self.hbase = self.config['hbase_store_raw_data']

    def mapper_init(self):
        # Read and save MongoDB config
        self.__read_config__()
        self.connection = self.config['mongo_db']

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_available_agents),
            MRStep(mapper_init=self.mapper_init, mapper=self.mapper_generate_network_config),
            MRStep(reducer_init=self.reducer_init_databases, reducer=self.reducer_generate_vpn)
        ]


if __name__ == '__main__':
    MRIxonJob.run()
