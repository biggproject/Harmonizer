import glob

import pandas as pd
from mrjob.job import MRJob
import gemweb
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pickle
import pytz
from utils import *


class Gemweb_gather(MRJob):
    frequencies = {
        # 'data_15m': {'freq': 'quart-horari', 'step': relativedelta(minutes=15)},
        'data_1h': {'freq': 'horari', 'step': relativedelta(hours=1), 'part': relativedelta(months=1)},
        'data_daily': {'freq': 'diari', 'step': relativedelta(days=1), 'part': relativedelta(months=6)},
        'data_month': {'freq': 'mensual', 'step': relativedelta(months=1), 'part': relativedelta(months=12)}
    }

    def setup(self, config):
        self.connection = config['connection']
        self.hbase_conf = config['config']['hbase_connection']
        self.mongo_conf = config['config']['mongo_connection']
        self.data_source = config['config']['data_source']
        self.job = config['job']

    def mapper_init(self):
        fn = glob.glob('*.json')
        config = pickle.load(open(fn[0], 'rb'))
        self.setup(config)

    def reducer_init(self):
        fn = glob.glob('*.json')
        config = pickle.load(open(fn[0], 'rb'))
        self.setup(config)

    def mapper(self, _, device):
        if self.job['freq'] == 'all':
            frequencies = ['data_1h', 'data_daily', 'data_month']  # 'data_15m'
        else:
            frequencies = [self.job['freq']]
        mongo = connection_mongo(self.mongo_conf)
        query_log = {"_id": self.job['report']}
        for k in frequencies:
            mongo[self.data_source['log']].update_one(query_log, {"$set": {f"devices.{device}.{k}": ["starting map"]}})
            yield "{}~{}".format(device, k), None

    def reducer(self, launch, _):
        device, freq = launch.split("~")
        mongo = connection_mongo(self.mongo_conf)
        query_log = {"_id": self.job['report']}
        query_report = {"device": device, "frequency": freq}
        current_report = mongo[self.data_source['report']].find_one(query_report)
        if not current_report:
            current_report = query_report

        current_report["updated"] = datetime.utcnow()
        for err in ["error_log", "error_date"]:
            try:
                current_report.pop(err)
            except KeyError:
                pass
        try:
            gemweb.gemweb.connection(self.connection['username'], self.connection['password'], timezone="UTC")
        except Exception as e:
            mongo[self.data_source['log']].update_one(query_log, {
                "$push": {f"devices.{device}.{freq}": {"$each": [str(e), "finish"]}}})
            current_report['error_log'] = str(e)
            mongo[self.data_source['report']].replace_one(query_report, current_report, upsert=True)
            return

        mongo[self.data_source['log']].update_one(query_log, {"$push": {f"devices.{device}.{freq}": "reduce start"}})

        user = self.connection['user']

        if not self.job['date_from'] and 'date_to' in current_report:
            date_from = current_report['date_to']
        elif not self.job['date_from']:
            mongo[self.data_source['log']].update_one(query_log, {
                "$push": {f"devices.{device}.{freq}": {"$each": ["can't find a date_from", "finish"]}}})
            current_report['error_data'] = "can't find data"
            mongo[self.data_source['report']].replace_one(query_report, current_report, upsert=True)
            return
        else:
            date_from = pytz.UTC.localize(datetime.strptime(self.job['date_from'], "%Y-%m-%d"))

        if self.job['date_to']:
            date_to = pytz.UTC.localize(datetime.strptime(self.job['date_to'], "%Y-%m-%d"))
        else:
            date_to = pytz.UTC.localize(datetime.utcnow())

        data_t = []
        while date_from < date_to:
            date_to2 = date_from + Gemweb_gather.frequencies[freq]['part']
            mongo[self.data_source['log']].update_one(query_log, {
                "$push": {f"devices.{device}.{freq}": f"getting from {date_from} to {date_to2}"}})
            try:
                x2 = gemweb.gemweb.gemweb_query(gemweb.ENDPOINTS.GET_METERING, id_=device,
                                                date_from=date_from,
                                                date_to=date_to2,
                                                period=Gemweb_gather.frequencies[freq]['freq'])
                mongo[self.data_source['log']].update_one(query_log, {
                    "$push": {f"devices.{device}.{freq}": f"succeed from {date_from} to {date_to2}"}})
            except Exception as e:
                mongo[self.data_source['log']].update_one(query_log, {
                    "$push": {f"devices.{device}.{freq}": f"failed from {date_from} to {date_to2}"}})
                x2 = []
            self.increment_counter('gathered', 'device', 1)
            date_from = date_to2 + relativedelta(days=1)
            data_t.append(x2)

        data = []
        for x in data_t:
            data.extend(x)

        if len(data) > 0:
            for d in data:
                d['building'] = device
                d['measurement_start'] = int(d['datetime'].timestamp())
                d['measurement_end'] = int((d['datetime'] + Gemweb_gather.frequencies[freq]['step']).timestamp())
            df = pd.DataFrame.from_records(data)
            # save obtained data to hbase
            hbase = connection_hbase(self.hbase_conf)
            htable = get_HTable(hbase, "{}_{}_{}".format(self.data_source["hbase_name"], freq, user), {"v": {}, "info": {}})
            mongo[self.data_source['log']].update_one(query_doc, {"$push": {f"devices.{device}.{freq}": f"saving to hbase"}})

            save_to_hbase(htable, df.to_dict(orient="records"), [("v", ["value"]), ("info", ["measurement_end"])], row_fields=['building', 'measurement_start'], version=version)
            self.increment_counter('saved', 'device', 1)

            if ('date_to' not in current_report) or ('date_to' in current_report and current_report['date_to'] < max(df.datetime)):
                current_report['date_to'] = max(df.datetime)

            if ('date_from' not in current_report) or 'date_from' in current_report and current_report['date_from'] > min(df.datetime):
                current_report['date_from'] = min(df.datetime)

        mongo = connection_mongo(self.mongo_conf)

        mongo[self.data_source['report']].update_one(query_report, current_report, upsert=True)
        self.increment_counter("finished", 'device', 1)
        mongo[self.data_source['log']].update_one(query_log, {
            "$push": {f"devices.{device}.{freq}": "finish"}})


if __name__ == '__main__':
    Gemweb_gather.run()
