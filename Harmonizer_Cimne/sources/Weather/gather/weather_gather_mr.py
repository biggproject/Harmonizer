from datetime import datetime, timedelta
import glob
import pickle
from abc import ABC
from mrjob.job import MRJob
from utils.mongo import mongo_logger, mongo_connection
from utils.kafka import save_to_kafka
from dateutil.relativedelta import relativedelta
from beemeteo.sources.darksky import DarkSky


def download_chunk(cp, type_params, config, date_ini, date_end):
    source = type_params['source'](config)
    data = source.get_historical_data(
        float(cp['latitude']),
        float(cp['longitude']),
        date_ini,
        date_end
    )
    return data


def save_weather_data(data, logger, config):
    kafka_message = {
        "namespace": "https://weather.beegroup-cimne.com#",
        "collection_type": "darksky",
        "freq": "PT1H",
        "source": config['source'],
        "logger": logger.export_log(),
        "data": data.to_dict(orient="records")
    }
    save_to_kafka(topic=config['kafka']['topic'], info_document=kafka_message,
                  config=config['kafka']['connection'], batch=config['kafka_message_size'])


data_weather_sources = {
    "darksky": {
        "freq_rec": relativedelta(months=2),
        "measurement_type": "0",
        "source": DarkSky,
        "params": ["latitude", "longitude", "date_from", "date_to"],
    }
}

INIT_DATE = datetime(2018, 1, 1)

class WeatherMRJob(MRJob, ABC):
    def __read_config__(self):
        fn = glob.glob('*.pickle')
        self.config = pickle.load(open(fn[0], 'rb'))

    def mapper_init(self):
        self.__read_config__()

    def mapper(self, id_key, line):
        # map receives lat, lon and downloads DarkSky data,
        cp = {k: v for k, v in zip(["latitude", "longitude"], line.split("\t"))}
        mongo_logger.import_log(self.config['mongo_logger'], "gather")
        weather_stations = \
            mongo_connection(self.config['mongo_db'])[self.config['data_sources'][self.config['source']]['log_stations']]

        # get the highest page document log
        station_id = f"{float(cp['latitude']):.3f}~{float(cp['longitude']):.3f}"
        try:
            station = weather_stations.find_one({"_id": station_id})
        except IndexError:
            station = None
        if not station:
            # if there is no log document create a new one
            station = {
                "_id": station_id
            }
        # create the data chunks we will gather the information for timeseries
        date_ini = INIT_DATE
        now = datetime.now() + timedelta(days=1)
        for t, type_params in data_weather_sources.items():
            if t not in station:
                station[t] = {
                    "date_ini": date_ini,
                    "date_end": None
                }
                start_obtaining_date = date_ini
            elif station[t]['date_end']:
                start_obtaining_date = station[t]['date_end']
            else:
                start_obtaining_date = date_ini
            while start_obtaining_date < now:
                chunk_end = min(now, start_obtaining_date + type_params['freq_rec'])
                data = download_chunk(cp, type_params, self.config['data_sources'][self.config['source']],
                                      start_obtaining_date, chunk_end)
                self.increment_counter('gathered', 'device', 1)
                if len(data) > 0:
                    last_data = data.iloc[-1].ts.to_pydatetime().replace(tzinfo=None)
                    save_weather_data(data, mongo_logger, self.config)
                    station[t]['date_end'] = last_data
                start_obtaining_date += type_params['freq_rec'] + relativedelta(days=1)
        weather_stations.replace_one({"_id": station_id}, station, upsert=True)
        self.increment_counter('gathered', 'device', 1)


if __name__ == '__main__':
    WeatherMRJob.run()
