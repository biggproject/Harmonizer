import pytz
from .gather import gather
from .harmonizer import harmonize_command_line
from .harmonizer.mapper_ts import harmonize_data
from .. import SourcePlugin
from utils.utils import log_string


class Plugin(SourcePlugin):
    source_name = "weather"

    def gather(self, arguments):
        gather(arguments, settings=self.settings, config=self.config)

    def get_mapper(self, message):
        if message["collection_type"] == "darksky":
            log_string("weather ts", mongo=False)
            return harmonize_data
        else:
            return None

    def get_kwargs(self, message):
        if message["collection_type"] == "darksky":
            return {
                "namespace": message['namespace'],
                "config": self.config,
                "freq": message['freq']
            }
        else:
            return None

    def transform_df(self, df):
        df['ts'] = df.ts.dt.tz_convert(pytz.UTC)
        df['station_id'] = df.apply(lambda el: f"{float(el.latitude):.3f}~{float(el.longitude):.3f}", axis=1)
        return df

    def get_store_table(self, message):
        return None

    def harmonizer_command_line(self, arguments):
        harmonize_command_line(arguments, config=self.config, settings=self.settings)
