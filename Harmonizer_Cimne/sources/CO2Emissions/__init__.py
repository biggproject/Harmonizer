import pandas as pd

from sources import SourcePlugin
from .gather import gather
from .harmonizer import harmonize_command_line
from .harmonizer.mapper import harmonize_data_ts


class Plugin(SourcePlugin):
    source_name = "co2emissions"

    def gather(self, arguments):
        gather(arguments, settings=self.settings, config=self.config)

    def harmonizer_command_line(self, arguments):
        harmonize_command_line(arguments, config=self.config, settings=self.settings)

    def get_mapper(self, message):
        if message['collection_type'] == "co2_ts":
            return harmonize_data_ts
        else:
            return None

    def get_kwargs(self, message):
        if message['collection_type'] == "co2_ts":
            return {
                    "namespace": message['namespace'],
                    "user": message['user'],
                    "date_ini": message['date_ini'],
                    "date_end": message['date_end'],
                    "co2_uid":  message['co2_uid'],
                    "measured_property": message['measured_property'],
                    "co2_property": message['co2_property'],
                    "co2_property_unit": message['co2_property_unit'],
                    "unit": message['unit'],
                    "config": self.config
                }
        else:
            return None

    def get_store_table(self, message):
        if message['collection_type'] == "co2_ts":
            prop = str(message['co2_property']).split("#")[1]
            return f"raw_co2emissions_ts_{prop}_PT1H_{message['user']}"
        else:
            return None

    def transform_df(self, df):
        if "values" in df.columns:
            return pd.DataFrame.from_records(df.loc[0, "values"])
        else:
            return df
