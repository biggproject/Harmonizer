from sources import SourcePlugin
from .gather import gather
from .harmonizer import *
from .harmonizer.mapper_ts import harmonize_ts_data


class Plugin(SourcePlugin):
    source_name = "ManualData"

    def gather(self, arguments):
        gather(arguments, settings=self.settings, config=self.config)

    def get_mapper(self, message):
        if message["collection_type"] == 'consumption':
            return harmonize_ts_data

    def get_kwargs(self, message):
        if message["collection_type"] in ['consumption']:
            return {
                "namespace": message['namespace'],
                "user": message['user'],
                "config": self.config,
                "collection_type": message['collection_type']
            }

    def get_store_table(self, message):
        if message["collection_type"] == 'consumption':
            return f"raw_{self.source_name}_ts_consumption__{message['user']}"

    def harmonizer_command_line(self, arguments):
        harmonize_command_line(arguments, config=self.config, settings=self.settings)
