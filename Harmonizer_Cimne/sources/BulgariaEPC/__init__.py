from sources import SourcePlugin
from .gather import gather
from .harmonizer import *
from .harmonizer.mapper_buildings import harmonize_static


class Plugin(SourcePlugin):
    source_name = "BulgariaEPC"

    def gather(self, arguments):
        gather(arguments, settings=self.settings, config=self.config)

    def get_mapper(self, message):
        if message["collection_type"] == 'epcData':
            return harmonize_static

    def get_kwargs(self, message):
        if message["collection_type"] in ['epcData']:
            return {
                "namespace": message['namespace'],
                "user": message['user'],
                "config": self.config,
                "collection_type": message['collection_type']
            }

    def get_store_table(self, message):
        if message["collection_type"] == 'epcData':
            return f"raw_{self.source_name}_static_epc__{message['user']}"

    def harmonizer_command_line(self, arguments):
        harmonize_command_line(arguments, config=self.config, settings=self.settings)
