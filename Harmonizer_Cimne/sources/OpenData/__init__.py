from sources import SourcePlugin
from sources.OpenData.gather import gather
from sources.OpenData.harmonizer import harmonize_data, harmonize_command_line
from utils.nomenclature import raw_nomenclature, RAW_MODE


class Plugin(SourcePlugin):
    source_name = "OpenData"

    def harmonizer_command_line(self, arguments):
        harmonize_command_line(arguments, config=self.config, settings=self.settings)

    def gather(self, arguments):
        gather(arguments, settings=self.settings, config=self.config)

    def get_mapper(self, message):
        if message["collection_type"] == 'EnergyPerformanceCertificate':
            return harmonize_data

    def get_kwargs(self, message):
        return {
            "namespace": message['namespace'],
            "user": message['user'],
            "config": self.config,
        }

    def get_store_table(self, message):
        if message['collection_type'] == "EnergyPerformanceCertificate":
            h_table_name = raw_nomenclature(mode=RAW_MODE.STATIC, data_type=message['collection_type'], frequency="",
                                            user=message['user'],
                                            source=self.source_name)
            return h_table_name
        else:
            return None  # Useless info
