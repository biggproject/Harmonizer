import re
from .gather import gather
from .harmonizer import harmonize_command_line
from .harmonizer.mapper_static import harmonize_data as harmonize_data_static
from .harmonizer.mapper_ts import harmonize_data as harmonize_data_ts
from .. import SourcePlugin
from utils.utils import log_string

class Plugin(SourcePlugin):
    source_name = "datadis"

    def gather(self, arguments):
        gather(arguments, settings=self.settings, config=self.config)

    def get_mapper(self, message):
        if message["collection_type"] == "supplies":
            log_string("datadis supplies", mongo=False)
            return harmonize_data_static
        elif re.match(r"EnergyConsumptionGridElectricity_.*", message["collection_type"]):
            log_string("datadis ts", mongo=False)
            return harmonize_data_ts
        else:
            return None

    def get_kwargs(self, message):
        if message["collection_type"] == "supplies":
            return {
                "namespace": message['namespace'],
                "user": message['user'],
                "config": self.config,
            }
        elif re.match(r"EnergyConsumptionGridElectricity_.*", message["collection_type"]):
            freq = message['collection_type'].split("_")[1]
            return {
                "namespace": message['namespace'],
                "user": message['user'],
                "config": self.config,
                "freq": freq
            }
        else:
            return None

    def get_store_table(self, message):
        if message["collection_type"] == "supplies":
            return f"raw_{self.source_name}_static_supplies__{message['user']}"
        elif re.match(r"EnergyConsumptionGridElectricity_.*", message["collection_type"]):
            return f"raw_{self.source_name}_ts_{message['collection_type']}_{message['user']}"
        elif message["collection_type"] == "Power_P1M":
            return f"{self.source_name}_ts_{message['collection_type']}_{message['user']}"
        else:
            return None

    def harmonizer_command_line(self, arguments):
        harmonize_command_line(arguments, config=self.config, settings=self.settings)
