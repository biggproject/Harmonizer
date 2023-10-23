from sources import SourcePlugin
from .gather import gather
from .harmonizer.mapper_static import harmonize_ps_gas_data, harmonize_ps_electric_data
from .harmonizer.mapper_ts import harmonize_ts_electric_data, harmonize_ts_gas_data


class Plugin(SourcePlugin):
    source_name = "SIPS"

    def gather(self, arguments):
        gather(arguments, settings=self.settings, config=self.config)

    def get_mapper(self, message):
        if message["collection_type"] == "ps_info-electricity":
            return harmonize_ps_electric_data
        elif message["collection_type"] == "ps_info-gas":
            return harmonize_ps_gas_data
        elif message["collection_type"] == "consumption-electricity":
            return harmonize_ts_electric_data
        elif message["collection_type"] == "consumption-gas":
            return harmonize_ts_gas_data

    def get_kwargs(self, message):
        return {
            "namespace": message['namespace'],
            "user": message['user'],
            "config": self.config
        }

    def get_store_table(self, message):
        if message['collection_type'] == "ps_info-electricity":
            return f"raw_SIPS_static_electricSupplies__{message['user']}"
        elif message['collection_type'] == "ps_info-gas":
            return f"raw_SIPS_static_gasSupplies__{message['user']}"
        elif message['collection_type'] == "consumption-electricity":
            return f"raw_SIPS_ts_electricInvoices__{message['user']}"
        elif message['collection_type'] == "consumption-gas":
            return f"raw_SIPS_ts_gasInvoices__{message['user']}"

