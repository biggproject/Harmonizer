import re
from functools import partial

import utils
from sources import SourcePlugin
from sources.Bulgaria.gather import gather
from sources.Bulgaria.harmonizer import harmonize_command_line
from sources.Bulgaria.harmonizer.mapper_buildings import harmonize_ts, harmonize_static, harmonize_kpi, \
    harmonize_eem_kpi
from utils.nomenclature import RAW_MODE


class Plugin(SourcePlugin):
    source_name = "Bulgaria"

    def gather(self, arguments):
        gather(arguments, settings=self.settings, config=self.config)

    def get_mapper(self, message):
        if message["collection_type"] == 'BuildingInfo':
            return harmonize_static
        if re.search(r"BuildingInfo_KPI_.*", message["collection_type"]):
            return partial(harmonize_kpi, split=int(message["collection_type"].split("_")[2]))
        if re.search(r"EEM_KPI_.*", message["collection_type"]):
            return partial(harmonize_eem_kpi, split=int(message["collection_type"].split("_")[2]))
        if message["collection_type"] == 'BuildingEnergyConsumption':
            return harmonize_ts

    def get_kwargs(self, message):
        if message["collection_type"] in ['BuildingInfo', "BuildingEnergyConsumption"] or \
                re.search(r"BuildingInfo_KPI_.*", message["collection_type"]) or \
                re.search(r"EEM_KPI_.*", message["collection_type"]):
            return {
                "namespace": message['namespace'],
                "user": message['user'],
                "config": self.config,
                "collection_type": message['collection_type']
             }

    def get_store_table(self, message):
        if message["collection_type"] == 'BuildingInfo':
            return utils.nomenclature.raw_nomenclature("Bulgaria", RAW_MODE.STATIC, data_type='BuildingInfo',
                                                       frequency="", user=message['user'])

    def harmonizer_command_line(self, arguments):
        harmonize_command_line(arguments, config=self.config, settings=self.settings)
