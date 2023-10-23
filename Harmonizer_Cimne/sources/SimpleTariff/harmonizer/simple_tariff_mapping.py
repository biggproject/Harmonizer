from ontology.namespaces_definition import Bigg, bigg_enums
from ontology.bigg_classes import Tariff
from slugify import slugify
from utils.data_transformations import *


class Mapping(object):

    def __init__(self, source, namespace):
        self.source = source
        Tariff.set_namespace(namespace)

    def get_mappings(self, group):

        tariff = {
            "name": "tariff",
            "class": Tariff,
            "type": {
                "origin": "row"
            },
            "params": {
                "mapping": {
                    "subject": {
                        "key": "tariff_subject",
                        "operations": []
                    },
                    "tariffName": {
                        "key": "tariff_name",
                        "operations": []
                    },
                    "hasTariffCurrencyUnit": {
                        "key": "tariff_currency_unit",
                        "operations": []
                    }
                }
            }
        }



        grouped_modules = {
            "linked": [building_space, utility_point, device],
            "unlinked": [device]
        }
        return grouped_modules[group]
