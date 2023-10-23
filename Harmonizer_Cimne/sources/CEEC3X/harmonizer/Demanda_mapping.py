from utils.data_transformations import *
from ontology.namespaces_definition import Bigg, bigg_enums, units, countries
from ontology.bigg_classes import Organization, Building, LocationInfo, CadastralInfo, BuildingSpace, \
    Area, BuildingConstructionElement, EnergyPerformanceCertificate, EnergyPerformanceCertificateAdditionalInfo


class Mapper(object):
    def __init__(self, source, namespace):
        self.source = source
        Organization.set_namespace(namespace)
        Building.set_namespace(namespace)
        LocationInfo.set_namespace(namespace)
        BuildingSpace.set_namespace(namespace)
        CadastralInfo.set_namespace(namespace)
        Area.set_namespace(namespace)
        BuildingConstructionElement.set_namespace(namespace)
        EnergyPerformanceCertificate.set_namespace(namespace)
        EnergyPerformanceCertificateAdditionalInfo.set_namespace(namespace)

    def get_mappings(self, group):
        epc = {
            "name": "epc",
            "class": EnergyPerformanceCertificate,
            "type": {
                "origin": "row"
            },
            "params": {
                "mapping": {
                    "subject": {
                        "key": "epc_subject",
                        "operations": []
                    },
                    "annualHeatingEnergyDemand": {
                        "key": "Calefaccion",
                        "operations": []
                    },
                    "annualCoolingEnergyDemand": {
                        "key": "Refrigeracion",
                        "operations": []
                    }
                }
            }
        }

        grouped_modules = {
            "all": [epc]
        }
        return grouped_modules[group]
