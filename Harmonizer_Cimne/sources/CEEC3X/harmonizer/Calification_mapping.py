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
                    "heatingEnergyDemandClass": {
                        "key": "heatingEnergyDemandClass",
                        "operations": []
                    },
                    "coolingEnergyDemandClass": {
                        "key": "coolingEnergyDemandClass",
                        "operations": []
                    },
                    "lightingCO2EmissionsClass": {
                        "key": "lightingCO2EmissionsClass",
                        "operations": []
                    },
                    "heatingCO2EmissionsClass": {
                        "key": "heatingCO2EmissionsClass",
                        "operations": []
                    },
                    "hotWaterCO2EmissionsClass": {
                        "key": "hotWaterCO2EmissionsClass",
                        "operations": []
                    },
                    "coolingCO2EmissionsClass": {
                        "key": "coolingCO2EmissionsClass",
                        "operations": []
                    },
                    "C02EmissionsClass": {
                        "key": "C02EmissionsClass",
                        "operations": []
                    }
                }
            }
        }

        grouped_modules = {
            "all": [epc]
        }
        return grouped_modules[group]
