from utils.data_transformations import to_object_property
from ontology.bigg_classes import Organization, Building, LocationInfo, BuildingSpace, Area, \
    EnergyPerformanceCertificate, BuildingSpaceUseType, AreaType, AreaUnitOfMeasurement, Device, \
    EnergyEfficiencyMeasure, Sensor, EnergySaving, BuildingConstructionElement, RenovationProject, UtilityPointOfDelivery
from ontology.namespaces_definition import Bigg, units, bigg_enums, countries


class Mapper(object):
    def __init__(self, source, namespace):
        self.source = source
        self.eem_list = None
        self.eem_start_column = None
        self.saving_list = None
        self.saving_start_column = None
        Organization.set_namespace(namespace)
        Building.set_namespace(namespace)
        LocationInfo.set_namespace(namespace)
        BuildingSpace.set_namespace(namespace)
        BuildingSpaceUseType.set_namespace(namespace)
        Area.set_namespace(namespace)
        EnergyPerformanceCertificate.set_namespace(namespace)
        AreaType.set_namespace(namespace)
        AreaUnitOfMeasurement.set_namespace(namespace)
        BuildingConstructionElement.set_namespace(namespace)
        Device.set_namespace(namespace)
        UtilityPointOfDelivery.set_namespace(namespace)
        EnergyEfficiencyMeasure.set_namespace(namespace)
        Sensor.set_namespace(namespace)
        EnergySaving.set_namespace(namespace)
        RenovationProject.set_namespace(namespace)

    def get_mappings(self, group):
        # building info mapping

        building = {
            "name": "building",
            "class": Building,
            "type": {
                "origin": "row"
            },
            "params": {
                "mapping": {
                    "subject": {
                        "key": "building_subject",
                        "operations": []
                    }
                }
            },
            "links": {
                "building_space": {
                    "type": Bigg.hasSpace,
                    "link": "subject"
                },
                "energy_performance_certificate": {
                    "type": Bigg.hasEPC,
                    "link": "subject"
                }
            }
        }

        energy_performance_certificate = {
            "name": "energy_performance_certificate",
            "class": EnergyPerformanceCertificate,
            "type": {
                "origin": "row"
            },
            "params": {
                "mapping": {
                    "subject": {
                        "key": "epc_before_subject",
                        "operations": []
                    },
                    "energyPerformanceCertificateDateOfAssessment": {
                        "key": "epc_date",
                        "operations": []
                    },
                    "energyPerformanceCertificateClass": {
                        "key": "energy_class_before",
                        "operations": []
                    },
                    "annualFinalEnergyConsumption": {
                        "key": "specific_energy_consumption_before (kWh/m2)",
                        "operations": []
                    }
                }
            }
        }

        building_space = {
            "name": "building_space",
            "class": BuildingSpace,
            "type": {
                "origin": "row"
            },
            "params": {
                "mapping": {
                    "subject": {
                        "key": "building_space_subject",
                        "operations": []
                    }
                }
            },
            "links": {
                "gross_floor_area": {
                    "type": Bigg.hasArea,
                    "link": "subject"
                },
                "element": {
                    "type": Bigg.isAssociatedWithElement,
                    "link": "subject"
                }
            }
        }

        gross_floor_area = {
            "name": "gross_floor_area",
            "class": Area,
            "type": {
                "origin": "row"
            },
            "params": {
                "raw": {
                    "hasAreaType": bigg_enums["GrossFloorArea"],
                    "hasAreaUnitOfMeasurement": units["M2"]
                },
                "mapping": {
                    "subject": {
                        "key": "gross_floor_area_subject",
                        "operations": []
                    },
                    "areaValue": {
                        "key": "gross_floor_area",
                        "operations": []
                    }
                }
            }
        }

        grouped_modules = {
            "building_info": [building, building_space,
                              gross_floor_area, energy_performance_certificate]
        }
        return grouped_modules[group]
