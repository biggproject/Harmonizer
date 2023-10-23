from utils.data_transformations import *
from ontology.namespaces_definition import Bigg, bigg_enums, units, countries
from ontology.bigg_classes import Organization, Building, LocationInfo, CadastralInfo, BuildingSpace, \
    Area, BuildingConstructionElement



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

    def get_mappings(self, group):
        buildingspace = {
            "name": "buildingspace",
            "class": BuildingSpace,
            "type": {
                "origin": "row"
            },
            "params": {
                "mapping": {
                    "subject": {
                        "key": "buildingspace_subject",
                        "operations": []
                    }
                }
            },
            "links": {
                "netfloor_area": {
                    "type": Bigg.hasArea,
                    "link": "buildingspace_subject"
                },
                "heated_area": {
                    "type": Bigg.hasArea,
                    "link": "buildingspace_subject"
                },
                "cooled_area": {
                    "type": Bigg.hasArea,
                    "link": "buildingspace_subject"
                }
            }
        }

        netfloor_area = {
            "name": "netfloor_area",
            "class": Area,
            "type": {
                "origin": "row"
            },
            "params": {
                "raw": {
                    "hasAreaType": to_object_property("NetFloorArea", namespace=bigg_enums),
                    "hasAreaUnitOfMeasurement": to_object_property("M2", namespace=units),
                },
                "mapping": {
                    "subject": {
                        "key": "netfloor_area_subject",
                        "operations": []
                    },
                    "areaValue": {
                        "key": "SuperficieHabitable",
                        "operations": []
                    }
                }
            }
        }

        heated_area = {
            "name": "heated_area",
            "class": Area,
            "type": {
                "origin": "row"
            },
            "params": {
                "raw": {
                    "hasAreaType": to_object_property("HeatedFloorArea", namespace=bigg_enums),
                    "hasAreaUnitOfMeasurement": to_object_property("M2", namespace=units),
                },
                "mapping": {
                    "subject": {
                        "key": "heated_area_subject",
                        "operations": []
                    },
                    "areaValue": {
                        "key": "heated_area_value",
                        "operations": []
                    }
                }
            }
        }

        cooled_area = {
            "name": "cooled_area",
            "class": Area,
            "type": {
                "origin": "row"
            },
            "params": {
                "raw": {
                    "hasAreaType": to_object_property("CooledFloorArea", namespace=bigg_enums),
                    "hasAreaUnitOfMeasurement": to_object_property("M2", namespace=units),
                },
                "mapping": {
                    "subject": {
                        "key": "cooling_area_subject",
                        "operations": []
                    },
                    "areaValue": {
                        "key": "cooling_area_value",
                        "operations": []
                    }
                }
            }
        }

        grouped_modules = {
            "all": [buildingspace, netfloor_area, heated_area, cooled_area]
        }
        return grouped_modules[group]
