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
                        "key": "building_space_main_subject",
                        "operations": []
                    }
                }
            },
            "links": {
                "buildingSubSpace": {
                    "type": Bigg.hasSubSpace,
                    "link": "building_space_current_subject"
                }
            }
        }

        buildingSubSpace = {
            "name": "buildingSubSpace",
            "class": BuildingSpace,
            "type": {
                "origin": "row"
            },
            "params": {
                "mapping": {
                    "subject": {
                        "key": "building_space_current_subject",
                        "operations": []
                    },
                    "buildingSpaceName": {
                        "key": "Nombre",
                        "operations": []
                    }
                }
            },
            "links": {
                "space_area": {
                    "type": Bigg.hasArea,
                    "link": "building_space_current_subject"
                }
            }
        }

        space_area = {
            "name": "space_area",
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
                        "key": "building_space_current_netfloorarea_subject",
                        "operations": []
                    },
                    "areaValue": {
                        "key": "Superficie",
                        "operations": []
                    }
                }
            }
        }

        grouped_modules = {
            "all": [buildingspace, buildingSubSpace, space_area]
        }
        return grouped_modules[group]
