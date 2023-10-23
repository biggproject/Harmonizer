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
        department_organization = {
            "name": "department_organization",
            "class": Organization,
            "type": {
                "origin": "row_split_column",
                "operations": [],
                "sep": ";",
                "column": "department_organization",
                "column_mapping": {
                    "subject": [],
                }
            },
            "params": {
                "column_mapping": {
                    "subject": "subject",
                }
            },
            "links": {
                "building_organization": {
                    "type": Bigg.hasSubOrganization,
                    "link": "Unique Code"
                }
            }
        }

        building_organization = {
            "name": "building_organization",
            "class": Organization,
            "type": {
                "origin": "row"
            },
            "params": {
                "raw": {
                    "organizationDivisionType": "Building"
                },
                "mapping": {
                    "subject": {
                        "key": "building_organization",
                        "operations": []
                    },
                    "organizationName": {
                        "key": "Name",
                        "operations": []
                    }
                }
            },
            "links": {
                "building": {
                    "type": Bigg.managesBuilding,
                    "link": "Unique Code"
                }
            }
        }

        building = {
            "name": "building",
            "class": Building,
            "type": {
                "origin": "row"
            },
            "params": {
                "mapping": {
                    "subject": {
                        "key": "building",
                        "operations": []
                    },
                    "buildingIDFromOrganization": {
                        "key": "Unique Code",
                        "operations": []
                    },
                    "buildingName": {
                        "key": "Name",
                        "operations": []
                    }
                }
            },
            "links": {
                "building_space": {
                    "type": Bigg.hasSpace,
                    "link": "Unique Code"
                },
                "location_info": {
                    "type": Bigg.hasLocationInfo,
                    "link": "Unique Code"
                },
                "cadastral_info": {
                    "type": Bigg.hasCadastralInfo,
                    "link": "Unique Code"
                }
            }
        }

        location_info = {
            "name": "location_info",
            "class": LocationInfo,
            "type": {
                "origin": "row"
            },
            "params": {
                "raw": {
                    "addressTimeZone": "EuropeMadrid"
                },
                "mapping": {
                    "subject": {
                        "key": "location_info",
                        "operations": []
                    },
                    "hasAddressCountry":{
                        "key": "hasAddressCountry",
                        "operations": []
                    },
                    "hasAddressProvince": {
                        "key": "hasAddressProvince",
                        "operations": []
                    },
                    "hasAddressCity": {
                        "key": "hasAddressCity",
                        "operations": []
                    },
                    "addressPostalCode": {
                        "key": "CP",
                        "operations": []
                    },
                    "addressStreetNumber": {
                        "key": 'Road Number',
                        "operations": []
                    },
                    "addressStreetName": {
                        "key": 'Road',
                        "operations": []
                    },
                    "addressLatitude": {
                        "key": 'Latitud',
                        "operations": []
                    },
                    "addressLongitude": {
                        "key": 'Longitud',
                        "operations": []
                    }
                }
            }
        }

        cadastral_info = {
            "name": "cadastral_info",
            "class": CadastralInfo,
            "type": {
                "origin": "row_split_column",
                "operations": [],
                "sep": ";",
                "column": 'Cadastral References',
                "column_mapping": {
                    "subject": [cadastral_info_subject],
                    "landCadastralReference": []
                }
            },
            "params": {
                "column_mapping": {
                    "subject": "subject",
                    "landCadastralReference": "landCadastralReference"
                },
                "mapping": {
                    "landArea": {
                        "key": "Land Area",
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
                "raw": {
                    "buildingSpaceName": "Building",
                },
                "mapping": {
                    "subject": {
                        "key": "building_space",
                        "operations": []
                    },
                    "hasBuildingSpaceUseType": {
                        "key": "hasBuildingSpaceUseType",
                        "operations": []
                    }
                }
            },
            "links": {
                "gross_floor_area": {
                    "type": Bigg.hasArea,
                    "link": "Unique Code"
                },
                "gross_floor_area_above_ground": {
                    "type": Bigg.hasArea,
                    "link": "Unique Code"
                },
                "gross_floor_area_under_ground": {
                    "type": Bigg.hasArea,
                    "link": "Unique Code"
                },
                "building_element": {
                    "type": Bigg.isAssociatedWithElement,
                    "link": "Unique Code"
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
                    "hasAreaType": to_object_property("GrossFloorArea", namespace=bigg_enums),
                    "hasAreaUnitOfMeasurement": to_object_property("M2", namespace=units),
                },
                "mapping": {
                    "subject": {
                        "key": "gross_floor_area",
                        "operations": []
                    },
                    "areaValue": {
                        "key": 'Gross Floor Area',
                        "operations": []
                    }
                }
            }
        }

        gross_floor_area_above_ground = {
            "name": "gross_floor_area_above_ground",
            "class": Area,
            "type": {
                "origin": "row"
            },
            "params": {
                "raw": {
                    "hasAreaType": to_object_property("GrossFloorAreaAboveGround", namespace=bigg_enums),
                    "hasAreaUnitOfMeasurement": to_object_property("M2", namespace=units)
                },
                "mapping": {
                    "subject": {
                        "key": "gross_floor_area_above_ground",
                        "operations": []
                    },
                    "areaValue": {
                        "key": 'Gross Floor Area Above Ground',
                        "operations": []
                    }
                }
            }
        }

        gross_floor_area_under_ground = {
            "name": "gross_floor_area_under_ground",
            "class": Area,
            "type": {
                "origin": "row"
            },
            "params": {
                "raw": {
                    "hasAreaType": to_object_property("GrossFloorAreaUnderGround", namespace=bigg_enums),
                    "hasAreaUnitOfMeasurement": to_object_property("M2", namespace=units),
                },
                "mapping": {
                    "subject": {
                        "key": "gross_floor_area_under_ground",
                        "operations": []
                    },
                    "areaValue": {
                        "key": 'Gross Floor Area Under Ground',
                        "operations": []
                    }
                }
            }
        }
        building_element = {
            "name": "building_element",
            "class": BuildingConstructionElement,
            "type": {
                "origin": "row"
            },
            "params": {
                "raw": {
                    "hasBuildingConstructionElementType": to_object_property("OtherBuildingConstructionElement",
                                                                             namespace=bigg_enums),
                },
                "mapping": {
                    "subject": {
                        "key": "building_element",
                        "operations": []
                    }
                }
            }
        }
        grouped_modules = {
            "all": [department_organization, building_organization, building, location_info, cadastral_info,
                    building_space, gross_floor_area, gross_floor_area_under_ground, gross_floor_area_above_ground,
                    building_element]
        }
        return grouped_modules[group]
