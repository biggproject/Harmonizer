from .transform_functions import ref_cadastral
from ontology.namespaces_definition import Bigg, bigg_enums, units, countries
from ontology.bigg_classes import Organization, Building, LocationInfo, CadastralInfo, BuildingSpace, \
     Area, Device, BuildingConstructionElement, UtilityPointOfDelivery
from slugify import slugify as slugify
from utils.data_transformations import *


class Mapping(object):

    def __init__(self, source, namespace):
        self.source = source
        Organization.set_namespace(namespace)
        Building.set_namespace(namespace)
        LocationInfo.set_namespace(namespace)
        BuildingSpace.set_namespace(namespace)
        CadastralInfo.set_namespace(namespace)
        Area.set_namespace(namespace)
        BuildingConstructionElement.set_namespace(namespace)
        Device.set_namespace(namespace)
        UtilityPointOfDelivery.set_namespace(namespace)

    def get_mappings(self, group):

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
                        "key": "num_ens",
                        "operations": []
                    },
                    "buildingName": {
                        "key": "nom",
                        "operations": []
                    },
                    # "buildingUseType": {
                    #     "key": "subtipus",
                    #     "operations": [decode_hbase]#, building_type_taxonomy]
                    # },
                    # "buildingOwnership": {
                    #     "key": "responsable",
                    #     "operations": [decode_hbase]
                    # },
                }
            },
            "links": {
                "building_space": {
                    "type": Bigg.hasSpace,
                    "link": "dev_gem_id"
                },
                "location_info": {
                    "type": Bigg.hasLocationInfo,
                    "link": "dev_gem_id"
                },
                "cadastral_info": {
                    "type": Bigg.hasCadastralInfo,
                    "link": "dev_gem_id"
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
                    "hasAddressCountry": to_object_property("2510769/", namespace=countries),
                },
                "mapping": {
                    "subject": {
                        "key": "location_info",
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
                        "key": "codi_postal",
                        "operations": []
                    },
                    "addressStreetName": {
                        "key": "direccio",
                        "operations": []
                    },
                    "addressLongitude": {
                        "key": "longitud",
                        "operations": []
                    },
                    "addressLatitude": {
                        "key": "latitud",
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
                "column": "cadastral_info",
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
                    "link": "dev_gem_id"
                },
                "building_element": {
                    "type": Bigg.isAssociatedWithElement,
                    "link": "dev_gem_id"
                },
                "device": {
                    "type": Bigg.isObservedByDevice,
                    "link": "dev_gem_id"
                },
                "utility_point": {
                    "type": Bigg.hasUtilityPointOfDelivery,
                    "link": "dev_gem_id"
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
                        "key": "superficie",
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

        utility_point = {
            "name": "utility_point",
            "class": UtilityPointOfDelivery,
            "type": {
                "origin": "row"
            },
            "params": {
                "mapping": {
                    "subject": {
                        "key": "utility_point",
                        "operations": []
                    },
                    "pointOfDeliveryIDFromOrganization": {
                        "key": "utility_point_id",
                        "operations": []
                    },
                    "hasUtilityType": {
                        "key": "hasUtilityType",
                        "operations": []
                    }
                }
            },
            "links": {
                "device": {
                    "type": Bigg.hasDevice,
                    "link": "dev_gem_id"
                }
            }
        }
        device = {
            "name": "device",
            "class": Device,
            "type": {
                "origin": "row"
            },
            "params": {
                "mapping": {
                    "subject": {
                        "key": "device_subject",
                        "operations": []
                    },
                    "deviceName":  {
                        "key": "cups",
                        "operations": []
                    },
                    "hasDeviceType": {
                        "key": "hasDeviceType",
                        "operations": []
                    },
                    "contractedPower": {
                        "key": "contractedPower",
                        "operations": []
                    },
                    "tariff": {
                        "key": "tarifa_acces",
                        "operations": []
                    }
                }
            },
            "links": {
                "device_location_info": {
                    "type": Bigg.hasDeviceLocationInfo,
                    "link": "dev_gem_id"
                }
            }
        }

        device_location_info = {
            "name": "device_location_info",
            "class": LocationInfo,
            "type": {
                "origin": "row"
            },
            "params": {
                "raw": {
                    "hasAddressCountry": to_object_property("2510769/", namespace=countries),
                },
                "mapping": {
                    "subject": {
                        "key": "device_location_info",
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
                        "key": "codi_postal",
                        "operations": []
                    },
                    "addressStreetName": {
                        "key": "direccio",
                        "operations": []
                    },
                    "addressLongitude": {
                        "key": "longitud",
                        "operations": []
                    },
                    "addressLatitude": {
                        "key": "latitud",
                        "operations": []
                    }
                }
            }
        }
        grouped_modules = {
            "linked": [building, location_info, cadastral_info, building_space,
                       gross_floor_area, building_element, device, utility_point, device_location_info],
            "unlinked": [utility_point, device, device_location_info]
        }
        return grouped_modules[group]
