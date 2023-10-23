from utils.data_transformations import *
from ontology.namespaces_definition import Bigg, bigg_enums, units, countries
from ontology.bigg_classes import Device, UtilityPointOfDelivery, BuildingSpace, LocationInfo


class Mapper(object):
    def __init__(self, source, namespace):
        self.source = source
        Device.set_namespace(namespace)
        UtilityPointOfDelivery.set_namespace(namespace)
        BuildingSpace.set_namespace(namespace)
        LocationInfo.set_namespace(namespace)

    def get_mappings(self, group):
        building_space = {
            "name": "building_space",
            "class": BuildingSpace,
            "type": {
                "origin": "row"
            },
            "params": {
                "mapping": {
                    "subject": {
                        "key": "building_space_uri",
                        "operations": []
                    }
                }
            },
            "links": {
                "device": {
                    "type": Bigg.isObservedByDevice,
                    "link": "supply_uri"
                },
                "supply": {
                    "type": Bigg.hasUtilityPointOfDelivery,
                    "link": "supply_uri"
                }
            }
        }
        supply = {
            "name": "supply",
            "class": UtilityPointOfDelivery,
            "type": {
                "origin": "row"
            },
            "params": {
                "raw": {
                    "hasUtilityType": to_object_property("GasUtility", namespace=bigg_enums),
                },
                "mapping": {
                    "subject": {
                        "key": "supply_uri",
                        "operations": []
                    },
                    "pointOfDeliveryIDFromOrganization": {
                        "key": "supply_name",
                        "operations": []
                    }
                }
            },
            "links": {
                "device": {
                    "type": Bigg.hasDevice,
                    "link": "supply_uri"
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
                "mapping": {
                    "subject": {
                        "key": "location_uri",
                        "operations": []
                    },
                    "hasAddressProvince": {
                        "key": "province",
                        "operations": []
                    },
                    "hasAddressCity": {
                        "key": "municipality",
                        "operations": []
                    },
                    "addressPostalCode": {
                        "key": "postal_code",
                        "operations": []
                    },
                    "addressStreetName": {
                        "key": "address",
                        "operations": []
                    }
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
                "raw": {
                    "hasDeviceType": to_object_property("Meter.EnergyMeter.Gas", bigg_enums)
                },
                "mapping": {
                    "subject": {
                        "key": "device_uri",
                        "operations": []
                    },
                    "deviceName": {
                        "key": "device_name",
                        "operations": []
                    },

                    # "toll": {
                    #     "key": "tc",
                    #     "operations": []
                    # },
                    # "renoteManagement": {
                    #     "key": "auto",
                    #     "operations": []
                    # }
                }
            },
            "links": {
                "location_info": {
                    "type": Bigg.hasDeviceLocationInfo,
                    "link": "supply_uri"
                }
            }
        }

        grouped_modules = {
            "electric_ps_linked": [supply, device, building_space, location_info],
            "electric_ps_unlinked": [supply, device, location_info],
        }
        return grouped_modules[group]

