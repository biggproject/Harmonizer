from ontology.namespaces_definition import Bigg, bigg_enums
from ontology.bigg_classes import LocationInfo, BuildingSpace, Device, UtilityPointOfDelivery
from slugify import slugify
from utils.data_transformations import *


class Mapping(object):

    def __init__(self, source, namespace):
        self.source = source
        LocationInfo.set_namespace(namespace)
        BuildingSpace.set_namespace(namespace)
        UtilityPointOfDelivery.set_namespace(namespace)
        Device.set_namespace(namespace)

    def get_mappings(self, group):
        location_info = {
            "name": "location_info",
            "class": LocationInfo,
            "type": {
                "origin": "row"
            },
            "params": {
                "mapping": {
                    "subject": {
                        "key": "location_subject",
                        "operations": []
                    },
                    "hasAddressProvince": {
                        "key": 'hasAddressProvince',
                        "operations": []
                    },
                    "hasAddressCity": {
                        "key": 'hasAddressCity',
                        "operations": []
                    },
                    "addressPostalCode": {
                        "key":  'postalCode',
                        "operations": []
                    },
                    "addressStreetName": {
                        "key": 'address',
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
                "device": {
                    "type": Bigg.isObservedByDevice,
                    "link": "cups"
                },
                "utility_point": {
                    "type": Bigg.hasUtilityPointOfDelivery,
                    "link": "cups"
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
                "raw": {
                    "hasUtilityType": to_object_property("ElectricUtility", namespace=bigg_enums)
                },
                "mapping": {
                    "subject": {
                        "key": 'utility_point_subject',
                        "operations": []
                    },
                    "pointOfDeliveryIDFromOrganization": {
                        "key": 'utility_point_id',
                        "operations": []
                    },
                }
            },
            "links": {
                "device": {
                    "type": Bigg.hasDevice,
                    "link": "cups"
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
                "mapping": {
                    "subject": {
                        "key": "device_location_subject",
                        "operations": []
                    },
                    "hasAddressProvince": {
                        "key": 'hasAddressProvince',
                        "operations": []
                    },
                    "hasAddressCity": {
                        "key": 'hasAddressCity',
                        "operations": []
                    },
                    "addressPostalCode": {
                        "key": 'postalCode',
                        "operations": []
                    },
                    "addressStreetName": {
                        "key": 'address',
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
                    "hasDeviceType": to_object_property("Meter.EnergyMeter.Electricity", namespace=bigg_enums)
                },
                "mapping": {
                    "subject": {
                        "key": "device_subject",
                        "operations": []
                    },
                    "deviceName":  {
                        "key": 'cups',
                        "operations": []
                    }
                }
            },
            "links": {
                "device_location_info": {
                    "type": Bigg.hasDeviceLocationInfo,
                    "link": "cups"
                }
            }
        }

        grouped_modules = {
            "linked": [location_info, building_space, utility_point, device, device_location_info],
            "unlinked": [utility_point, device, device_location_info]
        }
        return grouped_modules[group]
