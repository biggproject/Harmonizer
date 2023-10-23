from ontology.bigg_classes import Device, Sensor


class Mapper(object):
    def __init__(self, source, namespace):
        self.source = source
        Device.set_namespace(namespace)
        Sensor.set_namespace(namespace)

    def get_mappings(self, group):
        devices = {
            "name": "device",
            "class": Device,
            "type": {
                "origin": "row"
            },
            "params": {
                "raw": {
                    "deviceNumberOfOutputs": 1,
                },
                "mapping": {
                    "subject": {
                        "key": "device_subject",
                        "operations": []
                    },
                    "deviceIDFromOrganization": {
                        "key": "Standard Naming Complex",
                        "operations": []
                    },
                    "deviceName": {
                        "key": "BACnet Name",
                        "operations": []
                    },
                    "observesSpace": {
                        "key": "observesSpace",
                        "operations": []
                    },
                    "hasSensor": {
                        "key": "hasSensor",
                        "operations": []
                    },
                    "hasDeviceType": {
                        "key": "hasDeviceType",
                        "operations": []
                    }
                }
            }
        }

        sensors = {
            "name": "sensors",
            "class": Sensor,
            "type": {
                "origin": "row"
            },
            "params": {
                "mapping": {
                    "subject": {
                        "key": "sensor_subject",
                        "operations": []
                    },
                    "hasMeasuredProperty": {
                        "key": "measuredProperty_link",
                        "operations": []
                    }
                }
            }
        }

        grouped_modules = {
            "all": [devices, sensors]

        }
        return grouped_modules[group]
