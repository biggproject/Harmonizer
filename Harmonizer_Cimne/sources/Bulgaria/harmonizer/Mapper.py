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
        organization = {
            "name": "organization",
            "class": Organization,
            "type": {
                "origin": "static"
            },
            "params": {
                "raw": {
                    "subject": "bulgaria",
                    "organizationName": "Bulgaria"
                }
            },
            "links": {
                "location_organization": {
                    "type": Bigg.hasSubOrganization,
                    "link": "__all__"
                }
            }
        }

        location_organization = {
            "name": "location_organization",
            "class": Organization,
            "type": {
                "origin": "row"
            },
            "params": {
                "raw": {
                    "organizationDivisionType": "Location"
                },
                "mapping": {
                    "subject": {
                        "key": "location_org_subject",
                        "operations": []
                    },
                    "organizationName": {
                        "key": "Municipality",
                        "operations": []
                    }
                }
            },
            "links": {
                "building_organization": {
                    "type": Bigg.hasSubOrganization,
                    "link": "organization_subject"
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
                        "key": "organization_subject",
                        "operations": []
                    },
                    "organizationName": {
                        "key": "building_name",
                        "operations": []
                    }
                }
            },
            "links": {
                "buildings": {
                    "type": Bigg.managesBuilding,
                    "link": "subject"
                }
            }
        }

        buildings = {
            "name": "buildings",
            "class": Building,
            "type": {
                "origin": "row"
            },
            "params": {
                "mapping": {
                    "subject": {
                        "key": "building_subject",
                        "operations": []
                    },
                    "buildingName": {
                        "key": "building_name",
                        "operations": []
                    },
                    "buildingIDFromOrganization": {
                        "key": "building_id",
                        "operations": []
                    }
                }
            },
            "links": {
                "building_space": {
                    "type": Bigg.hasSpace,
                    "link": "subject"
                },
                "location_info": {
                    "type": Bigg.hasLocationInfo,
                    "link": "subject"
                },
                "energy_performance_certificate_before": {
                    "type": Bigg.hasEPC,
                    "link": "subject"
                },
                "energy_performance_certificate_after": {
                    "type": Bigg.hasEPC,
                    "link": "subject"
                },
                "project": {
                    "type": Bigg.hasProject,
                    "link": "subject"
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
                    "hasAddressCountry": countries["732800/"]
                },
                "mapping": {
                    "subject": {
                        "key": "location_subject",
                        "operations": []
                    },
                    "hasAddressCity": {
                        "key": "hasAddressCity",
                        "operations": []
                    },
                    "addressTimeZone": {
                        "key": "timezone",
                        "operations": []
                    }
                }
            }
        }

        energy_performance_certificate_before = {
            "name": "energy_performance_certificate_before",
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
                        "key": "epc_date_before",
                        "operations": []
                    },
                    "energyPerformanceCertificateClass": {
                        "key": "EPC_Energy class_Before",
                        "operations": []
                    },
                    "annualFinalEnergyConsumption": {
                        "key": "Annual energy consumption - before_Correction (normalisation) of the consumptions_kWh/a",
                        "operations": []
                    }
                }
            }
        }

        energy_performance_certificate_after = {
            "name": "energy_performance_certificate_after",
            "class": EnergyPerformanceCertificate,
            "type": {
                "origin": "row"
            },
            "params": {
                "mapping": {
                    "subject": {
                        "key": "epc_after_subject",
                        "operations": []
                    },
                    "energyPerformanceCertificateDateOfAssessment": {
                        "key": "epc_date",
                        "operations": []
                    },
                    "energyPerformanceCertificateClass": {
                        "key": "EPC_Energy class_After",
                        "operations": []
                    },
                    "annualFinalEnergyConsumption": {
                        "key": "consumption_total_after",
                        "operations": []
                    }
                }
            }
        }

        project = {
            "name": "project",
            "class": RenovationProject,
            "type": {
                "origin": "row"
            },
            "params": {
                "raw": {
                    "hasProjectInvestmentCurrency": units.BulgarianLev
                },
                "mapping": {
                    "subject": {
                        "key": "project_subject",
                        "operations": []
                    },
                    "projectIDFromOrganization": {
                        "key": "subject",
                        "operations": []
                    },
                    "projectStartDate": {
                        "key": "epc_date",
                        "operations": []
                    },
                    "projectInvestment": {
                        "key": "Total from the project_Investments_BGN",
                        "operations": []
                    },

                }
            },
            "links": {
                "energy_efficiency_measure": {
                    "type": Bigg.includesMeasure,
                    "link": "subject"
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
                    "buildingSpaceName": "Building"
                },
                "mapping": {
                    "subject": {
                        "key": "building_space_subject",
                        "operations": []
                    },
                    "hasBuildingSpaceUseType": {
                        "key": "buildingSpaceUseType",
                        "operations": []
                    },
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
                },
                # "device": {
                #     "type": Bigg.isObservedByDevice,
                #     "link": "subject"
                # },
                # "utilityPoint": {
                #     "type": Bigg.hasUtilityPointOfDelivery,
                #     "link": "subject"
                # }
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
                        "key": "GFA, m2",
                        "operations": []
                    }
                }
            }
        }

        element = {
            "name": "element",
            "class": BuildingConstructionElement,
            "type": {
                "origin": "row"
            },
            "params": {
                "raw": {
                    "hasBuildingConstructionElementType": bigg_enums["OtherBuildingConstructionElement"],
                },
                "mapping": {
                    "subject": {
                        "key": "element_subject",
                        "operations": []
                    }
                }
            },
            "links": {
                # "device": {
                #     "type": Bigg.isObservedByDevice,
                #     "link": "subject"
                # },
                "energy_efficiency_measure": {
                    "type": Bigg.isAffectedByMeasure,
                    "link": "subject"
                },
            }
        }

        building_space_link = {
            "name": "building_space_link",
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
                    "link": "building_space_subject"
                },
                "utilityPoint": {
                    "type": Bigg.hasUtilityPointOfDelivery,
                    "link": "building_space_subject"
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
                    "deviceName": {
                        "key": "name",
                        "operations": []
                    },
                    "hasDeviceType": {
                        "key": "device_type",
                        "operations": []
                    },

                }
            }
        }

        utilityPoint = {
            "name": "utilityPoint",
            "class": UtilityPointOfDelivery,
            "type": {
                "origin": "row"
            },
            "params": {
                "mapping": {
                    "subject": {
                        "key": "utility_subject",
                        "operations": []
                    },
                    "hasUtilityType": {
                        "key": "utility_type",
                        "operations": []
                    },
                    "pointOfDeliveryIDFromOrganization": {
                        "key": "name",
                        "operations": []
                    },
                }
            },
            "links": {
                "device": {
                    "type": Bigg.hasDevice,
                    "link": "building_space_subject"
                }
            }
        }
        energy_efficiency_measure = {
            "name": f"energy_efficiency_measure",
            "class": EnergyEfficiencyMeasure,
            "type": {
                "origin": "row"
            },
            "params": {
                "raw": {
                    "hasEnergyEfficiencyMeasureInvestmentCurrency": units["BulgarianLev"],
                    "energyEfficiencyMeasureCurrencyExchangeRate": "0.51",
                },
                "mapping": {
                    "subject": {
                        "key": f"eem_subject",
                        "operations": []
                    },
                    "hasEnergyEfficiencyMeasureType": {
                        "key": "measure_type",
                        "operations": []
                    },
                    "energyEfficiencyMeasureInvestment": {
                        "key": f"Investments",
                        "operations": []
                    },
                    "energyEfficiencyMeasureOperationalDate": {
                        "key": f"epc_date",
                        "operations": []
                    }
                },
            }

        }

        grouped_modules = {
            "building_info": [organization, location_organization, building_organization, buildings, building_space,
                              gross_floor_area, location_info, energy_performance_certificate_before,
                              energy_performance_certificate_after, element,  # device, utilityPoint,
                              project],
            "building_upods_dev": [building_space_link, device, utilityPoint],
            "eem_savings": [element, energy_efficiency_measure, project],
        }
        return grouped_modules[group]
