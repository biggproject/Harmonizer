from ontology.bigg_classes import LocationInfo, CadastralInfo, Building, EnergyPerformanceCertificate, \
    EnergyPerformanceCertificateAdditionalInfo
from ontology.namespaces_definition import countries


class Mapper(object):
    def __init__(self, source, namespace):
        self.source = source
        LocationInfo.set_namespace(namespace)
        CadastralInfo.set_namespace(namespace)
        Building.set_namespace(namespace)
        EnergyPerformanceCertificate.set_namespace(namespace)
        EnergyPerformanceCertificateAdditionalInfo.set_namespace(namespace)

    def get_mappings(self, group):
        location = {
            "name": "location",
            "class": LocationInfo,
            "type": {
                "origin": "row"
            },
            "params": {
                "raw": {
                    "hasAddressCountry": countries["2510769/"]
                },
                "mapping": {
                    "subject": {
                        "key": "location_subject",
                        "operations": []
                    },
                    "addressStreetName": {
                        "key": "adre_a",
                        "operations": []
                    },
                    "addressStreetNumber": {
                        "key": "numero",
                        "operations": []
                    },
                    "addressPostalCode": {
                        "key": "codi_postal",
                        "operations": []
                    },
                    "addressLongitude": {
                        "key": "longitud",
                        "operations": []
                    },
                    "addressLatitude": {
                        "key": "latitud",
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
                }
            }
        }

        cad_ref = {
            "name": "cad_ref",
            "class": CadastralInfo,
            "type": {
                "origin": "row"
            },
            "params": {
                "mapping": {
                    "subject": {
                        "key": "cadastral_subject",
                        "operations": []
                    },
                    "landArea": {
                        "key": "metres_cadastre",
                        "operations": []
                    },
                    "landCadastralReference": {
                        "key": "referencia_cadastral",
                        "operations": []
                    }
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
                        "key": "building_subject",
                        "operations": []
                    },
                    "buildingConstructionYear": {
                        "key": "any_construccio",
                        "operations": []
                    },
                    "hasLocationInfo": {
                        "key": "location_uri",
                        "operations": []
                    },
                    "hasCadastralInfo": {
                        "key": "cadastral_uri",
                        "operations": []
                    },
                    "hasEPC": {
                        "key": "epc_uri",
                        "operations": []
                    }
                }
            }
        }

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
                    "energyPerformanceCertificateReferenceNumber": {
                        "key": "num_cas",
                        "operations": []
                    },
                    "energyPerformanceCertificateClass": {
                        "key": "qualificaci_de_consum_d",
                        "operations": []
                    },
                    "energyPerformanceCertificateDateOfAssessment": {
                        "key": "data_entrada",
                        "operations": []
                    },
                    "C02EmissionsClass": {
                        "key": "qualificacio_d_emissions",
                        "operations": []
                    },
                    "annualC02Emissions": {
                        "key": "emissions_de_co2",
                        "operations": []
                    },
                    "annualEnergyCost": {
                        "key": "cost_anual_aproximat_d_energia",
                        "operations": []
                    },
                    "annualFinalEnergyConsumption": {
                        "key": "consum_d_energia_final",
                        "operations": []
                    },
                    "energyPerformanceCertificateCertificationTool": {
                        "key": "eina_de_certificacio",
                        "operations": []
                    },
                    "annualCoolingCO2Emissions": {
                        "key": "emissions_refrigeraci",
                        "operations": []
                    },
                    "coolingCO2EmissionsClass": {
                        "key": "qualificaci_emissions_1",
                        "operations": []
                    },
                    "annualHeatingCO2Emissions": {
                        "key": "emissions_calefacci",
                        "operations": []
                    }, "heatingCO2EmissionsClass": {
                        "key": "qualificaci_emissions",
                        "operations": []
                    },
                    "annualHotWaterCO2Emissions": {
                        "key": "emissions_acs",
                        "operations": []
                    },
                    "hotWaterCO2EmissionsClass": {
                        "key": "qualificaci_emissions_acs",
                        "operations": []
                    },
                    "annualLightingCO2Emissions": {
                        "key": "emissions_enllumenament",
                        "operations": []
                    },
                    "lightingCO2EmissionsClass": {
                        "key": "qualificaci_emissions_2",
                        "operations": []
                    },
                    "hotWaterPrimaryEnergyClass": {
                        "key": "qualificaci_energia_acs",
                        "operations": []
                    },
                    "lightingPrimaryEnergyClass": {
                        "key": "qualificaci_energia_1",
                        "operations": []
                    },
                    "heatingEnergyDemandClass": {
                        "key": "qualificaci_energia_calefacci_1",
                        "operations": []
                    },
                    "energyPerformanceCertificateCertificationMotivation": {
                        "key": "motiu_de_la_certificacio",
                        "operations": []
                    },
                    "hasAdditionalInfo": {
                        "key": "additional_epc_uri",
                        "operations": []
                    },

                }
            }
        }

        additional_epc = {
            "name": "additional_epc",
            "class": EnergyPerformanceCertificateAdditionalInfo,
            "type": {
                "origin": "row"
            },
            "params": {
                "mapping": {
                    "subject": {
                        "key": "additional_epc_subject",
                        "operations": []
                    },
                    "electricVehicleChargerPresence": {
                        "key": "vehicle_electric",
                        "operations": []
                    },
                    "solarThermalSystemPresence": {
                        "key": "solar_termica",
                        "operations": []
                    },
                    "solarPVSystemPresence": {
                        "key": "solar_fotovoltaica",
                        "operations": []
                    },
                    "biomassSystemPresence": {
                        "key": "sistema_biomassa",
                        "operations": []
                    },
                    "districtHeatingOrCoolingConnection": {
                        "key": "xarxa_districte",
                        "operations": []
                    },
                    "geothermalSystemPresence": {
                        "key": "energia_geotermica",
                        "operations": []
                    },
                    "averageWindowsTransmittance": {
                        "key": "valor_finestres",
                        "operations": []
                    },
                    "averageFacadeTransmittance": {
                        "key": "valor_aillaments",
                        "operations": []
                    }
                }
            }
        }

        grouped_modules = {
            "linked": [location, cad_ref, building, epc, additional_epc],
            "unlinked": [epc, additional_epc],
        }
        return grouped_modules[group]
