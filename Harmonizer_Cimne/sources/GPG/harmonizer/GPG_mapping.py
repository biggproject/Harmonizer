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
        organization_gene = {
            "name": "organization_gene",
            "class": Organization,
            "type": {
                "origin": "static",
            },
            "params": {
                "raw": {
                    "subject": "generalitat-de-catalunya",
                }
            },
            "links": {
                # "main_organization": {
                #     "type": Bigg.hasSuperOrganization,
                #     "link": "__all__"
                # },
                "organization_unfound": {
                    "type": Bigg.hasSubOrganization,
                    "link": "__all__"
                }
            }
        }
        organization_organization = {
            "name": "organization_unfound",
            "class": Organization,
            "type": {
                "origin": "static",
            },
            "params": {
                "raw": {
                    "subject": "no-trobat",
                    "organizationName": "No Trobat",
                    "organizationDivisionType": "Department"
                }
            },
            "links": {
                # "main_organization": {
                #     "type": Bigg.hasSuperOrganization,
                #     "link": "__all__"
                # },
                "building_organization": {
                    "type": Bigg.hasSubOrganization,
                    "link": "__all__"
                }
            }
        }

        department_organization = {
            "name": "department_organization",
            "class": Organization,
            "type": {
                "origin": "row",
                "operations": [],
            },
            "params": {
                "raw": {
                    "organizationDivisionType": "Department"
                },
                "mapping": {
                    "subject": {
                        "key": "department_organization",
                        "operations": []
                    },
                }
            },
            "links": {
                # "main_organization": {
                #     "type": Bigg.hasSuperOrganization,
                #     "link": "__all__"
                # },
                "building_organization": {
                    "type": Bigg.hasSubOrganization,
                    "link": "Num ens"
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
                        "key": "Nom",
                        "operations": []
                    }
                }
            },
            "links": {
                # "department_organization": {
                #     "type": Bigg.hasSuperOrganization,
                #     "link": "Num_Ens_Inventari",
                #     "fallback": {
                #         "key": "main_organization",
                #         "bidirectional": Bigg.hasSubOrganization
                #     }
                # },
                "building": {
                    "type": Bigg.managesBuilding,
                    "link": "Num ens"
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
                        "key": "buildingIDFromOrganization",
                        "operations": []
                    },
                    "buildingName": {
                        "key": "Nom",
                        "operations": []
                    }
                }
            },
            "links": {
                # "building_organization": {
                #     "type": Bigg.pertainsToOrganization,
                #     "link": "Num_Ens_Inventari"
                # },
                "building_space": {
                    "type": Bigg.hasSpace,
                    "link": "Num ens"
                },
                "location_info": {
                    "type": Bigg.hasLocationInfo,
                    "link": "Num ens"
                },
                "cadastral_info": {
                    "type": Bigg.hasCadastralInfo,
                    "link": "Num ens"
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
                    "hasAddressCountry":
                        to_object_property("2510769/", namespace=countries),
                    "addressTimeZone": "Europe/Madrid"
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
                        "key": "Codi_postal",
                        "operations": []
                    },
                    "addressStreetNumber": {
                        "key": "Núm. via",
                        "operations": []
                    },
                    "addressStreetName": {
                        "key": "Via",
                        "operations": []
                    },
                    "addressLatitude": {
                        "key": "Component Y",
                        "operations": []
                    },
                    "addressLongitude": {
                        "key": "Component X",
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
                "mapping": {
                    "landArea": {
                        "key": "Sup. del terreny",
                        "operations": []
                    }  # ,
                    # "landType": {
                    #     "key": "Classificacio_sol",
                    #     "operations": [decode_hbase, ]
                    # }
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
                    "link": "Num ens"
                },
                "gross_floor_area_above_ground": {
                    "type": Bigg.hasArea,
                    "link": "Num ens"
                },
                "gross_floor_area_under_ground": {
                    "type": Bigg.hasArea,
                    "link": "Num ens"
                },
                "building_element": {
                    "type": Bigg.isAssociatedWithElement,
                    "link": "Num ens"
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
                        "key": "Sup. construïda total",
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
                        "key": "Sup. const. sobre rasant",
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
                        "key": "Sup. const. sota rasant",
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
            "main_org": [organization_gene, organization_organization, building_organization, building, location_info, cadastral_info,
                         building_space, gross_floor_area, gross_floor_area_under_ground, gross_floor_area_above_ground,
                         building_element],
            "dep_org": [department_organization, building_organization, building, location_info, cadastral_info,
                         building_space, gross_floor_area, gross_floor_area_under_ground, gross_floor_area_above_ground,
                         building_element],
            "buildings": [building_organization, building, location_info, cadastral_info, building_space,
                          gross_floor_area, gross_floor_area_under_ground, gross_floor_area_above_ground,
                          building_element]
        }
        return grouped_modules[group]

    def get_update_relationships(self, group):
        """The relationships returned will be removed from the graph before applying the update"""
        with_organization = {
                Bigg.hasSubOrganization: {
                    "rdf": {
                        "query": """SELECT ?s ?o WHERE {{
                            ?s <{p}> ?o. 
                            ?o <http://bigg-project.eu/ontology#organizationDivisionType> "Building"}}""",
                        "response": {"object": 1}
                    },
                    "cypher": {
                        "query": """Match (n)-[r:{p}]-() where n.uri in {object}"""
                    }
                },
                Bigg.hasAddressCountry: {
                    "rdf": {
                        "query": """SELECT ?s ?o WHERE {{?s <{p}> ?o}}""",
                        "response": {"object": 0}
                    },
                    "cypher": {
                        "query": """Match (n)-[r:{p}{{source:"GPG"}}]-() where n.uri in {object}"""
                    }
                },
                Bigg.hasAddressProvince: {
                    "rdf": {
                        "query": """SELECT ?s ?o WHERE {{?s <{p}> ?o}}""",
                        "response": {"object": 0}
                    },
                    "cypher": {
                        "query": """Match (n)-[r:{p}{{source:"GPG"}}]-() where n.uri in {object}"""
                    }
                },
                Bigg.hasAddressCity: {
                    "rdf": {
                        "query": """SELECT ?s ?o WHERE {{?s <{p}> ?o}}""",
                        "response": {"object": 0}
                    },
                    "cypher": {
                        "query": """Match (n)-[r:{p}{{source:"GPG"}}]-() where n.uri in {object}"""
                    }
                },
                Bigg.hasBuildingSpaceUseType: {
                    "rdf": {
                        "query": """SELECT ?s ?o WHERE {{?s <{p}> ?o}}""",
                        "response": {"object": 0}
                    },
                    "cypher": {
                        "query": """Match (n)-[r:{p}{{source:"GPG"}}]-() where n.uri in {object}"""
                    }
                },
                Bigg.hasArea: {
                    "rdf": {
                        "query": """SELECT ?s ?o WHERE {{?s <{p}> ?o}}""",
                        "response": {"object": 0}
                    },
                    "cypher": {
                        "query": """Match (n)-[r:{p}{{source:"GPG"}}]-() where n.uri in {object}"""
                    }
                },
            }
        only_buildings = {
                Bigg.hasAddressCountry: '{source:"GPG"}',
                Bigg.hasAddressProvince: '{source:"GPG"}',
                Bigg.hasAddressCity: '{source:"GPG"}',
                Bigg.hasBuildingSpaceUseType: '{source:"GPG"}',
                Bigg.hasArea: '{source:"GPG"}'
            }
        relationships_to_update = {
            "main_org": with_organization,
            "dep_org": with_organization,
            "buildings": only_buildings
        }
        return relationships_to_update[group]
