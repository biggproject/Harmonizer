# BIS description
BIS(Building Information Source) is file defined by CIMNE to introduce the building information or patrimony available for any
organization. The file format can be obtained [here](sources/BIS/BIS.xls)

## Gathering tool
This data source comes in the format of an Excel file where each row is the information about a building.

#### RUN import application
To run the gathering application, execute the python script with the following parameters:

```bash
python3 -m gather -so BIS -f <file> -n <namespace> -u <user_importing> -st <storage>
```

## Raw Data Format
The key of the file will be made using the unique field `Unique Code`, that will unequivocally identify each 
building. Future changes on the building will override the previous values. The rest of the data will be stored
to the column family `info` with column using the raw format name.

## Harmonization

The harmonization of the file will be done with the following mapping:

#### Organization =>
| Origin                 | Harmonization                                           |
|------------------------|---------------------------------------------------------|
 | Name                   | organizationName {organizationDivisionType: "Building"} | 
 | split(Departments, ";" | only link                                               | 

#### Building =>
| Origin                        | Harmonization              |
|-------------------------------|----------------------------|
| Unique Code                   | buildingIDFromOrganization |
| Name                          | buildingName               |

#### LocationInfo =>
| Origin                        | Harmonization       |
|-------------------------------|---------------------|
| Country                       | hasAddressCountry   | 
| Province                      | hasAddressProvince  |
| Municipality                  | hasAddressCity      |
| Road                          | addressStreetName   |
| Road Number                   | addressStreetNumber |
| PostalCode                    | addressPostalCode   | 
| Longitud                      | addressLongitude    |
| Latitud                       | addressLatitude     |

#### CadastralInfo =>
| Origin                           | Harmonization            |
|----------------------------------|--------------------------|
| split(Cadastral References, ";") | landCadastralReference   |
| Land Area                        | landArea                 |

#### BuildingSpace =>
| Origin             | Harmonization           |
|--------------------|-------------------------|
| value(Building)    | buildingSpaceName       |
 | taxonomy(Use Type) | hasBuildingSpaceUseType |

#### Area =>
| Origin                         | Harmonization                                                |
|--------------------------------|--------------------------------------------------------------|
| Gross Floor Area Above Ground  | areaValue {units:M2, hasAreaType: GrossFloorAreaAboveGround} |
| Gross Floor Area Under Ground  | areaValue {units:M2, hasAreaType: GrossFloorAreaUnderGround} |
| Gross Floor Area               | areaValue {units:M2, hasAreaType: GrossFloorArea }           |

#### BuildingElement =>
| Origin                                  | Harmonization                       |
|-----------------------------------------|-------------------------------------|
 | value(OtherBuildingConstructionElement) | hasBuildingConstructionElementType  |



## Import script logging

For each import run a log document will be stored in mongo to identify the problems that may arise during the execution:
```json
{
    "user" : "the user that imported data",
    "logs": {
      "gather" : "list with the logs of the import",
      "store" : "list with the logs of the store",
      "harmonize" : "list with the logs of the harmonization"
    },
    "log_exec": "timestamp of execution"
}
```

