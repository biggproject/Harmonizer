# BULGARIA description
Bulgaria (Building Extraction from Bulgaria) is a formats defined by the Bulgarian partner to ENTRACK to introduce the building information, renovation Projects,
EnergyPerformanceCertificates, Consumptions and Savings for the Buildings.

## Gathering tool

The data can come in two different formats: Detail and Summary.

### Detail

> Description not available yet.

### Summary
This data source comes in the format of an Excel file where each row is the information about a renovation project.

#### RUN import application
To run the gathering application, execute the python script with the following parameters:

```bash
python3 -m gather -so Bulgaria -f <directory> -n <namespace> -u <user_importing> -t [detail, summary] -st <storage>
```

## Raw Data Format
The key of the file will be made generating a unique field based on the hash `md5` of the name of the file and the `row index` of the building, concatenated by "~".

This ensures that if the same file is reloaded, the data can be overwritten.
The rest of the data will be stored to the column family `info` with column using the raw format name.

## Harmonization

The harmonization of the file will be done with the following mapping:
### Detail
> Description not available yet.

### Summary


#### Organization =>
| Origin                               | Harmonization                                        |
|--------------------------------------|------------------------------------------------------|
 | value("Bulgaria")                    | only link                                            |
 | fileId-municipality-type_of_building | organizationName {organizationDivisionType: Building |


#### Building =>
| Origin                               | Harmonization              |
|--------------------------------------|----------------------------|
| filehash~fileId                      | buildingIDFromOrganization |
| fileId-municipality-type_of_building | buildingName               |

#### LocationInfo =>
| Origin           | Harmonization       |
|------------------|---------------------|
| value("732800/") | hasAddressCountry   | 
| Municipality     | hasAddressProvince  |

#### BuildingSpace =>
| Origin                     | Harmonization           |
|----------------------------|-------------------------|
| value(Building)            | buildingSpaceName       |
 | taxonomy(type_of_building) | hasBuildingSpaceUseType |

#### Area =>
| Origin                    | Harmonization                                                |
|---------------------------|--------------------------------------------------------------|
| gross_floor_area          | areaValue {units:M2, hasAreaType: GrossFloorArea }           |

#### BuildingElement =>
| Origin                                  | Harmonization                       |
|-----------------------------------------|-------------------------------------|
 | value(OtherBuildingConstructionElement) | hasBuildingConstructionElementType  |

#### EnergyPerfornamceCertificate =>
| Origin                                             | Harmonization                                                 |
|----------------------------------------------------|---------------------------------------------------------------|
 | epc_date_before  / epc_date                        | energyPerformanceCertificateDateOfAssessment {Before / After) |
| epc_energy_class_before /epc_energy_class_after    | energyPerformanceCertificateClass                             |
| annual_energy_consumption_before_total_consumption | annualFinalEnergyConsumption                                  |


#### BuildingElement =>
| Origin                                  | Harmonization                       |
|-----------------------------------------|-------------------------------------|
 | value(OtherBuildingConstructionElement) | hasBuildingConstructionElementType  |

#### RenovationProject =>
| Origin                               | Harmonization                |
|--------------------------------------|------------------------------|
| BulgarianLev                         | hasProjectInvestmentCurrency | 
| fileId-municipality-type_of_building | projectIDFromOrganization    | 
| projectStartDate                     | epc_date                     |
| total_savings_Investments            | projectInvestment            |

#### Devoce =>
| Origin | Harmonization |
|--------|---------------|
|        | only link     |

#### EnergyEfficiencyMeasure =>
| Origin                                  | Harmonization                                |
|-----------------------------------------|----------------------------------------------|
| BulgarianLev                            | hasEnergyEfficiencyMeasureInvestmentCurrency | 
| "0.51"                                  | energyEfficiencyMeasureCurrencyExchangeRate  |                              
| enum_energy_efficiency_measurement_type | hasEnergyEfficiencyMeasureType               |
|    measurement_{column}_Investments     | energyEfficiencyMeasureInvestment            |
|             epc_date                    | energyEfficiencyMeasureOperationalDate       | 

#### EnergySaving =>
| Origin                                  | Harmonization                  |
|-----------------------------------------|--------------------------------|
| energy_saving_type                      | hasEnergySavingType            | 
| measurement_{column}_{measurement_type} | hasEnergyEfficiencyMeasureType |
| epc_date_before                         | energySavingStartDate          |
| epc_date                                | energySavingEndDate            | 

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

