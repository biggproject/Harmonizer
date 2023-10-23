# Gemweb description
Gemweb is an application to manage the invoices and increase energy efficiency of buildings and properties.

## Gathering tool
This data source comes in the format of an [API](http://manual.gemweb.es/), where different endpoints are available.
The gathering tool of datadis, will make use of the [beegweb](https://github.com/BeeGroup-cimne/beegweb) python library to obtain the data.

**Since the Gemweb Application is not used anymore for Generalitat de catalunya, we only use the already imported data to create links between building and Devices.**

#### RUN import application
To run the import application, execute the python script with the following parameters:

```bash
python3 -m gather -so Gemweb -st <storage>
```

## Raw Data Format
The data imported will be stored in the Hbase table, each endpointthat provides a different kind of information will have its own  row key, that will be generated as follows:

| endpoint    | Hbase key          |
|-------------|--------------------|
| building    |        id          |
| entities    |        id          | 
| supplies    |        id          |
| invoices    |      id~d_mod      |
| time-series |    id~timestamp    |

*Mapping keys from Gemweb source where `d_mod` is the modification date and `time_ini` is the time of the measure*

## Harmonization
The harmonization of the data will be done with the following mapping:

#### Building =>
| Origin  | Harmonization              |
|---------|----------------------------|
 | num_ens | buildingIDFromOrganization | 
| nom     | buildingName               |

#### LocationInfo =>
| Origin      | Harmonization               |
|-------------|-----------------------------|
 | pais        | hasAddressCountry           | 
| provincia   | hasAddressProvince          |
| poblacio    | hasAddressCity              |
| codi_postal | addressPostalCode           |
| direccio    | addressStreetName           |
| longitud    | addressLongitude            |
| latitud     | addressLatitude             |


#### CadastralInfo =>
| Origin                    | Harmonization               |
|---------------------------|-----------------------------|
 | split(cadastral_info,";") | landCadastralReference      | 

#### BuildingSpace =>
| Origin               | Harmonization               |
|----------------------|-----------------------------|
 | taxonomy(subtipus)   | hasBuildingSpaceUseType     | 


#### Area =>
| Origin      | Harmonization                                                |
|-------------|--------------------------------------------------------------|
 | superficie  | areaValue {units:M2, hasAreaType: GrossFloorArea }           |


#### BuildingConstructionElement =>
| Origin | Harmonization |
|--------|---------------|
 |        | only link     |


#### UtilityPointOfDelivery =>
| Origin                 | Harmonization                     |
|------------------------|-----------------------------------|
 | cups                   | pointOfDeliveryIDFromOrganization |
 | taxonomy(tipus_submin) | hasUtilityType                    |

#### Device =>
| Origin                      | Harmonization |
|-----------------------------|---------------|
 | cups                        | deviceName    |
 | value("Meter.EnergyMeter" ) | hasDeviceType |

## Import script information

For each static data import run, the information stored regarding the status of this import will be a document containing the 
following information:
```json
{
    "user" : "the user that imported data",
    "user_datasource": "the username of the user importing the data",
    "logs": {
      "gather" : "list with the logs of the import",
      "store" : "list with the logs of the store",
      "harmonize" : "list with the logs of the harmonization"
    },
    "log_exec": "timestamp of execution"
}
```
For the timeseries, we will contain a document for each imported device and each granularity, the information contained will be:
**USER BANNED**
```json

```



[//]: # (```bash)

[//]: # (## import static data)

[//]: # (#python Gemweb/gemweb_gather.py -d <data_type>)

[//]: # (## where data_type can be one of ['entities', 'buildings', 'solarpv', 'supplies', 'invoices'])

[//]: # (#)

[//]: # (## import timeseries)

[//]: # (#python Gemweb/timeseries_gather.py)

[//]: # (```)