# Datadis description
Datadis(Datos de distribuïdora) is an API that provides energy consumption from the distribution company of spain in 15 min and 1 h rate.

## Gathering tool
This data source comes in the format of an [API](https://datadis.es/home), where different endpoints are available.
The gathering tool of datadis, will make use of the [beedis](https://github.com/BeeGroup-cimne/beedis) python library to obtain the data.

#### RUN import application
To run the import application, execute the python script with the following parameters:

```bash
python3 -m gather -so Datadis -st <storage> -p <policy>
```

where policy can be "last" to obtain the last period of each supply, or "repair" to try to import missing periods skipped in the past.

## Raw Data Format
The data imported will be stored in the Hbase table, each endpoint that provides a different kind of information will have its own  row key, that will be generated as follows:

 | endpoint  | Hbase key      |
|-----------|----------------|
 | supplies  | cups           |
 | data_1h   | cups~time_ini  |
| data_15m  | cups~time_ini  |
| max_power | cups~time_ini  |

*Mapping key from Datadis source where `time_ini` is the indicated time of the measure*

## Harmonization

The harmonization of the data will be done with the following mapping:

#### BuildingSpace =>
| Origin | Harmonization     |
|--------|-------------------|
 |        | only link         | 

#### LocationInfo =>
| Origin              | Harmonization       |
|---------------------|---------------------|
 | fuzzy(province)     | hasAddressProvince  | 
 | fuzzy(municipality) | hasAddressCity      | 
 | postalCode          | addressPostalCode   | 
 | address             | addressStreetName   | 

#### UtilityPointOfDelivery =>
| Origin  | Harmonization                     |
|---------|-----------------------------------|
| cups    | pointOfDeliveryIDFromOrganization |

#### Device =>
| Origin  | Harmonization  |
|---------|----------------|
 | cups    | deviceName     | 

#### Measurements =>

| Hbase Row                 | Value           | isReal                         | table tyoe |
|---------------------------|-----------------|--------------------------------|------------|
| bucket~sensor_code~ts_ini | consumptionKWH  | True if `Tipo Lectura` is Real | Online     |
| bucket~ts_ini~sensor_code | consumptionKWH  | True if `Tipo Lectura` is Real | Batch      |

`bucket` is calculated with: `(ts_ini // 10000000 ) % 20`
`sensor_code` is the hash `sha256` of the sensor URI

## Import script information

For each import run a log document will be stored in mongo:
```json
{
    "user" : "the user that imported data",
    "user_datasource": "the nif of the user importing the data",
    "logs": {
      "gather" : "list with the logs of the import",
      "store" : "list with the logs of the store",
      "harmonize" : "list with the logs of the harmonization"
    },
    "log_exec": "timestamp of execution"
}
```
Additionally, a set of collections for each timeseries device will be generated, splitting all the imported time period in
chunks and the status of importation of each chunk.

```json
{"_id": "cups for the device",
  "type of data": {
    "period": {
     "date_ini_block": "data_init for the block",
     "date_end_block": "date_end for the block",
     "values": "number of values obtained",
     "total": "total number of values",
     "retries": "retries for this chunk",
     "date_min": "minimum real data imported",
     "date_max": "maximum real data imported"
    }
   }
}
```

