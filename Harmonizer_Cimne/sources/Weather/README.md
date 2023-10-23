# Weather description
Weather data can be obtained from different weather sources, one of them is dakrsky.

## Gathering tool
This data source comes in the format of different API and implementations.
The gathering tool of weather, will make use of the [beemeteo](https://github.com/BeeGroup-cimne/beemeteo) python 
library to obtain the data, where different sources are available as can be seen in the documentation

#### RUN import application
To run the import application, execute the python script with the following parameters:

```bash
python3 -m gather -so Weather -st <storage>
```

## Raw Data Format
The data imported will be stored in the Hbase table automatically for the beemeteo library, so we are not going to use this
feature in this source.


## Harmonization

The harmonization of the data will be done with the following mapping:

#### Measurements =>

| Hbase Row                 | Value  | isReal | table tyoe |
|---------------------------|--------|--------|------------|
| bucket~sensor_code~ts_ini | value  | True   | Online     |
| bucket~ts_ini~sensor_code | value  | True   | Batch      |

`bucket` is calculated with: `(ts_ini // 10000000 ) % 20`
`sensor_code` is the hash `sha256` of the sensor URI

* The sensors are managed and obtained by quering the Neo4j objects in the configured Database, this way, the weatehr is obtained and no need to send the list of devices.

## Import script information

For each import run a log document will be stored in mongo:
```json
{
    "logs": {
      "gather" : "list with the logs of the import",
      "store" : "list with the logs of the store",
      "harmonize" : "list with the logs of the harmonization"
    },
    "log_exec": "timestamp of execution"
}
```
Additionally, a set of collections for each timeseries device will be generated, to keep the status for each weather station.

```json
{
 "_id": "station id",
  "type of data": {
     "date_ini": "initial real data imported",
     "date_end": "final real data imported"
  }
}
```
