# SimpleTariff description

The simpleTariff is a source created to be able to upload an excel file with the tariff hourly timeseries for 1 year duration.


## Gathering tool

This data source comes in the format of an Excel file where each row is an invoice related to a supply.
Each tariff will have a list of uploaded files, and the periods they are affecting.
METADATA:
tariff_related.
    -File with 8784 rows
    -start_date -> end_date
If the day does not exist in the year (29-2) the rows related to this time will be ignored.
The data will be replicated for all the time, if several years are indicated, at the same time and hour.

#### RUN import application
To run the import application, execute the python script with the following parameters:

```bash
python3 -m gather -so SimpleTariff -f <file> -n <namespace> -u <user_importing> -tar <tariff_name> -ds <data_source> -di <dt_ini> -de <dt_end> -st <store> 
```

## Raw Data Format
The data imported will be stored in the Hbase table, each endpoint that provides a different kind of information will have its own  row key, that will be generated as follows:

 | class       | Hbase key                          |
|-------------|------------------------------------|
| Measurement | tariff_name~ts_ini~ts_end~file_pos |

*Mapping key for Nedgia source, where `ts_ini` is the starting date of the file, `ts_end` is the endign date and `file_pos` the row of the data into the file*

## Harmonization

The harmonization of the data will be done with the following mapping:

#### Tariff=>
| Origin      | Harmonization |
|-------------|---------------|
 | tariff_name | tariffName    | 


#### Measurements =>

| Hbase Row                 | Value  | isReal | table tyoe |
|---------------------------|--------|--------|------------|
| bucket~sensor_code~ts_ini | value  | True   | Online     |
| bucket~ts_ini~sensor_code | value  | True   | Batch      |

`bucket` is calculated with: `(ts_ini // 10000000 ) % 20`
`sensor_code` is the hash `sha256` of the sensor URI


## Import script information

For each import run a log document will be stored in mongo:
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


