# Nedgia description
Nedgia(Gas supplier) is a datasource containing the timeseries of gas invoices

## Gathering tool
This data source comes in the format of an Excel file where each row is an invoice related to a supply.

#### RUN import application
To run the import application, execute the python script with the following parameters:

```bash
python3 -m gather -so Nedgia -f <file> -n <namespace> -u <user_importing> -tz <file_timezone> -st <storage>
```

## Raw Data Format
The data imported will be stored in the Hbase table, each endpoint that provides a different kind of information will have its own  row key, that will be generated as follows:

 | class       | Hbase key   |
|-------------|-------------|
| Measurement | cups~ts_ini |

*Mapping key for Nedgia source, where `ts_ini` is the starting date of the invoice*

## Harmonization

The harmonization of the data will be done with the following mapping:

#### BuildingSpace=>
| Origin | Harmonization |
|--------|---------------|
 |        | only link     | 

#### UtilityPointOfDelivery=>
| Origin | Harmonization                     |
|--------|-----------------------------------|
 | CUPS   | pointOfDeliveryIDFromOrganization | 

#### Device=>
| Origin                     | Harmonization  |
|----------------------------|----------------|
 | value("Meter.EnergyMeter") | hasDeviceType  | 
 | CUPS                       | deviceName     | 


#### Measurements =>

| Hbase Row                 | Value                                | isReal                         | table tyoe |
|---------------------------|--------------------------------------|--------------------------------|------------|
| bucket~sensor_code~ts_ini | 'Consumo kWh ATR + Consumo kWh GLP'  | True if `obtainMethod` is Real | Online     |
| bucket~ts_ini~sensor_code | 'Consumo kWh ATR + Consumo kWh GLP'  | True if `obtainMethod` is Real | Batch      |

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


`