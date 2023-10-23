# Genercat description
Genercat(Energy Eficiency Measures from Generalitat) is an application to describe the energy efficiency measures (eem) applied in buildings and properties
belonging to the Generalitat de Catalunya. 

## Gathering tool
This data source comes in the format of an Excel file where each row is the information about a EEM.

#### RUN import application
To run the import application, execute the python script with the following parameters:

```bash
python3 -m gather -so Genercat -f <file> -u <user> -n <namespace> -st storage
```


## Raw Data Format

The data imported will be stored in the Hbase table, each endpoint that provides a different kind of information will have its own  row key, that will be generated as follows:

| class | Hbase key    |
|-------|--------------|
| eem   | checksum~row |

*Mapping key from Genercat source where `checksum` is the checksum of the filename and the row where the measure is in the file.


## Harmonization

The harmonization of the data will be done with the following mapping:

#### BuildingConstructionElement =>
| Origin | Harmonization     |
|--------|-------------------|
 |        | only link         | 


#### EnergyEfficiencyMeasure =>
| Origin                                                         | Harmonization                                 |
|----------------------------------------------------------------|-----------------------------------------------|
 | value(Euro)                                                    | hasEnergyEfficiencyMeasureInvestmentCurrency  | 
 | value(1)                                                       | energyEfficiencyMeasureCurrencyExchangeRate   | 
 | Tipus de millora (1,2,3,4)                                     | hasEnergyEfficiencyMeasureType                | 
 | Descripció                                                     | energyEfficiencyMeasureDescription            | 
 | % de la instal·lació millorada / Potencia FV instal·lada [kW]  | shareOfAffectedElement                        | 
 | Data de finalització de l'obra / millora                       | energyEfficiencyMeasureOperationalDate        | 
 | Inversió \n(€) \n(IVA no inclòs)                               | energyEfficiencyMeasureInvestment             | 


## Import script information

For each import run, the information stored regarding the status of this import will be a document containing the 
following information:
```json
{
    "user" : "the user that imported data",
    "logs": {
      "gather" : "list with the logs of the import",
      "logs.store" : "list with the logs of the store",
      "logs.harmonize" : "list with the logs of the harmonization"
    },
    "log_exec": "timestamp of execution"
}
```