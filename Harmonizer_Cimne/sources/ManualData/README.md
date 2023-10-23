# ManualData description

To run the gathering application, execute the python script with the following parameters:

```bash
python3 -m gather -so ManualData <parameters>
```

The harmonization of the file will be done with the following mapping:

| Origin                  | Harmonization                               |
|-------------------------|---------------------------------------------|
| <field name>            | <field name> {raw field name: "Raw info"}   | 
| split(field name, sep)  | only link                                   | 
| value(<static raw>)     | <field name>                                |
| taxonomy(<field name>)  | <field name>                                |

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

