# CEEE description
CEEE (Certificats d’eficiència energètica d’edificis) is dataset of the energy certificates that are compulsory for the existing buildings or dwellings in Catalonia.

## Raw Data Format
This data source is obtained from the API, where we can obtain all the certificates. 

## Import script information

For each import run, the information stored regarding the status of this import will be a document containing the 
following information:

```json
{
    "version" : "the version of the import, to be able to get the current data",
    "date" : "datetime of execution",
    "user" : "user importing this file"
}

```


## RUN import application
To run the import application, execute the python script with the following parameters:

```bash
python DadesObertes/CEEE/gather.py
```
