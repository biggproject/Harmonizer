# GPG description
GPG(Gestió de Patrimoni de la Generalitat) is an application to manage the inventory of buildings and properties
belonging to the Generalitat de Catalunya. 

## Gathering tool
This data source comes in the format of an Excel file where each row is the information about a building. 

#### RUN import application
To run the import application, execute the python script with the following parameters:

```bash
python3 -m gather -so GPG -f <gpg file> -n <namespace> -u <user_importing> -st <storage>
```


## Raw Data Format
The data imported will be stored in the Hbase table, each endpoint that provides a different kind of information will have its own  row key, that will be generated as follows:

 |  class    | Hbase key          |
|-----------|--------------------|
|  building | Num_Ens_Inventari  |

*Mapping key from GPG source*

## Harmonization

The harmonization of the data will be done with the following mapping:

#### Organization (only Link)=>
| Origin                          | Harmonization            |
|---------------------------------|--------------------------|
 | value(Organization /Department) | organizationDivisionType | 

#### Organization (Building)=>
| Origin          | Harmonization            |
|-----------------|--------------------------|
 | value(Building) | organizationDivisionType | 
 | Espai           |  organizationName        | 


#### Building=>
| Origin            | Harmonization              |
|-------------------|----------------------------|
 | Num_Ens_Inventari | buildingIDFromOrganization | 
 | Espai             | buildingName               | 


#### LocationInfo=>
| Origin           | Harmonization               |
|------------------|-----------------------------|
 | value(2510769/)  | hasAddressCountry           | 
 | fuzzy(Provincia) | hasAddressProvince          | 
 | fuzzy(Municipi)  | hasAddressCity              | 
 | Codi_postal      | addressPostalCode           | 
 | Num_via          | addressStreetNumber         | 
 | Via              | addressStreetName           | 

#### CadastralInfo=>
| Origin                   | Harmonization          |
|--------------------------|------------------------|
 | split(cadastral_info, ;) | landCadastralReference | 
 | Sup_terreny              | landArea               | 

#### BuildingSpace=>
| Origin             | Harmonization           |
|--------------------|-------------------------|
 | value(Building)    | buildingSpaceName       | 
 | taxonomy(Tipus_us) | hasBuildingSpaceUseType | 

#### Area=>
| Origin                 | Harmonization                                                 |
|------------------------|---------------------------------------------------------------|
 | Sup_const_sota rasant  | areaValue {units:M2, hasAreaType: GrossFloorAreaUnderGround } | 
 | Sup_const_sobre_rasant | areaValue {units:M2, hasAreaType: GrossFloorAreaAboveGround } | 
 | Sup_const_total        | areaValue {units:M2, hasAreaType: GrossFloorArea }            | 

#### BuildingConstructionElement=>
| Origin                                   | Harmonization                       |
|------------------------------------------|-------------------------------------|
 | value(OtherBuildingConstructionElement ) | hasBuildingConstructionElementType  | 


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