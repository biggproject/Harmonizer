# CEEC3X description
CEEC3X is a format of the exportation for spanish energy performance certificates.

## Gathering tool
This data source comes in the format of a standard XML file containing the information of the building.
The format can be studied [here](sources/CEEC3X/3-DatosEnergeticosDelEdificioSchema21.xsd)

#### RUN import application
To run the import application, execute the python script with the following parameters:

```bash
python3 -m gather -so CEEC3X -f <gpg file> -n <namespace> -u <user_importing>  -b <buildingIDFromOrganization> -st <storage>
```
###

## Raw Data Format

The key of the file will be made generating a unique field based on the hash `md5` of the name of the file and the `buildingIDFromOrganization` of the building, concatenated by "~".

This ensures that if the same file is reloaded, the data can be overwritten.
The rest of the data will be stored to the column family `info` with column using the raw format name.

Also, the information will be split by the different elements in the XML file


## Harmonization

The harmonization of the file will be done with the following mapping, also it will be split depending on the elements on the XML

####  "DatosDelCertificador" ==> NOT MAPPED

-----
####  "IdentificacionEdificio" =>
   ###### Building =>
| Origin                      | Harmonization                 |
|-----------------------------|-------------------------------|
| NombreDelEdificio           | buildingName                  |
| AnoConstruccion             | buildingConstructionYear      |

   ###### LocationInfo =>
| Origin               | Harmonization         |
|----------------------|-----------------------|
| Direccion            | addressStreetName     |
 | CodigoPostal         | addressPostalCode     |
| fuzzy(Municipio)     | hasAddressCity        |
| fuzzy(Provincia)     | hasAddressProvince    |
 | fuzzy(ZonaClimatica) | hasAddressClimateZone |
 
###### CadastralInfo =>
| Origin              | Harmonization          |
|---------------------|------------------------|
 | ReferenciaCatastral | landCadastralReference |

###### BuildingSpace =>
| Origin                   | Harmonization           |
|--------------------------|-------------------------|
 | taxonomy(TipoDeEdificio) | hasBuildingSpaceUseType |

###### EnergyPerformanceCertificate =>
| Origin                | Harmonization                             |
|-----------------------|-------------------------------------------|
 | Procedimiento         | energyPerformanceCertificateProcedureType | 

###### EnergyPerformanceCertificateAdditionalInfo =>
| Origin                 | Harmonization      |
|------------------------|--------------------|
 | constructionRegulation | NormativaVigente   |




###### NO-> 
```
AlcanceInformacionXML
ComunidadAutonoma
```
       

------
#### DatosGeneralesyGeometria =>
###### BuildingSpace =>
| Origin | Harmonization |
|--------|---------------|
|        | only link     |

###### Area =>
| Origin                                                           | Harmonization                             |
|------------------------------------------------------------------|-------------------------------------------|
| SuperficieHabitable                                              | areaValue {hasAreaType: NetFloorArea }    |
| PorcentajeSuperficieHabitableCalefactada * SuperficieHabitable   | areaValue {hasAreaType: HeatedFloorArea}  |
| PorcentajeSuperficieHabitableRefrigerada" * "SuperficieHabitable | areaValue {hasAreaType: CooledFloorArea } |

###### NO-> 
```
        DensidadFuentesInternas
        VentilacionUsoResidencial
        VentilacionTotal
        DemandaDiariaACS
        NumeroDePlantasSobreRasante
        NumeroDePlantasBajoRasante
        VolumenEspacioHabitable
        Compacidad
        PorcentajeSuperficieAcristalada
        Imagen
        Plano
```
------
#### DatosEnvolventeTermica => NO

-----
#### InstalacionesTermicas => NO

-----
#### InstalacionesIluminacion => NO

-----
#### CondicionesFuncionamientoyOcupacion =>
###### BuildingSpace =>

| Origin  | Harmonization                   |
|---------|---------------------------------|
 | Nombre  | buildingSpaceName               |
 | Nombre  | buildingSpaceIDFromOrganization |

###### Area =>
| Origin      | Harmonization                          |
|-------------|----------------------------------------|
 | Superficie  | areaValue {hasAreaType: NetFloorArea } |

###### NO =>
``` 
    NivelDeAcondicionamiento
    PerfilDeUso
```
------
#### EnergiasRenovables => NO

-----
#### Demanda =>
###### EnergyPerformanceCertificate =>

| Origin        | Harmonization                   |
|---------------|---------------------------------|
 | Calefaccion   | annualHeatingEnergyDemand       |
 | Refrigeracion | annualCoolingEnergyDemand       |

###### NO =>
``` 
    Global
    ACS
```

------
#### Consumo => 
###### EnergyPerformanceCertificate =>

| Origin                            | Harmonization                    |
|-----------------------------------|----------------------------------|
 | EnergiaPrimariaNoRenovable>Global | annualPrimaryEnergyConsumption   |
 | suma(EnergiaFinalVectores>Global) | annualFinalEnergyConsumption     |

-----
#### EmisionesCO2 => NO
| Origin                                         | Harmonization              |
|------------------------------------------------|----------------------------|
 | suma(TotalConsumoElectrico, TotalConsumoOtros) | annualC02Emissions         |
 | Area * Iluminacion                             | annualLightingCO2Emissions |
 | Area * Calefaccion                             | annualHeatingCO2Emissions  |
 | Area * ACS                                     | annualHotWaterCO2Emissions |
 | Area * Refrigeracion                           | annualCoolingCO2Emissions  |

-----
#### Calificacion =>
###### EnergyPerformanceCertificate =>

| Origin                     | Harmonization             |
|----------------------------|---------------------------|
 | Demanda>Calefaccion        | heatingEnergyDemandClass  |
 | Demanda>Refrigeracion      | coolingEnergyDemandClass  |
 | EmisionesCO2>Iluminacion   | lightingCO2EmissionsClass |
 | EmisionesCO2>Calefaccion   | heatingCO2EmissionsClass  |
 | EmisionesCO2>ACS           | hotWaterCO2EmissionsClass |
 | EmisionesCO2>Refrigeracion | coolingCO2EmissionsClass  |
 | EmisionesCO2>Global        | C02EmissionsClass         |

###### NO =>
``` 
 
```
------
#### MedidasDeMejora => NO

-----



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

[//]: # ()
[//]: # ("DatosEnvolventeTermica")

[//]: # (    BuildingConstructionElement->)

[//]: # (        For element in group_label:)

[//]: # (            hasBuildingConstructionElementType -> taxonomy&#40;&#40;group_label, Tipo&#41; ,)

[//]: # (                {CerramientosOpacos, HuecosyLucernarios, PuentesTermicos},)

[//]: # (                {"Fachada", "Cubierta", "Suelo", "ParticionInteriorVertical","ParticionInteriorHorizontal","Adiabatico"}&#41;)

[//]: # (            buildingConstructionElementIDFromOrganization: "Nombre")

[//]: # (            NO->)

[//]: # (                "Superficie")

[//]: # (                "Orientacion")

[//]: # (                "Transmitancia")

[//]: # (                "FactorSolar")

[//]: # (                if group_label = HuecosyLurernarios:)

[//]: # (                    ModoDeObtencionTransmitancia)

[//]: # (                    ModoDeObtencionFactorSolar)

[//]: # (                else =)

[//]: # (                "ModoDeObtencion")

[//]: # (------)

[//]: # ("InstalacionesTermicas")

[//]: # (    BuildingSystemElement->)

[//]: # (        For element in group_label:)

[//]: # (            hasBuildingSystemElementType -> taxonomy&#40;&#40;group_label&#41; ,)

[//]: # (                {GeneradoresDeCalefaccion, GeneradoresDeRefrigeracion, InstalacionesACS, SistemasSecundariosCalefaccionRefrigeracion},)

[//]: # (            buildingSystemElementIDFromOrganization: "Nombre")

[//]: # (            NO->)

[//]: # (                "Tipo")

[//]: # (                "PotenciaNominal")

[//]: # (                "RendimientoNominal")

[//]: # (                "RendimientoEstacional")

[//]: # (                "VectorEnergetico")

[//]: # (                "ModoDeObtencion")

[//]: # (------)

[//]: # ("InstalacionesIluminacion")

[//]: # (    BuildingSpace->)

[//]: # (        For element in list:)

[//]: # (            buildingSpaceName : "Nombre")

[//]: # (            buildingSpaceIDFromOrganization: "Nombre")

[//]: # (            NO->)

[//]: # (                "PotenciaInstalada")

[//]: # (                "VEEI")

[//]: # (                "IluminanciaMedia")

[//]: # (                "ModoDeObtencion")

[//]: # (------)

[//]: # ()
[//]: # (------)

[//]: # (FALTA:)

[//]: # ()
[//]: # ("EnergiasRenovables")

[//]: # (------)

[//]: # ("Demanda")

[//]: # (------)

[//]: # ("Consumo")

[//]: # (------)

[//]: # ("EmisionesCO2")

[//]: # (------)

[//]: # ("Calificacion")

[//]: # (------)

[//]: # ("MedidasDeMejora")

[//]: # (------)

[//]: # ("PruebasComprobacionesInspecciones")
