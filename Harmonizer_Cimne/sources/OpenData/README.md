# Open Data - CEEE Dataset

CEEE (Certificats d’eficiència energètica d’edificis) is dataset of the energy certificates that are compulsory for the
existing buildings or dwellings in Catalonia.

## Gathering tool

This data source is obtained from the API, where we can obtain all the certificates.

#### RUN import application

To run the import application, execute the python script with the following parameters:

```bash
python3 -m gather -so OpenData -n <namespace> -st <storage> -u <user_importing>
```

## Harmonization

The harmonization of the response will be done with the following mapping.

### LocationInfo

| Origin                | Harmonization                     |
|-----------------------|-----------------------------------|
| adre_a         | addressStreetName    | 
| numero | addressStreetNumber | 
| codi_postal | addressPostalCode | 
| longitud | addressLongitude | 
| latitud | addressLatitude | 
| nom_provincia | hasAddressProvince | 
| poblacio | hasAddressCity | 

### CadastralInfo

| Origin                | Harmonization                     |
|-----------------------|-----------------------------------|
| referencia_cadastral         | landCadastralReference    | 
| metres_cadastre | landArea | 

### Building

| Origin                | Harmonization                     |
|-----------------------|-----------------------------------|
| any_construccio         | buildingConstructionYear    | 

### EnergyPerformanceCertificate

| Origin                | Harmonization                     |
|-----------------------|-----------------------------------|
| num_cas         | energyPerformanceCertificateReferenceNumber    | 
| qualificaci_de_consum_d         | energyPerformanceCertificateClass    | 
| qualificacio_d_emissions         | C02EmissionsClass    | 
| emissions_de_co2         | annualC02Emissions    | 
| cost_anual_aproximat_d_energia         | annualEnergyCost    | 
| consum_d_energia_final         | annualFinalEnergyConsumption    | 
| eina_de_certificacio         | energyPerformanceCertificateCertificationTool    | 
| emissions_refrigeraci         | annualCoolingCO2Emissions    | 
| qualificaci_emissions_1         | coolingCO2EmissionsClass    | 
| emissions_calefacci         | annualHeatingCO2Emissions    | 
| qualificaci_emissions         | heatingCO2EmissionsClass    | 
| emissions_acs         | annualHotWaterCO2Emissions    | 
| qualificaci_emissions_acs         | hotWaterCO2EmissionsClass    | 
| emissions_enllumenament         | annualLightingCO2Emissions    | 
| qualificaci_emissions_2         | lightingCO2EmissionsClass    | 
| qualificaci_energia_acs         | hotWaterPrimaryEnergyClass    | 
| qualificaci_energia_1         | lightingPrimaryEnergyClass    | 
| qualificaci_energia_calefacci_1         | heatingEnergyDemandClass    | 
| motiu_de_la_certificacio         | energyPerformanceCertificateCertificationMotivation    |
| motiu_de_la_certificacio         | energyPerformanceCertificateCertificationMotivation    |

### EnergyPerformanceCertificateAdditionalInfo

| Origin                | Harmonization                     |
|-----------------------|-----------------------------------|
| vehicle_electric         | electricVehicleChargerPresence    | 
| solar_termica         | solarThermalSystemPresence    | 
| solar_fotovoltaica         | solarPVSystemPresence    | 
| sistema_biomassa         | biomassSystemPresence    | 
| xarxa_districte         | districtHeatingOrCoolingConnection    | 
| energia_geotermica         | geothermalSystemPresence    | 
| valor_finestres         | averageWindowsTransmittance    | 
| valor_aillaments         | averageFacadeTransmittance    | 