from string import ascii_uppercase

import pandas as pd


def gather_contacts(wb):
    epc = wb['B2'].value
    years_of_validity = wb['B3'].value  # EnergyPerformanceContract.contractEndDate
    building_type = wb['C14'].value  # Building.buildingType
    energy_class_before_ee_measures = wb['C17'].value  # EnergyPerformanceCertificate.energyPerformanceClass
    energy_class_after_ee_measures = wb['D17'].value  # EnergyPerformanceCertificate.energyPerformanceClass
    energy_consumption_before_ee_measures = wb['C18'].value  # EnergyPerformanceCertificate.annualFinalEnergyConsumption
    energy_consumption_after_ee_measures = wb['D18'].value  # EnergyPerformanceCertificate.annualFinalEnergyConsumption
    organization_type = wb['C19'].value  # Organization.organizationType
    organization_contact_info = wb['C20'].value
    cadastral_ref = wb['C21'].value  # CadastralReference
    municipality = wb['C23'].value  # Location.municipality
    town = wb['C24'].value  # Location.city
    commissioning_date = wb['C25'].value
    floor_area = wb['C26'].value
    gross_floor_area = wb['C27'].value
    heating_area = wb['C28'].value
    heating_volume = wb['C29'].value
    cooling_area = wb['C30'].value
    cooling_volume = wb['C31'].value
    number_of_floors_before = wb['C32'].value
    number_of_floors_after = wb['D32'].value
    number_of_inhabitants = wb['C33'].value
    return {
        "epc_id": epc,
        "years_of_validity": years_of_validity,
        "energy_class_before": energy_class_before_ee_measures,
        "energy_class_after": energy_class_after_ee_measures,
        "specific_energy_consumption_before (kWh/m2)": energy_consumption_before_ee_measures,
        "specific_energy_consumption_after (kWh/m2)": energy_consumption_after_ee_measures,
        "building_type": building_type,
        "type": organization_type,
        "contact_info": organization_contact_info,
        "commissioning_date": commissioning_date,
        "municipality": municipality,
        "town": town,
        "gross_floor_area": gross_floor_area,
        "floor_area": floor_area,
        "heating_area": heating_area,
        "heating_volume": heating_volume,
        "cooling_area": cooling_area,
        "cooling_volume": cooling_volume,
        "number_of_floors_before": number_of_floors_before,
        "number_of_floors_after": number_of_floors_after,
        "number_of_inhabitants": number_of_inhabitants,
        "cadastral_reference": cadastral_ref
    }


def gather_building_description(wb):
    climate_zone = wb['B3'].value
    type_of_construction = wb['B8'].value

    return {"climate_zone": climate_zone, "type_of_construction": type_of_construction}


def excel_matrix(init_row, init_alphabet, end_alphabet, header_names, rows_names, wb, id):
    res = []
    for index2 in range(init_row, init_row + len(rows_names)):
        aux = {"type": rows_names[index2 - init_row], "id": id}
        for index, letter in enumerate(ascii_uppercase[init_alphabet:end_alphabet]):
            aux.update({header_names[index]: wb[f"{letter}{index2}"].value})
        res.append(aux)
    return res


def gather_consumption(path, sheet_name, wb):
    header_names = ["ConsumptionTm", "ConsumptionM3", "ConsumptionKWH", "CalorificValueKWH/volume", "BGN/volume", "BGN/kWh"]
    rows_names = ["Heavy fuel oil",
                  "Diesel oil",
                  "LPG",
                  "Diesel oil 2",
                  "Natural gas",
                  "Coal",
                  "Pellets",
                  "Wood",
                  "Other (specify)",
                  "Heat energy",
                  "Electricity"
                  ]
    #consumptions = excel_matrix(11, 2, 8, header_names, rows_names, wb, id)

    consumptions = pd.read_excel(path, sheet_name, names=header_names, skiprows=9, nrows=len(rows_names), usecols='C:H')
    consumptions.index = rows_names
    total_consumption = wb['E22'].value
    consumptions = pd.concat([consumptions, pd.DataFrame(index=["Total"], data=[{"ConsumptionKWH": total_consumption}])])

    header_names = ["Actual Specific", "Actual Total", "Corrected Specific", "Corrected Total", "Expected Specific",
                    'Expected Total']
    rows_names = ["Heating",
                  "Ventilation",
                  "DHW",
                  "Fans and pumps",
                  "Lighting",
                  "Appliances",
                  "Cooling",
                  "Total",
                  ]
    consumption_distribution = pd.read_excel(path, sheet_name, names=header_names, skiprows=27, nrows=len(rows_names), usecols='C:H')
    consumption_distribution.index = rows_names

    return {f"{k}_{k1}": v for k, inner in consumptions.to_dict(orient="dict").items() for k1, v in inner.items()},\
        {f"{k}_{k1}": v for k, inner in consumption_distribution.to_dict(orient="dict").items() for k1, v in inner.items()}


def gather_savings(path, sheet_name, wb):
    measurements = []
    measure_list = [
        ("1. Thermal insulation of external walls", 5),
        ("2. Thermal insulation of interior walls (walls between heated and non heated premises)",17),
        ("3. Thermal insulatio of roof", 29),
        ("4. Thermal insulatio of floor", 41),
        ("5. Replacemenent of windows and doors", 53),
        ("6. Energy saving measures in heat generation. Heating and ventilation", 69),
        ("7. Energy saving measures in the generation of cold. Cooling.", 84),
        ("8. Energy saving measures for replacement of pumps, fans and other elements in the generation of heat and / or cold", 96),
        ("9. Energy saving measures to improve the energy performance of a distribution network for the transport of hot water and / or air network", 108),
        ("10. Measures on measurement systems, systems for automation, parameter control and monitoring of heat and cold supply, which aim at energy saving", 120),
        ("11. Energy saving measures in the DHW system", 135),
        ("12. Energy saving measures for utilization of energy from renewable sources", 147),
        ("13. Energy saving measures for lighting systems", 159),
        ("14. Energy saving measures for replacement of household appliances and / or office equipment", 171),
        ("Total From The Project", 188)
    ]

    header_names = ["SavingTm", "SavingM3", "SavingKWH", "SavingBGN", "InvestmentBGN", "PaybackYears", "SavedCO2Tm"]
    rows_names = ["Heavy fuel oil",
                  "Diesel oil",
                  "LPG",
                  "Diesel oil 2",
                  "Natural gas",
                  "Coal",
                  "Pellets",
                  "Wood",
                  "Other (specify)",
                  "Heat energy",
                  "Electricity",
                  "Total"
                  ]

    for meas, index in measure_list:
        temp_mes = pd.read_excel(path, sheet_name, names=header_names, skiprows=index,
                                                 nrows=len(rows_names), usecols='E:K')
        temp_mes.index = rows_names
        temp_mes.columns = [f"{meas}_{c}" for c in temp_mes.columns]
        measurements.append(temp_mes)
    measurements = pd.concat(measurements, axis=1)

    energy_saved = {"total_energy_saved": wb['G204'].value, "shared_energy_saved": wb['G206'].value}
    return energy_saved, {f"{k}_{k1}": v for k, inner in measurements.to_dict(orient="dict").items() for k1, v in inner.items()}


def transform_data(data):
    general_info = data['general_info']
    consumption = data['consumption']
    distribution = data['distribution']
    measurements = data['measurements']
    total_annual_savings = data['total_annual_savings']

    row = {}
    row.update(**general_info)
    row.update({"epc_id": data['epc_id']})

    for i in range(len(consumption)):
        if consumption[i]["kWh"] is not None:
            row.update({f"consumption_{i}": consumption[i]["kWh"],
                        f"consumption_{i}_type": consumption[i]["type"]
                        })

    for i in range(len(distribution)):
        row.update(
            {f"distribution_{i}": distribution[i]['Actual Total'], f"distribution_{i}_type": distribution[i]['type']})

    for i in range(len(measurements)):
        val = measurements[i]['id'].split('~')[1]
        row.update({f"measure_{int(val) - 1}_{i % 12}": measurements[i]['kWh/a.'],
                    f"measure_{int(val) - 1}_{i % 12}_type": measurements[i]['type']})

    for i in range(len(total_annual_savings)):
        if total_annual_savings[i]['kWh/a.'] is not None:
            row.update({f"total_annual_savings_{i}": total_annual_savings[i]['kWh/a.'],
                        f"total_annual_savings_{i}_type": total_annual_savings[i]['type']})

    return [row]
