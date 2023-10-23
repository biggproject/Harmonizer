import warnings
import hashlib
import itertools
import time
from datetime import timedelta

import numpy as np
import numpy_financial as npf
import pandas as pd
import pymongo
import rdflib
import utils.mongo
from neo4j import GraphDatabase
from rdflib import Namespace
from slugify import slugify
from thefuzz import process

import settings
from utils.cache import Cache
from sources.Bulgaria.harmonizer.Mapper import Mapper
from utils.data_transformations import *
from utils.hbase import save_to_hbase
from utils.neo4j import create_sensor, create_KPI
from ontology.namespaces_definition import bigg_enums, units, Bigg
from utils.rdf.rdf_functions import generate_rdf
from utils.rdf.save_rdf import save_rdf_with_source, link_devices_with_source

warnings.simplefilter("ignore")
consumption_sources_mapping = {"Gas": "EnergyConsumptionGas",
                               "Hard fuels": "EnergyConsumptionHeavyFuel",
                               "Heat energy": "EnergyConsumptionDistrictHeating",
                               "Liquid fuels": "EnergyConsumptionDiesel",
                               "Others": "EnergyConsumptionOther",
                               "Electricity": "EnergyConsumptionGridElectricity",
                                "Total": "EnergyConsumptionTotal"
                               }

resource_type_map = {"EnergyConsumptionGas": ("GasUtility", "Meter.EnergyMeter.Gas"),
                    "EnergyConsumptionHeavyFuel": ("HeavyFuelUtility", "Meter.EnergyMeter.HeavyFuel"),
                    "EnergyConsumptionDistrictHeating": ("DistrictHeatingUtility", "Meter.EnergyMeter.Heat"),
                    "EnergyConsumptionDiesel": ("DieselUtility", "Meter.EnergyMeter.Diesel"),
                    "EnergyConsumptionOther": ("OtherUtility", "Meter.EnergyMeter.Other"),
                    "EnergyConsumptionGridElectricity": ("ElectricUtility", "Meter.EnergyMeter.Electricity"),
                    "EnergyConsumptionTotal": ("TotalUtility", "Meter.EnergyMeter.Total")}

device_subject_reg = r"device_subject_.*"
utility_subject_reg = r"utility_subject_.*"

def set_taxonomy(df):
    df['Type of building'] = df['Type of building'].str.strip()
    building_type_taxonomy = get_taxonomy_mapping(
        taxonomy_file="sources/Bulgaria/harmonizer/BuildingSpaceUseTypeTaxonomy.xlsx",
        default="Other")
    df['buildingSpaceUseType'] = df['Type of building'].map(building_type_taxonomy).apply(partial(to_object_property,
                                                                                              namespace=bigg_enums))


def set_municipality(df):
    municipality_dic = Cache.municipality_dic_BG
    municipality_fuzz = partial(fuzzy_dictionary_match,
                            map_dict=fuzz_params(
                                municipality_dic,
                                ['ns1:alternateName', 'ns1:officialName']
                            ),
                            default=None
                            )
    unique_municipality = df['Municipality'].unique()
    municipality_map = {k: municipality_fuzz(k) for k in unique_municipality}
    df.loc[:, 'hasAddressCity'] = df['Municipality'].map(municipality_map)


def clean_dataframe_building_info(df_orig, source):
    timezone = "Europe/Sofia"
    df = df_orig.copy(deep=True)
    df['subject'] = df['filename'] + '~' + df['id'].astype(str)
    df['location_org_subject'] = df['Municipality'].apply(slugify).apply(building_department_subject)
    df['organization_subject'] = df['subject'].apply(building_department_subject)
    df['building_subject'] = df['subject'].apply(building_subject)
    df['building_name'] = df.apply(lambda x: f"{x.Municipality}:{x.name}~{x['Type of building']}", axis=1)
    df['building_id'] = df['filename'].str.slice(-5) + '~' + df['id'].astype(str)
    df['location_subject'] = df['subject'].apply(location_info_subject)
    df['timezone'] = timezone
    df['epc_date'] = pd.to_datetime(df['EPC_Date_Date']).dt.tz_localize(timezone).dt.tz_convert("UTC")
    df['epc_date_before'] = df['epc_date'] - pd.DateOffset(years=1)
    df['epc_date'] = df['epc_date'].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df['epc_date_before'] = df['epc_date_before'].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df['epc_before_subject'] = df['subject'].apply(lambda x: x + '~before').apply(epc_subject)
    df['epc_after_subject'] = df['subject'].apply(lambda x: x + '~after').apply(epc_subject)
    df["EPC_Energy class_Before"] = df["EPC_Energy class_Before"].str.strip()
    df["EPC_Energy class_Before"] = df["EPC_Energy class_After"].str.strip()
    df['building_space_subject'] = df['subject'].apply(building_space_subject)
    df['gross_floor_area_subject'] = df['subject'].apply(partial(gross_area_subject, a_source=source))
    df['element_subject'] = df['subject'].apply(construction_element_subject)
    for et in consumption_sources_mapping.values():
        df[f'device_subject_{et}'] = (df['subject'] + f"_{et}").apply(partial(device_subject, source=source))
        df[f'utility_subject_{et}'] = (df['subject'] + f"_{et}").apply(delivery_subject)
    df['project_subject'] = df['subject'].apply(project_subject)
    return df


def clean_dataframe_eem(df_orig, source):
    eem_columns = [r".*Savings_Liquid fuels.*", r".*Savings_Hard fuels.*", r".*Savings_Gas.*", r".*Savings_Others.*",
                   r".*Savings_Heat energy.*", r".*Savings_Electricity.*", r".*Savings_Total.*",
                   r".*Savings_Emission reduction.*", r".*Savings_Finacial savings.*", r".*Investments.*", r".*Payback.*"]
    df = df_orig.copy(deep=True)

    # melt df to get 1 row for measure
    df_eem_melt = df.melt(id_vars=["subject"], value_vars=[x for x in df.columns if any([re.match(c, x) for c in eem_columns])])
    df_eem_melt["measure_type"] = df_eem_melt.variable.apply(lambda x: x.split("_")[0])
    eem_type_taxonomy = get_taxonomy_mapping(
        taxonomy_file="sources/Bulgaria/harmonizer/EnergyEfficiencyMeasureTypeTaxonomy.xlsx",
        default=None)
    df_eem_melt["measure_type"] = df_eem_melt.measure_type.map(eem_type_taxonomy)
    df_eem_melt.dropna(inplace=True)
    df_eem_melt["measure_type_id"] = df_eem_melt["measure_type"]
    df_eem_melt["measure_type"] = df_eem_melt["measure_type"].apply(partial(to_object_property, namespace=bigg_enums))

    # split variable information
    df_eem_melt["unit"] = df_eem_melt.variable.apply(lambda x: x.split("_")[-1])
    df_eem_melt["field"] = df_eem_melt.variable.apply(lambda x: "_".join(x.split("_")[1:-1]))
    # create the subject and filter measures
    df_eem_melt['eem_subject'] = df_eem_melt.apply(lambda x: f"{x['subject']}-{x['measure_type_id']}", axis=1).apply(
        eem_subject)
    df_eem_subjects = df_eem_melt.loc[(df_eem_melt.field == "Investments") & (df_eem_melt.value.astype("float") > 0)].eem_subject
    df_eem_melt = df_eem_melt.loc[np.isin(df_eem_melt.eem_subject, df_eem_subjects)]
    # set the variables as columns
    df_eem = df_eem_melt[["eem_subject", "field", "value"]].pivot(index='eem_subject', columns='field')
    df_eem.reset_index(inplace=True)
    # add extra fields
    df_eem["subject"] = df_eem['eem_subject'].map(df_eem_melt[['eem_subject', 'subject']].drop_duplicates(subset=["eem_subject"]).set_index("eem_subject").subject)
    df_eem['measure_type'] = df_eem['eem_subject'].map(df_eem_melt[['eem_subject', 'measure_type']].drop_duplicates(subset=["eem_subject"]).set_index("eem_subject")['measure_type'])

    df_eem["epc_date"] = df_eem['subject'].map(df[['subject', 'epc_date']].set_index("subject").epc_date)
    df_eem["element_subject"] = df_eem['subject'].map(df[['subject', 'element_subject']].set_index("subject").element_subject)
    df_eem['GFA, m2'] = df_eem['subject'].map(df[['subject', 'GFA, m2']].set_index("subject")['GFA, m2'])
    df_eem['project_subject'] = df_eem['subject'].map(df[['subject', 'project_subject']].set_index("subject")['project_subject'])
    df_eem.columns = [x[0] if x[0] != "value" else x[1] for x in df_eem.columns]
    df_kpi = bulgaria_eem_calculator(df_eem)
    return df_eem, df_kpi


def set_source_id(df, user, connection):
    neo = GraphDatabase.driver(**connection)
    with neo.session() as session:
        source_id = session.run(f"""Match(bigg__Organization{{userID:"{user}"}})-[:hasSource]->(s) return id(s) as id""").data()[0]['id']
    df['source_id'] = source_id


def clean_energies_consumption(df_orig):
    before_columns_reg = r"Annual energy consumption - before_.*"
    after_columns_reg = r"Total from the project_Savings_.*"
    device_subject_reg = r"device_subject_.*"
    utility_subject_reg = r"utility_subject_.*"
    df = df_orig.copy(deep=True)

    before_columns = [x for x in df.columns if re.match(before_columns_reg, x)]
    before_columns = {c: c.split("_")[1] for c in before_columns}
    before_columns.update({'epc_date_before': 'start', 'epc_date': 'end',
                           'building_space_subject': 'building_space_subject', 'source_id': 'source_id',
                           'building_subject': 'building_subject', 'GFA, m2': 'GFA, m2'})
    before_columns.update({c: c for c in df.columns if re.match(device_subject_reg, c)})
    before_columns.update({c: c for c in df.columns if re.match(utility_subject_reg, c)})
    df_consumption_before = df[list(before_columns.keys())]

    after_columns = [x for x in df.columns if re.match(after_columns_reg, x)]
    after_columns = {c: c.split("_")[2] for c in after_columns}
    after_columns.update({'epc_date': 'start', 'building_space_subject': 'building_space_subject',
                          'building_subject': 'building_subject', 'GFA, m2': 'GFA, m2', 'source_id': 'source_id'})
    after_columns.update({c: c for c in df.columns if re.match(device_subject_reg, c)})
    after_columns.update({c: c for c in df.columns if re.match(utility_subject_reg, c)})
    df_consumption_after = df[list(after_columns.keys())]

    df_consumption_before = df_consumption_before.rename(before_columns, axis=1)
    df_consumption_after = df_consumption_after.rename(after_columns, axis=1)

    df_consumption_before['start'] = pd.to_datetime(df_consumption_before['start'])
    df_consumption_after['start'] = pd.to_datetime(df_consumption_after['start'])
    df_consumption_before['end'] = pd.to_datetime(df_consumption_before['end'])
    df_consumption_before['end'] = df_consumption_before['end'] - pd.DateOffset(seconds=1)
    df_consumption_after['end'] = df_consumption_after['start'] + pd.DateOffset(years=1, seconds=-1)

    existing_columns = {k: v for k, v in consumption_sources_mapping.items() if k in df_consumption_before.columns}
    existing_devices = []
    existing_utilities = []
    for con_col in existing_columns.keys():
        df_consumption_before[con_col] = df_consumption_before[con_col].astype(float)
        df_consumption_after[con_col] = df_consumption_after[con_col].astype(float)
        df_consumption_before[con_col] = df_consumption_before[con_col].fillna(0)
        df_consumption_after[con_col] = df_consumption_after[con_col].fillna(0)
        df_consumption_after[con_col] = df_consumption_before[con_col] - df_consumption_after[con_col]
        df_consumption_after[con_col] = df_consumption_after[con_col].apply(lambda x: x if x > 0 else 0)
        set_na = (df_consumption_after[con_col] == 0) * (df_consumption_before[con_col] == 0)
        df_consumption_before.loc[set_na, con_col] = np.NaN
        df_consumption_after.loc[set_na, con_col] = np.NaN
        existing_devices.append(f"device_subject_{existing_columns[con_col]}")
        existing_utilities.append(f"utility_subject_{existing_columns[con_col]}")
    df_consumption_before = df_consumption_before[
        existing_devices + existing_utilities + ["building_subject", "source_id", 'building_space_subject', "start", "end", 'GFA, m2'] +
        list(existing_columns.keys())]
    df_consumption_after = df_consumption_after[
        existing_devices + existing_utilities + ["building_subject", "source_id", 'building_space_subject', "start", "end", 'GFA, m2'] +
        list(existing_columns.keys())]
    df_consumption = pd.concat([df_consumption_before, df_consumption_after])
    df_consumption = df_consumption.rename(existing_columns, axis=1)
    df_kpi_use_intensity = df_consumption[["building_subject", "start", "end", 'GFA, m2']]
    for con_col in existing_columns.values():
        df_kpi_use_intensity[con_col] = df_consumption[con_col]/df_kpi_use_intensity['GFA, m2'].astype(float)

    df['consumption_total_after'] = df_consumption_after[list(existing_columns.keys())].sum(axis=1)

    return df, \
        df_consumption[existing_devices + existing_utilities + ["building_subject", "source_id", 'building_space_subject', "start", "end"] +
                        list(existing_columns.values())], \
        df_kpi_use_intensity[["building_subject", "start", "end"] + list(existing_columns.values())]


def bulgaria_eem_calculator(df):
    df = df.copy(deep=True)
    lifespan_by_eem = defaultdict(lambda: 10)
    lifespan_by_eem.update(
        {
            "BuildingFabricMeasure": 25,
            "BuildingFabricMeasure.FloorMeasure": 25,
            "BuildingFabricMeasure.RoofAndCeilingMeasure": 25,
            "BuildingFabricMeasure.WallMeasure.WallCavityInsulation": 25,
            "LightingMeasure": 10,
            "RenewableGenerationMeasure": 25,
            "HVACAndHotWaterMeasure.CombinedHeatingCoolingSystemMeasure.HeatingAndCoolingDistributionMeasure.HeatingAndCoolingDistributionSystemReplacement": 15,
            "HVACAndHotWaterMeasure": 15
        }
    )
    columns_mapping = {
        "eem_subject": "eem_subject",
        "eem_type": "eem_type",
        "Investments": "eem_investment",
        "Savings_Electricity": "EnergyUseSavings~EnergyConsumptionGridElectricity~KiloW-HR",
        "Savings_Emission reduction": "EnergyEmissionsSavings~EnergyConsumptionTotal~KiloGM-CO2",
        "Savings_Finacial savings": "EnergyCostSavings~EnergyConsumptionTotal~BulgarianLev",
        "Savings_Gas": "EnergyUseSavings~EnergyConsumptionGas~KiloW-HR",
        "Savings_Hard fuels": "EnergyUseSavings~EnergyConsumptionHeavyFuel~KiloW-HR",
        "Savings_Heat energy": "EnergyUseSavings~EnergyConsumptionDistrictHeating~KiloW-HR",
        "Savings_Liquid fuels": "EnergyUseSavings~EnergyConsumptionDiesel~KiloW-HR",
        "Savings_Others": "EnergyUseSavings~EnergyConsumptionOthers~KiloW-HR",
        "Savings_Total": "EnergyUseSavings~EnergyConsumptionTotal~KiloW-HR",
        "subject": "building_id",
        "epc_date": "start",
        "GFA, m2": "building_area"
    }
    kpis = ["EnergyUseSavings", "EnergyEmissionsSavings", "EnergyCostSavings"]
    discount_rate = 0.05

    # Get EEM types and change column names
    df["eem_type"] = df.measure_type.str.split("#", expand=True)[[1]]
    df = df.drop(columns=df.columns.difference(list(columns_mapping.keys())))
    df.columns = list(pd.Series(df.columns).replace(columns_mapping, regex=False))

    # Add the lifespan and discount_rate
    df["lifespan"] = df.eem_type.map(lifespan_by_eem)
    df["discount_rate"] = discount_rate

    # Cast to correct class
    df["eem_investment"] = pd.to_numeric(df["eem_investment"], errors='coerce')
    df['building_area'] = pd.to_numeric(df["building_area"], errors='coerce')
    df['start'] = pd.to_datetime(df["start"], errors='coerce')
    kpi_columns = list(filter(lambda i: i.startswith(tuple(kpis)), list(df.columns)))
    for kpi_item in kpi_columns:
        df[kpi_item] = pd.to_numeric(df[kpi_item], errors='coerce')

    # Generate all the area-normalised KPIs
    suffix_kpi = "Intensity"
    suffix_unit_kpi = "-M2"
    for kpi_item in kpi_columns:
        kpi_items = kpi_item.split("~")
        kpi_name = f"{kpi_items[0]}{suffix_kpi}"
        kpi_unit = f"{kpi_items[2]}{suffix_unit_kpi}"
        if kpi_name not in kpis:
            kpis.append(kpi_name)
        df[f"{kpi_name}~{kpi_items[1]}~{kpi_unit}"] = \
            df[kpi_item]/pd.to_numeric(df["building_area"], errors='coerce')

    df["EnergyEmissionsSavings~EnergyConsumptionTotal~KiloGM-CO2"] = df['EnergyEmissionsSavings~EnergyConsumptionTotal~KiloGM-CO2'] * 1000
    df["EnergyEmissionsSavingsIntensity~EnergyConsumptionTotal~KiloGM-CO2-M2"] = df['EnergyEmissionsSavingsIntensity~EnergyConsumptionTotal~KiloGM-CO2-M2'] * 1000

    df["NormalisedInvestmentCost~~BulgarianLev-M2"] = df["eem_investment"]/df["building_area"]
    kpis.append("NormalisedInvestmentCost")

    df["AvoidanceCost~~BulgarianLev-KiloW-HR"] = \
        df["eem_investment"] / (df["EnergyUseSavings~EnergyConsumptionTotal~KiloW-HR"] * df["lifespan"])
    kpis.append("AvoidanceCost")

    df["SimplePayback~~YR"] = \
        df["eem_investment"] / (df["EnergyCostSavings~EnergyConsumptionTotal~BulgarianLev"])
    kpis.append("SimplePayback")

    df["NetPresentValue~~BulgarianLev"] = df.apply(
        lambda x:
            npf.npv(x['discount_rate'],
                    [-x['eem_investment']] + [x['EnergyCostSavings~EnergyConsumptionTotal~BulgarianLev']] * int(x['lifespan'])),
        axis=1
    )
    kpis.append("NetPresentValue")

    df["ProfitabilityIndex~~UNITLESS"] = (df["NetPresentValue~~BulgarianLev"] + df["eem_investment"]) / df["eem_investment"]
    kpis.append("ProfitabilityIndex")

    df["NetPresentValueQuotient~~UNITLESS"] = df["NetPresentValue~~BulgarianLev"] / df["eem_investment"]
    kpis.append("NetPresentValueQuotient")

    df["InternalRateOfReturn~~PERCENT"] = df.apply(
        lambda x:
            npf.irr([-x['eem_investment']] + [x['EnergyCostSavings~EnergyConsumptionTotal~BulgarianLev']] * int(x['lifespan'])),
        axis=1
    )
    df["InternalRateOfReturn~~PERCENT"] = df["InternalRateOfReturn~~PERCENT"].apply(lambda x: x*100)
    kpis.append("InternalRateOfReturn")

    # Melt KPIs to longitudinal format
    kpi_columns = list(filter(lambda i: i.startswith(tuple(kpis)), list(df.columns)))
    #df = df[df[kpi_columns].notnull().all(1)] # podem tenir algun kpi a null i no el pujem, però no cal eliminar-ho tot
    df = df[np.negative((df[kpi_columns] == 0).all(1))]

    # Melt KPIs to longitudinal format
    non_kpi_columns = list(filter(lambda i: np.logical_not(i.startswith(tuple(kpis))), list(df.columns)))
    df = pd.melt(df, non_kpi_columns)
    kpi_info = df.variable.str.split("~", expand=True)
    kpi_info.columns = ["KPI", "measured_property", "unit"]
    df = pd.concat([df, kpi_info], axis=1)

    # Delete useless columns and generate final features
    df = df.drop(columns=["variable", "lifespan", "discount_rate"])
    df["end"] = df["start"] + pd.offsets.DateOffset(years=1, seconds=-1)
    return df


def clean_factor_kpi(df_orig):
    df = df_orig.copy(deep=True)
    df['emission_factor'] = df["Total from the project_Savings_Emission reduction_tCO2/a"].astype(float) * 1000 / df["Total from the project_Savings_Total_kWh/a"].astype(float)
    df['cost_factor'] = df["Total from the project_Savings_Finacial savings_BGN/a"].astype(float) / df["Total from the project_Savings_Total_kWh/a"].astype(float)
    df['emission_before'] = df['emission_factor'] * df['Annual energy consumption - before_Total energy consumpion (by invoices)_kWh/a'].astype(float)
    df['cost_before'] = df['cost_factor'] * df['Annual energy consumption - before_Total energy consumpion (by invoices)_kWh/a'].astype(float)
    df["emission_after"] = df['emission_before'] - df["Total from the project_Savings_Emission reduction_tCO2/a"].astype(float)
    df["cost_after"] = df['cost_before'] - df["Total from the project_Savings_Finacial savings_BGN/a"].astype(float)
    df["emission_after"] = df["emission_after"].apply(lambda x: x if x > 0 else 0)
    df["cost_after"] = df["cost_after"].apply(lambda x: x if x > 0 else 0)

    before_columns = {'epc_date_before': 'start', 'epc_date': 'end', #'device_subject': 'device_subject',
                      'building_subject': 'building_subject', 'GFA, m2': 'GFA, m2',
                      'emission_before': 'EnergyEmission', 'cost_before': 'EnergyCost'}
    df_consumption_before = df[list(before_columns.keys())]

    after_columns = {'epc_date': 'start', #'device_subject': 'device_subject',
                     'building_subject': 'building_subject', 'GFA, m2': 'GFA, m2',
                     'emission_after': 'EnergyEmission', 'cost_after': 'EnergyCost'}
    df_consumption_after = df[list(after_columns.keys())]

    df_consumption_before = df_consumption_before.rename(before_columns, axis=1)
    df_consumption_after = df_consumption_after.rename(after_columns, axis=1)

    df_consumption_before['start'] = pd.to_datetime(df_consumption_before['start'])
    df_consumption_after['start'] = pd.to_datetime(df_consumption_after['start'])
    df_consumption_before['end'] = pd.to_datetime(df_consumption_before['end'])
    df_consumption_before['end'] = df_consumption_before['end'] - pd.DateOffset(seconds=1)
    df_consumption_after['end'] = df_consumption_after['start'] + pd.DateOffset(years=1, seconds=-1)
    df_consumption = pd.concat([df_consumption_before, df_consumption_after])
    df_consumption["EnergyEmissionIntensity"] = df_consumption["EnergyEmission"]/df_consumption['GFA, m2'].astype(float)
    df_consumption["EnergyCostIntensity"] = df_consumption["EnergyCost"]/df_consumption['GFA, m2'].astype(float)

    return df_consumption[['start', 'end', 'building_subject', "EnergyCost"]].rename({"EnergyCost": "EnergyConsumptionTotal"}, axis=1), \
        df_consumption[['start', 'end', 'building_subject', "EnergyEmission"]].rename({"EnergyEmission": "EnergyConsumptionTotal"}, axis=1),\
        df_consumption[['start', 'end', 'building_subject', "EnergyCostIntensity"]].rename({"EnergyCostIntensity": "EnergyConsumptionTotal"}, axis=1), \
        df_consumption[['start', 'end', 'building_subject', "EnergyEmissionIntensity"]].rename({"EnergyEmissionIntensity": "EnergyConsumptionTotal"}, axis=1)


def prepare_all(df, user, config):

    set_source_id(df, user, config['neo4j'])
    set_taxonomy(df)
    set_municipality(df)
    df_building = clean_dataframe_building_info(df, config['source'])
    df_building, df_consumption, df_kpi_use_intensity = clean_energies_consumption(df_building)
    df_measures, df_measures_kpi = clean_dataframe_eem(df_building, config['source'])
    df_cost, df_emissions, df_cost_intensity, df_emissions_intensity = clean_factor_kpi(df_building)
    return df_building, df_measures, df_measures_kpi, df_consumption, df_kpi_use_intensity, df_cost, df_emissions, \
        df_cost_intensity, df_emissions_intensity

def convert(tz):
    if not tz.tz:
        tz = tz.tz_localize("UTC")
    if "UTC" != tz.tz.tzname(tz):
        tz = tz.tz_convert("UTC")
    return tz

def create_sensor_bulk(df, property_uri, estimation_method_uri, unit_uri, r, c, o, freq, agg_func):
    df['end'] = df.end.dt.strftime(date_format="%y-%m-%dT%H:%M:%S+00:00")
    df['start'] = df.start.dt.strftime(date_format="%y-%m-%dT%H:%M:%S+00:00")
    df['serialize'] = df.apply(lambda x: ", ".join([f"{k}:'{v}'" for k, v in x.to_dict().items()]), axis=1)
    data_str = "["+",".join(["{"+l+"}" for l in df['serialize'].to_list()])+"]"
    query = f"""
    MATCH (mp: bigg__MeasuredProperty {{uri:"{property_uri}"}})
    MATCH (se: bigg__EstimationMethod {{uri:"{estimation_method_uri}"}})
    MATCH (msu: bigg__MeasurementUnit {{uri:"{unit_uri}"}})
    UNWIND {data_str} as data
    MATCH (device {{uri: data.link_uri}})
    MERGE (s: bigg__Sensor:bigg__TimeSeriesList:Resource {{uri:data.ts_uri}})   
    MERGE (ms: bigg__Measurement:Resource:bigg__TimeSeriesPoint{{uri: data.measurement_uri}})

    MERGE(s)-[:bigg__hasMeasuredProperty]->(mp)
    MERGE(s)-[:bigg__hasEstimationMethod]->(se)  
    MERGE(s)<-[:bigg__hasSensor]-(device)  
    MERGE(s)-[:bigg__hasMeasurementUnit]->(msu)
    MERGE(s)-[:bigg__hasMeasurement]->(ms)    
    SET
       s.bigg__timeSeriesIsCumulative= {c},
       s.bigg__timeSeriesIsRegular= {r},
       s.bigg__timeSeriesIsOnChange= {o},
       s.bigg__timeSeriesFrequency= "{freq}",
       s.bigg__timeSeriesTimeAggregationFunction= "{agg_func}",
       s.bigg__timeSeriesStart = CASE 
           WHEN s.bigg__timeSeriesStart < 
            datetime(data.start) 
               THEN s.bigg__timeSeriesStart
               ELSE datetime(data.start) 
           END,
       s.bigg__timeSeriesEnd = CASE 
           WHEN s.bigg__timeSeriesEnd >
            datetime(data.end) 
               THEN s.bigg__timeSeriesEnd
               ELSE datetime(data.end)
           END  
   return s
    """
    return query

def create_kpi_bulk(df, property_uri, estimation_method_uri,kpi_uri, kpi_component_uri, unit_uri, r, c, o, freq, agg_func):
    df['end'] = df.end.dt.strftime(date_format="%y-%m-%dT%H:%M:%S+00:00")
    df['start'] = df.start.dt.strftime(date_format="%y-%m-%dT%H:%M:%S+00:00")
    df['serialize'] = df.apply(lambda x: ", ".join([f"{k}:'{v}'" for k, v in x.to_dict().items()]), axis=1)
    data_str = "["+",".join(["{"+l+"}" for l in df['serialize'].to_list()])+"]"
    query = f"""
    MATCH (mp: bigg__MeasuredProperty {{uri:"{property_uri}"}})
    MATCH (se: bigg__EstimationMethod {{uri:"{estimation_method_uri}"}})
    MATCH (msu: bigg__MeasurementUnit {{uri:"{unit_uri}"}})
    MATCH (kpi:bigg__KeyPerformanceIndicator {{uri:"{kpi_uri}"}})   
    MATCH (comp {{uri:"{kpi_component_uri}"}})   

    UNWIND {data_str} as data
    MATCH (calculation_item {{uri: data.link_uri}})

    MERGE (s:bigg__SingleKPIAssessment:bigg__KPIAssessment:bigg__TimeSeriesList:Resource {{uri:data.ts_uri}})   
    MERGE (ms: bigg__SingleKPIAssessmentPoint:Resource:bigg__TimeSeriesPoint{{uri: data.measurement_uri}})
    
    MERGE(s)-[:bigg__hasMeasuredProperty]->(mp)
    MERGE(s)-[:bigg__hasEstimationMethod]->(se)  
    MERGE (s)<-[:bigg__assessesSingleKPI]-(calculation_item)
    MERGE (s)<-[:bigg__hasMeasuredPropertyComponent]->(comp)
    Merge(s)-[:bigg__hasKPIUnit]->(msu)
    Merge(s)-[:bigg__hasSingleKPIPoint]->(ms)
    Merge(s)-[:bigg__quantifiesKPI]->(kpi)
    SET
       s.bigg__timeSeriesIsCumulative= {c},
       s.bigg__timeSeriesIsRegular= {r},
       s.bigg__timeSeriesIsOnChange= {o},
       s.bigg__timeSeriesFrequency= "{freq}",
       s.bigg__timeSeriesTimeAggregationFunction= "{agg_func}",
       s.bigg__timeSeriesStart = CASE 
           WHEN s.bigg__timeSeriesStart < 
            datetime(data.start) 
               THEN s.bigg__timeSeriesStart
               ELSE datetime(data.start) 
           END,
       s.bigg__timeSeriesEnd = CASE 
           WHEN s.bigg__timeSeriesEnd >
            datetime(data.end) 
               THEN s.bigg__timeSeriesEnd
               ELSE datetime(data.end)
           END  
   return s
    """
    return query


def create_timeseries_from_element(uri_type, session, df, property_uri, estimation_method_uri, kpi_uri,
                                   kpi_component_uri, unit_uri, r, c, o, freq, agg_func):
    if uri_type == "SENSOR":
        session.run(create_sensor_bulk(df, property_uri, estimation_method_uri, unit_uri, r, c, o, freq, agg_func))
    elif uri_type == "SingleKPIAssessment":
        query = create_kpi_bulk(df, property_uri, estimation_method_uri,kpi_uri, kpi_component_uri, unit_uri, r, c, o, freq, agg_func)
        session.run(query)

def create_ts_id(uri_type, source, subject, date_type, freq):
    return f"{uri_type}-{source}-{subject}-{date_type}-RAW-{freq}"


def save_harmonized(df, config_save, n, user, config):
    neo = GraphDatabase.driver(**config['neo4j'])
    hbase_conn2 = config['hbase_store_harmonized_data']
    table_map = list(consumption_sources_mapping.values())
    for energy_type in [x for x in df.columns if x not in ['device_subject', "building_subject", 'start', 'end']]:
        st = time.time()
        utils.utils.log_string(f"saving {energy_type}", mongo=False)
        energy_df = df[[config_save['linking_subject'], 'start', 'end'] + [energy_type]].rename({energy_type: "value"},
                                                                                                axis=1)
        energy_df = energy_df.dropna(subset=['value'])
        energy_df['isReal'] = config_save['isReal']
        subject_hash_map = {}
        sensor_creation_df = pd.DataFrame()
        unit_uri = config_save['unit_uri']
        property_uri = config_save['property_uri_ns'][energy_type]
        if 'kpi_type_uri' in config_save:
            kpi_type_uri = f"http://bigg-project.eu/ontology#KPI-{config_save['kpi_type_uri']}"
            kpi_component_uri = bigg_enums.Total
        else:
            kpi_type_uri = None
            kpi_component_uri = None
        g = rdflib.Graph()
        for subject, consumption in energy_df.groupby(config_save['linking_subject']):
            link_uri = str(n[subject])
            if 'kpi_type_uri' in config_save:
                ts_id = create_ts_id(config_save['type'], config['source'], subject,
                                     f"{config_save['kpi_type_uri']}-Total-{energy_type}", config_save['freq'])
            else:
                ts_id = create_ts_id(config_save['type'], config['source'], subject, energy_type, config_save['freq'])
            ts_uri = str(n[ts_id])
            measurement_id = hashlib.sha256(ts_uri.encode("utf-8"))
            measurement_id = measurement_id.hexdigest()
            measurement_uri = str(n[measurement_id])
            subject_hash_map.update({subject: measurement_id})

            g.add((rdflib.URIRef(ts_uri), Bigg.hasMeasuredProperty, rdflib.URIRef(property_uri)))
            g.add((rdflib.URIRef(ts_uri), Bigg.hasEstimationMethod, rdflib.URIRef(bigg_enums.Naive)))
            g.add((rdflib.URIRef(ts_uri), Bigg.timeSeriesIsCumulative, rdflib.Literal(False)))
            g.add((rdflib.URIRef(ts_uri), Bigg.timeSeriesIsRegular, rdflib.Literal(True)))
            g.add((rdflib.URIRef(ts_uri), Bigg.timeSeriesIsOnChange, rdflib.Literal(False)))
            g.add((rdflib.URIRef(ts_uri), Bigg.timeSeriesFrequency, rdflib.Literal("P1Y")))
            g.add((rdflib.URIRef(ts_uri), Bigg.timeSeriesTimeAggregationFunction, rdflib.Literal("SUM")))
            g.add((rdflib.URIRef(ts_uri), Bigg.timeSeriesStart, rdflib.Literal(consumption.start.min())))
            g.add((rdflib.URIRef(ts_uri), Bigg.timeSeriesEnd, rdflib.Literal(consumption.end.max())))
            if config_save['type'] == "SENSOR":
                g.add((rdflib.URIRef(link_uri), Bigg.hasSensor, rdflib.URIRef(ts_uri)))
                g.add((rdflib.URIRef(ts_uri), rdflib.namespace.RDF.type, Bigg.Sensor))
                g.add((rdflib.URIRef(ts_uri), rdflib.namespace.RDF.type, Bigg.TimeSeriesList))
                g.add((rdflib.URIRef(measurement_uri), rdflib.namespace.RDF.type, Bigg.Measurement))
                g.add((rdflib.URIRef(measurement_uri), rdflib.namespace.RDF.type, Bigg.TimeSeriesPoint))
                g.add((rdflib.URIRef(ts_uri), Bigg.hasMeasurementUnit, rdflib.URIRef(unit_uri)))
                g.add((rdflib.URIRef(ts_uri), Bigg.hasMeasurement, rdflib.URIRef(measurement_uri)))
            elif config_save['type'] == "SingleKPIAssessment":
                g.add((rdflib.URIRef(link_uri), Bigg.assessesSingleKPI, rdflib.URIRef(ts_uri)))
                g.add((rdflib.URIRef(ts_uri), rdflib.namespace.RDF.type, Bigg.SingleKPIAssessment))
                g.add((rdflib.URIRef(ts_uri), rdflib.namespace.RDF.type, Bigg.TimeSeriesList))
                g.add((rdflib.URIRef(ts_uri), rdflib.namespace.RDF.type, Bigg.KPIAssessment))
                g.add((rdflib.URIRef(measurement_uri), rdflib.namespace.RDF.type, Bigg.SingleKPIAssessmentPoint))
                g.add((rdflib.URIRef(measurement_uri), rdflib.namespace.RDF.type, Bigg.TimeSeriesPoint))
                g.add((rdflib.URIRef(ts_uri), Bigg.hasKPIUnit, rdflib.URIRef(unit_uri)))
                g.add((rdflib.URIRef(ts_uri), Bigg.hasSingleKPIPoint, rdflib.URIRef(measurement_uri)))
                g.add((rdflib.URIRef(ts_uri), Bigg.hasMeasuredPropertyComponent, rdflib.URIRef(bigg_enums.Total)))
                g.add((rdflib.URIRef(ts_uri), Bigg.quantifiesKPI, rdflib.URIRef(kpi_type_uri)))

        utils.utils.log_string(f"parsing: {time.time()-st}", mongo=False)
        st = time.time()
        content = g.serialize(format="ttl")
        content = content.replace('\\"', "&apos;")
        content = content.replace("'", "&apos;")
        with neo.session() as s:
            response = s.run(f"""CALL n10s.rdf.import.inline('{content}','Turtle')""")
            print(response.single())

        utils.utils.log_string(f"saving neo4j: {time.time()-st}", mongo=False)
        st = time.time()
        energy_df['listKey'] = energy_df[config_save['linking_subject']].map(subject_hash_map)
        energy_df['bucket'] = ((pd.to_datetime(energy_df['start']).values.astype(
            int) // 10 ** 9) // settings.ts_buckets) % settings.buckets
        energy_df['start'] = (pd.to_datetime(energy_df['start']).values.astype(int)) // 10 ** 9
        energy_df['end'] = (pd.to_datetime(energy_df['end']).values.astype(int)) // 10 ** 9

        if config_save['type'] == "SENSOR":
            device_table = f"harmonized_online_{energy_type}_100_SUM_{config_save['freq']}_{user}"
            period_table = f"harmonized_batch_{energy_type}_100_SUM_{config_save['freq']}_{user}"
        elif config_save['type'] == "SingleKPIAssessment":
            device_table = f"harmonized_online_KPI-{config_save['kpi_type_uri']}_100_SUM_{config_save['freq']}_{user}"
            period_table = None
        else:
            print("type not known")
            return
        save_to_hbase(energy_df.to_dict(orient="records"), device_table, hbase_conn2,
                      [("info", ['end', 'isReal']), ("v", ['value'])],
                      row_fields=['bucket', 'listKey', 'start'])

        if period_table:
            save_to_hbase(energy_df.to_dict(orient="records"), period_table, hbase_conn2,
                          [("info", ['end', 'isReal']), ("v", ['value'])],
                          row_fields=['bucket', 'start', 'listKey'])
        utils.utils.log_string(f"save to hbase: {time.time()-st}", mongo=False)
        st = time.time()

        # save to mongo
        if config_save['type'] == "SingleKPIAssessment":
            mongo_con = utils.mongo.mongo_connection(config["mongo_cross"])
            collection = f"SingleKPI-{config_save['element']}-{config_save['kpi_type_uri']}-P1Y"
            data = energy_df[["start", "end", "isReal", "value"]]
            data['start'] = pd.to_datetime(data['start'], unit="s").dt.tz_localize("UTC")
            data['end'] = pd.to_datetime(data['end'], unit="s").dt.tz_localize("UTC")
            data['individualSubject'] = energy_df[config_save['linking_subject']].apply(lambda x: str(n[x]))
            data['measuredProperty'] = str(bigg_enums[energy_type])
            data['measuredPropertyComponent'] = str(bigg_enums.Total)
            data['unit'] = str(config_save['unit_uri'])
            data['modelSubject'] = None
            data['modelBased'] = False
            data['year'] = data['start'].dt.tz_convert("Europe/Sofia").dt.year
            new_doc = data.to_dict(orient="records")
            filter = data[["individualSubject", "measuredProperty", "measuredPropertyComponent", "year", "unit", "isReal"]].to_dict(orient="records")
            operations = []
            for i in range(len(filter)):
                operations.append(pymongo.ReplaceOne(filter[i], new_doc[i], upsert=True))
                if len(operations) > 1000:
                    mongo_con[collection].bulk_write(operations, ordered=False)
                    operations = []
            if len(operations) > 0:
                mongo_con[collection].bulk_write(operations, ordered=False)
        utils.utils.log_string(f"save to mongo: {time.time()-st}", mongo=False)
        st = time.time()
def harmonize_ts_kpi(kpi_config, **kwargs):
    namespace = kwargs['namespace']
    user = kwargs['user']
    n = Namespace(namespace)
    config = kwargs['config']
    for save_item in kpi_config:
        save_harmonized(save_item['df'], save_item['save_config'], n, user, config)
    print("finished")


def harmonize_all(data, **kwargs):
    """
    This function will harmonize all data in one execution. It is only used when reading from HBASE directly as it
    takes too long for kafka executions
    :param data: The json list with the data
    :param kwargs: the set of parameters for the harmonizer (user, namespace and config)
    :return:
    """
    harmonize_static(data, **kwargs)
    for s in range(0, 2):
        harmonize_kpi(data, split=s, **kwargs)
    for s in range(0, 25):
        harmonize_eem_kpi(data, split=s, **kwargs)
    harmonize_ts(data, **kwargs)


def harmonize_static(data, **kwargs):
    """
    This function will harmonize only the building information.
    :param data: The json list with the data
    :param kwargs: the set of parameters for the harmonizer (user, namespace and config)
    :return:
    """
    namespace = kwargs['namespace']
    user = kwargs['user']
    n = Namespace(namespace)
    config = kwargs['config']
    df = pd.DataFrame.from_records(data)
    df = df.applymap(decode_hbase)
    df_building, df_measures, df_measures_kpi, df_consumption, df_kpi_use_intensity, df_cost, df_emissions, \
        df_cost_intensity, df_emissions_intensity = prepare_all(df, user, config)
    mapper = Mapper(config['source'], n)
    g_building = generate_rdf(mapper.get_mappings("building_info"), df_building)
    save_rdf_with_source(g_building, config['source'], config['neo4j'])
    g_measures = generate_rdf(mapper.get_mappings("eem_savings"), df_measures)
    save_rdf_with_source(g_measures, config['source'], config['neo4j'])
    print("harmonized_static")


def harmonize_eem_kpi(data, split=None, **kwargs):
    """
    This function will harmonize only the EnergyCost and EnergyEmissions KPI the building information.
    :param data: The json list with the data
    :param split: the number of splits to run with all kpi (2)
    :param kwargs: the set of parameters for the harmonizer (user, namespace and config)
    :return:
    """
    prop_kpi = ["EnergyUseSavings", "EnergyUseSavingsIntensity"]
    #prop_kpi = []
    total_kpi = ["EnergyEmissionsSavings", "EnergyEmissionsSavingsIntensity", "EnergyCostSavings", "EnergyCostSavingsIntensity"]
    #total_kpi = ["EnergyEmissionsSavingsIntensity"]
    no_prop_kpi = ['NormalisedInvestmentCost', 'AvoidanceCost', 'SimplePayback', 'NetPresentValue', 'ProfitabilityIndex',
                    'NetPresentValueQuotient', 'InternalRateOfReturn']
    #no_prop_kpi = ['ProfitabilityIndex', 'InternalRateOfReturn']
    kpi_list = list(itertools.chain.from_iterable([[[f"{kpi}~{prop}"] for prop in consumption_sources_mapping.values()] for kpi in prop_kpi]))
    kpi_list += [[f"{kpi}~EnergyConsumptionTotal"] for kpi in total_kpi]
    kpi_list += [[i] for i in no_prop_kpi]
    if split >= len(kpi_list):
        return
    kpi_list = kpi_list[split]
    namespace = kwargs['namespace']
    user = kwargs['user']
    n = Namespace(namespace)
    config = kwargs['config']
    df = pd.DataFrame.from_records(data)
    df = df.applymap(decode_hbase)
    df_building, df_measures, df_measures_kpi, df_consumption, df_kpi_use_intensity, df_cost, df_emissions, \
        df_cost_intensity, df_emissions_intensity = prepare_all(df, user, config)
    df_measures_kpi["KPI-prop"] = \
        df_measures_kpi.apply(lambda x: "~".join([x.KPI, x.measured_property]) if x.measured_property else x.KPI, axis=1)
    kpi_config = []
    for kpi_prop in kpi_list:
        try:
            kpi_df = df_measures_kpi.groupby("KPI-prop").get_group(kpi_prop)
        except:
            continue
        if kpi_df.empty:
            continue
        try:
            kpi, energy_type = kpi_prop.split("~")
        except:
            energy_type = "NOPROPERTY"
            kpi = kpi_prop

        try:
            unit = bigg_enums[kpi_df.unit.unique()[0]]
        except:
            unit = units[kpi_df.unit.unique()[0]]
        kpi_df = kpi_df.pivot_table(index=["eem_subject", "start", "end"], columns="KPI",
                                     values="value").reset_index()
        if kpi_df.empty:
            continue

        kpi_df = kpi_df.rename({"eem_subject": "device_subject", kpi: energy_type}, axis=1)
        kpi_config.append(
            {
                "df": kpi_df[["start", "end", "device_subject", energy_type]],
                "save_config": {
                    "element": "EEM",
                    "type": "SingleKPIAssessment",
                    "linking_subject": "device_subject",
                    "isReal": False,
                    "unit_uri": unit,
                    "property_uri_ns": bigg_enums,
                    "kpi_type_uri": kpi,
                    "freq": "P1Y"
                }
            }
        )
    harmonize_ts_kpi(kpi_config, namespace=namespace, user=user, config=config)
    print(f"harmonized {[x for x in kpi_list]}")


def harmonize_kpi(data, split=None, **kwargs):
    """
    This function will harmonize only the EnergyCost and EnergyEmissions KPI the building information.
    :param data: The json list with the data
    :param split: the number of splits to run with all kpi (2)
    :param kwargs: the set of parameters for the harmonizer (user, namespace and config)
    :return:
    """
    namespace = kwargs['namespace']
    user = kwargs['user']
    n = Namespace(namespace)
    config = kwargs['config']
    df = pd.DataFrame.from_records(data)
    df = df.applymap(decode_hbase)
    df_building, df_measures, df_measures_kpi, df_consumption, df_kpi_use_intensity, df_cost, df_emissions, \
        df_cost_intensity, df_emissions_intensity = prepare_all(df, user, config)
    kpi_list = [[("EnergyCost", df_cost, units['BulgarianLev']), ("EnergyEmissions", df_emissions, bigg_enums['KiloGM-CO2'])],
                [("EnergyCostIntensity", df_cost_intensity, bigg_enums['BulgarianLev-M2']),
                 ("EnergyEmissionsIntensity", df_emissions_intensity, bigg_enums['KiloGM-CO2-M2'])]]
    try:
        kpi_list = kpi_list[split]
    except:
        print("the split number is too big")
    kpi_config = []
    for kpi, df, unit in kpi_list:
        kpi_config.append(
            {
                "df": df,
                "save_config": {
                    "type": "SingleKPIAssessment",
                    "element": "Building",
                    "linking_subject": "building_subject",
                    "isReal": False,
                    "unit_uri": unit,
                    "property_uri_ns": bigg_enums,
                    "kpi_type_uri": f"{kpi}",
                    "freq": "P1Y"
                }
            }
        )
    harmonize_ts_kpi(kpi_config, namespace=namespace, user=user, config=config)
    print(f"harmonized {[x[0] for x in kpi_list]}")


def harmonize_ts(data, **kwargs):
    """
    This function will harmonize for each column sent, the data for each energy type..
    :param data: The json list with the data
    :param kwargs: the set of parameters for the harmonizer (user, namespace and config)
    :return:
    """
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']
    n = Namespace(namespace)
    df = pd.DataFrame.from_records(data)
    df = df.applymap(decode_hbase)
    df_building, df_measures, df_measures_kpi, df_consumption, df_kpi_use_intensity, df_cost, df_emissions, \
        df_cost_intensity, df_emissions_intensity = prepare_all(df, user, config)
    mapper = Mapper(config['source'], n)
    for con in consumption_sources_mapping.values():
        if any([True if re.match(fr".*{con}", x) else False for x in df_consumption.columns
                if re.match(device_subject_reg, x)]):
            df_tmp = df_consumption.copy(deep=True)
            df_tmp = df_tmp.rename({f'device_subject_{con}': 'device_subject',
                                    f'utility_subject_{con}': 'utility_subject'}, axis=1)
            df_tmp['name'] = df_tmp['building_space_subject'].apply(lambda x: x[-5:] + "-" + con)
            df_tmp['utility_type'] = to_object_property(resource_type_map[con][0], namespace=bigg_enums)
            df_tmp['device_type'] = to_object_property(resource_type_map[con][1], namespace=bigg_enums)
            df_tmp = df_tmp.dropna(subset=[con])
            if df_tmp.empty:
                continue
            g_building = generate_rdf(mapper.get_mappings("building_upods_dev"), df_tmp)
            save_rdf_with_source(g_building, config['source'], config['neo4j'])
            #link_devices_with_source(df_tmp, n, config['neo4j'])
            df_tmp = df_tmp[['device_subject', "building_subject", 'start', 'end', con]]
            kpi_config = [
                {
                    "df": df_tmp,
                    "save_config": {
                        "type": "SENSOR",
                        "linking_subject": "device_subject",
                        "isReal": False,
                        "unit_uri": units["KiloW-HR"],
                        "property_uri_ns": bigg_enums,
                        "freq": "P1Y"
                    }
                },
                {
                    "df": df_tmp,
                    "save_config": {
                        "type": "SingleKPIAssessment",
                        "element": "Building",
                        "linking_subject": "building_subject",
                        "isReal": False,
                        "unit_uri": units["KiloW-HR"],
                        "property_uri_ns": bigg_enums,
                        "kpi_type_uri": "EnergyUse",
                        "freq": "P1Y"
                    }
                }
            ]
            harmonize_ts_kpi(kpi_config, namespace=namespace, user=user, config=config)
            print(f"harmonized {[x for x in df_consumption if x not in ['device_subject', 'building_subject', 'start', 'end']]}")
    kpi_config = [
        {
            "df": df_kpi_use_intensity,
            "save_config": {
                "type": "SingleKPIAssessment",
                "element": "Building",
                "linking_subject": "building_subject",
                "isReal": False,
                "unit_uri": bigg_enums["KiloW-HR-M2"],
                "property_uri_ns": bigg_enums,
                "kpi_type_uri": "EnergyUseIntensity",
                "freq": "P1Y"
            }
        }
    ]
    harmonize_ts_kpi(kpi_config, namespace=namespace, user=user, config=config)
    print(f"harmonized {[x for x in df_kpi_use_intensity if x not in ['device_subject', 'building_subject', 'start', 'end']]}")

"""
1- delete all objects:
Match(n) where n.uri=~"https://bulgaria.bg#.*" detach delete n
2- create the things

echo "main org"
python3 -m set_up.Organizations -f data/Organizations/bulgaria-organizations.xls -name "Bulgaria" -u "bulgaria" -n "https://bulgaria.bg#"
echo "summary source"
python3 -m set_up.DataSources -u "bulgaria" -n "https://bulgaria.bg#" -f data/DataSources/bulgaria.xls -d SummarySource
echo "simpleTariff source"
python3 -m set_up.DataSources -u "bulgaria" -n "https://bulgaria.bg#" -f data/DataSources/bulgaria.xls -d SimpleTariffSource
echo "co2Emisions source"
python3 -m set_up.DataSources -u "bulgaria" -n "https://bulgaria.bg#" -f data/DataSources/bulgaria.xls -d CO2EmissionsSource


MATCH (l1:Language{iso__code: 'bg'}) 
MATCH (l2:Language{iso__code: 'en'}) 
MATCH (o1:bigg__Organization{userID:'bulgaria'}) 
Merge (o1)-[:hasAvailableLanguage {selected: false, languageByDefault: false}]->(l1)
Merge (o1)-[:hasAvailableLanguage {selected: true, languageByDefault: true}]->(l2); 

Match(n:bigg__Organization{userID:"bulgaria"}) 
Merge (n)-[:hasConfig]->(c:AppConfiguration{createBuilding: true, epc:"bulgaria", uploadManualData: true});

MATCH (p:bigg__Person{bigg__userName:'admin'})
MATCH (o:bigg__Organization) WHERE not o.userID is null
MERGE (p)-[:bigg__managesOrganization]->(o);


3- create with new file
python3 -m gather -so Bulgaria -f "data/Bulgaria" -u "bulgaria" -n "https://bulgaria.bg#" -st direct

4- create the links between the sources and the main source:
Match(n:bigg__Device) where n.uri=~"https://bulgaria.bg#.*" and not exists(()<-[:importedFromSource]-(n)) set n.source="SummarySource" return n;

Match(n:bigg__Device) where n.uri=~"https://bulgaria.bg#.*" and not exists(()<-[:importedFromSource]-(n)) Match(s) 
where id(s)=<id> merge (s)<-[:importedFromSource]-(n)

ANALYTICS:
Delete the freq=null unused kpi
MAtch(k)<-[:bigg__quantifiesKPI]-(n:bigg__SingleKPIAssessment)<-[:bigg__assessesSingleKPI]-(:bigg__EnergyEfficiencyMeasure) 
where n.uri=~"https://icaen.cat#.*" and k.uri=~"http://bigg-project.eu/ontology#KPI-[Energy].*"  
return distinct k.uri, collect(distinct coalesce(n.bigg__timeSeriesFrequency,"null"))

MAtch(k)<-[:bigg__quantifiesKPI]-(n:bigg__SingleKPIAssessment)<-[:bigg__assessesSingleKPI]-(:bigg__EnergyEfficiencyMeasure) 
where n.uri=~"https://icaen.cat#.*" and k.uri=~"http://bigg-project.eu/ontology#KPI-[Energy].*" and n.bigg__timeSeriesFrequency is null detach delete n


set financial and cost freq 

MAtch(k)<-[:bigg__quantifiesKPI]-(n:bigg__SingleKPIAssessment)<-[:bigg__assessesSingleKPI]-(:bigg__EnergyEfficiencyMeasure) 
where n.uri=~"https://icaen.cat#.*" and k.uri=~"http://bigg-project.eu/ontology#KPI-[^Energy].*"
return distinct k.uri, collect(distinct coalesce(n.bigg__timeSeriesFrequency,"null"))

set P1Y and SUM
MAtch(k)<-[:bigg__quantifiesKPI]-(n:bigg__SingleKPIAssessment)<-[:bigg__assessesSingleKPI]-(:bigg__EnergyEfficiencyMeasure) 
where n.uri=~"https://icaen.cat#.*" and k.uri=~"http://bigg-project.eu/ontology#KPI-[^Energy].*"
SET n.bigg__timeSeriesFrequency="P1Y", n.bigg__timeSeriesTimeAggregationFunction="SUM" 
return distinct k.uri, collect(distinct coalesce(n.bigg__timeSeriesFrequency,"null"))

set propoerty to NOPROPERTY
MAtch(k)<-[:bigg__quantifiesKPI]-(n:bigg__SingleKPIAssessment)<-[:bigg__assessesSingleKPI]-(:bigg__EnergyEfficiencyMeasure) 
where n.uri=~"https://icaen.cat#.*" and k.uri=~"http://bigg-project.eu/ontology#KPI-[^Energy].*"
Match(n)-[:bigg__hasMeasuredProperty]-(p)
return distinct k.uri, collect(distinct coalesce(n.bigg__timeSeriesFrequency,"null")), collect(distinct coalesce(p.uri,"null"))

MAtch(k)<-[:bigg__quantifiesKPI]-(n:bigg__SingleKPIAssessment)<-[:bigg__assessesSingleKPI]-(:bigg__EnergyEfficiencyMeasure) 
where n.uri=~"https://icaen.cat#.*" and k.uri=~"http://bigg-project.eu/ontology#KPI-[^Energy].*"
Match(p1{uri:"http://bigg-project.eu/ontology#NOPROPERTY"})
Match(n)-[r:bigg__hasMeasuredProperty]->(p)
Merge(n)-[:bigg__hasMeasuredProperty]->(p1)
delete r
return n,p,p1

MONGO:
for energy,cost,emissions
(*SavingsIntensity-P1Y').updateMany({individualSubject:{$regex:"https://icaen.cat.*"}}, [{$set: {"start":{$toDate:"$start"}}},{$set: {"end":{$toDate:"$end"}}},{$set:{modelBased:false}}])
for cost and finance
(*)).updateMany({individualSubject:{$regex:"https://icaen.cat.*"}}, [{$set: {"start":{$toDate:"$start"}}},{$set: {"end":{$toDate:"$end"}}},{$set:{modelBased:false}},{$set:{"measuredProperty" : "http://bigg-project.eu/ontology#NOPROPERTY"}}])
"""
