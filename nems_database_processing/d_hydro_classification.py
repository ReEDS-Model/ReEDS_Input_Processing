import pandas as pd
import os
import sys

def categorize(eha_file, dispatchability_file, reedsgens):

    # Load EHA dataset

    eha = pd.read_excel(eha_file, sheet_name="Operational")
    dispatchability = pd.read_csv(dispatchability_file, index_col="Mode")
    eha = eha.join(dispatchability, on="Mode")
    eha["dispatchable"] = (eha.Dispatchability == "hydED") * eha.Number_of_Units
    eha["nondispatchable"] = (eha.Dispatchability == "hydEND") * eha.Number_of_Units
    eha["unknowndispatchable"] = eha.Dispatchability.isna() * eha.Number_of_Units

    # Determine plant-level dispatchability classifications

    eha_plants = eha.loc[:, ["Number_of_Units", "CH_MW", "ReEDSPCA", "dispatchable", "nondispatchable", "unknowndispatchable", "EIA_PtID"]].groupby("EIA_PtID").sum()
    eha_plants.columns = ["eha_units", "eha_capacity", "eha_pca",
        "eha_units_dispatchable", "eha_units_nondispatchable", "eha_units_unknowndispatchable"]
    eha_plants.index.name = "EIAPlantID"

    eha_plants["eha_tech"] = "mixed"
    eha_plants.loc[eha_plants.eha_units == eha_plants.eha_units_dispatchable, "eha_tech"] = "hydED"
    eha_plants.loc[eha_plants.eha_units == eha_plants.eha_units_nondispatchable, "eha_tech"] = "hydEND"
    eha_plants.loc[eha_plants.eha_units == eha_plants.eha_units_unknowndispatchable, "eha_tech"] = "unavailable"

    # Map dispatchabilities to ReEDS units

    result = reedsgens.loc[reedsgens.tech == "hydro", ["T_PID", "T_UID", "TSTATE", "TC_SUM"]]
    result.rename({"T_PID": "eia_plant_id", "T_UID": "eia_unit_id",
                   "TSTATE": "state", "TC_SUM": "summer_power_capacity_MW"}, inplace=True, axis=1)

    result = result.join(eha_plants.loc[:, ["eha_tech", "eha_pca"]], on="eia_plant_id")

    # Plant ID 54134 is actually 6 different plants, all but one
    # (unit IDs with "WEH") are dispatchable

    id_54134_all = (result.eha_tech == "mixed") & (result.eia_plant_id == 54134)
    result.loc[id_54134_all, "eha_tech"] = "hydED"

    id_54134_disp = id_54134_all & (result.eia_unit_id.str.contains("WEH"))
    result.loc[id_54134_disp, "eha_tech"] = "hydEND"

    # Make sure all "mixed" plants have been addressed
    assert((result.eha_tech != "mixed").all())

    # Impute dispatchability data when values aren't available from EHA

    unclassified_units = (result.eha_tech != "hydED") & (result.eha_tech != "hydEND")
    n_unclassified = unclassified_units.sum()
    mw_unclassified = result.loc[unclassified_units, "summer_power_capacity_MW"].sum()

    if n_unclassified > 0:
        print("NOTE:", n_unclassified, "hydro units (", round(mw_unclassified,2), "MW ) "
              "are missing dispatchability data - "
              "they will be assumed non-dispatchable")

    result["is_fallback_tech"] = False
    result.loc[unclassified_units, "is_fallback_tech"] = True
    result.loc[unclassified_units, "eha_tech"] = "hydEND"

    # Make sure all units are categorized
    assert(((result.eha_tech == "hydED") | (result.eha_tech == "hydEND")).all())

    return result

if __name__ == "__main__":

    gdbinputname = 'c_to_d.csv'
    gdboutputname = 'd_to_e.csv'

    ornl_hydro_unit_ver = sys.argv[1]
    hydro_dispatchability = sys.argv[2]

    gendb = pd.read_csv(os.path.join('Outputs', gdbinputname),
                        float_precision="round_trip", low_memory=False)

    eha_techs = categorize(
        os.path.join('Inputs','ORNL_EHA',ornl_hydro_unit_ver),
        os.path.join('Inputs','ORNL_EHA',hydro_dispatchability),
        gendb)

    # Merge EHA unit data back into new unit database file

    gendb = gendb.join(eha_techs.eha_tech, how='left')
    gendb.loc[~gendb.eha_tech.isna(), "tech"] = gendb.loc[~gendb.eha_tech.isna(), "eha_tech"]
    gendb.drop(columns="eha_tech", inplace=True)

    gendb.to_csv(os.path.join('Outputs', gdboutputname), index=False)

