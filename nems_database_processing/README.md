# Overview
This repo includes scripts and inputs to preprocess the final NEMS fleet that is used to run ReEDS 2.0.

# Running scripts
All the scripts are run sequentially from `run.sh`

# run.sh
`run.sh` runs 5 python scripts that process NEMS fleet data sequentially:
* `a_data_cleaning.py`: This script cleans raw AEO-NEMS and EIA860M files and appends planned and missing existing EIA860M units into AEO-NEMS, and also updates unit retirement years according to specified version of EIA860M
* `b_geospatial_mapping.py`: This script maps the lon/lats of units database established in step a to their FIPS. For any units that are missing lon/lats, please look up their lon/lats and manually add these units with their lon/lats to in `/inputs/user_adjusted_units_missing_lon_lats.csv`. This step will incur errors until all units are mapped to their FIPS. Any units that need manually adjusted locations should be done in this step.
* `c_hydro_classification.py`: This script determines which ReEDS hydro units are dispatchable or non-dispatchable
* `d_additional_inputs.py`, which includes `d1_set_retire_years.py`, `d2_fix_upgrades.py`, and `d3_merge_psh_dbs.py`: This script handles updated retirement years that are outdated or missing in AEO-NEMS and EIA860M, fix upgrades, and handles other additional adjustments. Any manually adjusted retirement years should be included in `d1_set_retire_years.py`.
* `e_comparison_plotting.py`: This script generates comparison figures between previous version of NEMS and the newly updated version for validation.

# Input files and params to run run.sh
All the input files to run all 5 python scripts are now specified upfront in run.sh. All inputs files are loacted in `Inputs` folder.
| Input | Description |
| --- | --- |
| `current_reeds_fleet_ver` | Most recent version of final NEMS fleet used in ReEDS. Right now is `2025` |
| `aeo_file` | Most recent version of AEO file. Right now it is `PLTF860_RDB.xlsx` |
| `eia860M_ver_mon` | Most recent EIA 860M version month (all lower case). Right now it's `june`  |
| `eia860M_ver_year` | Most recent EIA 860M version year (number). Right now is `2026`|
| `ornl_hydro_plant_ver` | Most recent version of EHA hydro plants from ORNL used in `c_hydro_classification.py`. Right now it is `ORNL_EHAHydroPlant_PublicFY2024.xlsx`|
| `hydro_dispatchability` | Most recent version of EHA units dispatchability used in `c_hydro_classification.py`. Right now it is `EHA_dispatchability.csv`|
| `coal_plant_retirement` | List of coal plant retirement data that are exempt from general coal retirement rules, used in `df_set_retire_year.py`. Right now it is `EIA860_2025ER_CoalRetirements.csv`|
| `ornl_hydro_unit_ver` | Most recent version of EHA hydro plants from ORN used in `d3_merge_psh_dbs.py`. Right now it is `ORNL_EHAHydroUnit_PublicFY2024.xlsx`|
| `hydro_prjtype` | Most recent version EHA hydro unit classification updates, used in `d3_merge_psh_dbs.py`. Right now it is `EHA_FY22_post2009_prjtype.xlsx`|


# Other input files that probably are updated less frequently
All located in `inputs/tech_mappings` folder:
* `aeo_reeds_tech_map.csv`: Mapping between AOE-NEMS tech names and ReEDS tech names
* `eia_reeds_tech_map.csv`: Mapping between EIA860M tech names and ReEDS tech names
* `tech_to_cooling_tech_map`: Mapping cooling techs to ReEDS techs

# Output file
Located in `outputs` folder. This is the final file that will be used to run ReEDS:
* `ReEDS_generator_database_final_EIA-NEMS.csv`

# Output comparison figures
Located in `outputs/figures`. This folder includes figures of online/retire capacity by year for different NEMS versions and their differences. Figures are both national and at `z90` resolution. The figures are generated from `e_comparison_plotting.py`.

# Debugging
A `debug` folder, which stores the FIPS that have mismatched online/retire capacity between two NEMS versions, is created for easy debugging. If the two NEMS versions have no changes at all at FIPS-level, this folder is not created or is empty.

# Note
Sometimes when running `b_geospatial_mapping.py`, geopandas operation results in invalid geometry - point (inf, inf) or polygon (inf, inf). If this occurs, run `conda install -c conda-forge proj-data` to pre-download projection data in the current environment before rerunning the script.
