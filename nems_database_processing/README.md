# Overview
This repo includes scripts and inputs to preprocess the final NEMS fleet that is used to run ReEDS 2.0.

# Running scripts
All the scripts are run sequentially from `run.sh`

# run.sh
`run.sh` runs 5 python scripts that process NEMS fleet data sequentially:
* `a_inheritance.py` 
* `b_aeo_cleaning.py`: This script cleans raw AEO-NEMS and EIA860M files and appends planned and missing existing EIA860M units into AEO-NEMS, and also updates unit retirement years according to specified version of EIA860M
* `c_geospatial_mapping.py`: This script maps the lon/lats of units database established in step b to their counties and FIPS. For any units that are missing lon/lats, please look up their lon/lats and manually add these units with their lon/lats to in `/Inputs/user_adjusted_units_missing_lon_lats.csv`. This step will incur errors until all units are mapped to their counties and FIPS. Any units that need manually adjusted locations should be done in this step.
* `d_hydro_classification.py` 
* `e_additional_inputs.py`, which includes `e1_set_retire_years.py`, `e2_fix_upgrades.py`, and `e3_merge_psh_dbs.py`: This script handles updated retirement years that are outdated in AEO-NEMS and EIA860M, fix upgrades, and handles other additional adjustments. Any manually adjusted retirement years should be included in `e1_set_retire_years.py`.

# Input files and params to run run.sh
All the input files to run all 5 python scripts are now specified upfront in run.sh. All inputs files are loacted in `Inputs` folder.
| Input | Description |
| --- | --- |
| `current_reeds_fleet_ver` | Most recent version of final NEMS fleet used in ReEDS. Right now is `2024` |
| `aeo_file` | Most recent version of AEO file. Right now it is `PLTF860_RDB.xlsx` |
| `eia860M_ver_mon` | Most recent EIA 860M version month (all lower case). Right now it's `march`  |
| `eia860M_ver_year` | Most recent EIA 860M version year (number). Right now is `2025`|
| `append_operating_units` | `True` if append both missing operating and planned units from EIA860 to NEMS, `False` if append only planned units|
| `nems_ver` | Most recent version of NEMS. Right now it is `2023`|
| `ornl_hydro_plant_ver` | Most recent version of EHA hydro plants from ORNL used in `d_hydro_classification.py`. Right now it is `ORNL_EHAHydroPlant_FY2024.xlsx`|
| `hydro_dispatchability` | Most recent version of EHA units dispatchability used in `d_hydro_classification.py`. Right now it is `EHA_dispatchability.csv`|
| `hydro_prjtype` | Most recent version EHA hydro unit classification updates, used in `e3_merge_psh_dbs.py`. Right now it is `EHA_FY22_post2009_prjtype.xlsx`|
| `ornl_hydro_unit_ver` | Most recent version of EHA hydro plants from ORN used in `e3_merge_psh_dbs.py`. Right now it is `ORNL_EHAHydroUnit_PublicFY2024.xlsx`|


# Other input files that probably are updated less frequently
All located in `Inputs` folder:
* `county_to_reeds_region.csv`
* `tech_to_cooling_tech_map.csv`

# Output file:
Located in `Outputs` folder. This is the final file that will be used to run ReEDS-2.0:
* `ReEDS_generator_database_final_EIA-NEMS.csv`
