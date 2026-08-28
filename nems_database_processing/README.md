# Overview
This repo includes scripts and inputs to clean and process AEO-NEMS generator capacity data and EIA860M generator inventory data, merge them together and perform additional adjustment and clean-up to generate final EIA-NEMS generator database input file that is used to run ReEDS.

# Running scripts
All the scripts are run sequentially from `run.sh`

# run.sh
`run.sh` runs 5 python scripts sequentially to generate final EIA-NEMS input data file:
* `a_data_cleaning.py`: This script cleans raw AEO-NEMS (https://github.com/EIAgov/NEMS/blob/main/input/emm/emm_db/PLTF860_RDB.xlsx) and EIA860M (https://www.eia.gov/electricity/data/eia860m/) files, appends planned, retired, and missing operating EIA860M units into AEO-NEMS, and updates unit online and retire years according to specified version of EIA860M
* `b_geospatial_mapping.py`: This script maps the lon/lats of units database established in step a to their FIPS. For any units that are missing lon/lats, please look up their lon/lats and manually add these units with their lon/lats to in `/inputs/user_adjusted_units_missing_lon_lats.csv`. This step will incur errors until all units are mapped to their FIPS. Any units that need manually adjusted locations should be done in this step.
* `c_hydro_classification.py`: This script determines which ReEDS hydro units are dispatchable or non-dispatchable using input hydro unit data from ORNL.
* `d_additional_inputs.py`, which includes `d1_set_retire_years.py`, `d2_fix_upgrades.py`, and `d3_merge_psh_dbs.py`, and handles other additional adjustments and clean up before generating final EIA-NEMS dataset. 
* * `d1_set_retire_years.py`: This script handles updated retire years that are outdated or missing in AEO-NEMS and EIA860M, using published list of coal plants that are exempted from 2024 Mercury and Air Toxics Standards (MATS) (https://www.epa.gov/system/files/documents/2025-04/regulatory-relief-for-certain-stationary-annex-1.pdf), and online articles announcing specific plants' early retirements, restarts, and retrofits. Any manually adjusted retirement years should be included in this step.
* * `d2_fix_upgrades.py`: This script updates online and retire dates for units that are upgraded.
* * `d3_merge_psh_dbs.py`: This script merges the two hydro databases from ORNL and uses the merged database to reclassify various hydro units.
* `e_comparison_plotting.py`: This script generates comparison figures between previous version of NEMS and the newly updated version for validation.

# Input files and params to run run.sh
All the input files to run all 5 python scripts are now specified upfront in run.sh. All inputs files are loacted in `inputs` folder.
| Input | Description |
| --- | --- |
| `current_year` | Specify current year. Right now it is `2026` |
| `aeo_file` | Most recent version of AEO file. Right now it is `PLTF860_RDB.xlsx` |
| `eia860M_ver_mon` | Most recent EIA 860M version month (all lower case). Right now it's `june`  |
| `eia860M_ver_year` | Most recent EIA 860M version year (number). Right now is `2026`|
| `ornl_hydro_plant_ver` | Most recent version of EHA hydro plants from ORNL used in `c_hydro_classification.py`. Right now it is `ORNL_EHAHydroPlant_PublicFY2024.xlsx`|
| `hydro_dispatchability` | Most recent version of EHA units dispatchability used in `c_hydro_classification.py`. Right now it is `EHA_dispatchability.csv`|
| `coal_plant_retirement` | List of coal plant retirement data that are exempt from general coal retirement rules, used in `d1_set_retire_year.py`. Right now it is `EIA860_2025ER_CoalRetirements.csv`|
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
- When read in EIA860M csv file, make sure to check if the file has empty first row and empty last two rows to drop them out of the dataframe before processing.
- Sometimes when running `b_geospatial_mapping.py`, geopandas operation results in invalid geometry - point (inf, inf) or polygon (inf, inf). If this occurs, run `conda install -c conda-forge proj-data` to pre-download projection data in the current environment before rerunning the script.
