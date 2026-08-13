#! /usr/bin/env sh

############## Input parameters ##############
##############################################
# Specify reeds_path (to get data from ReEDS repo, locally or on HPC):
reeds_path='~/Documents/GitHub/ReEDS/public_ReEDS/ReEDS/'

# Most recent ReEDS fleet and AEO versions:
current_reeds_fleet_ver=2025
current_year=2026
# Data source: https://github.com/EIAgov/NEMS/blob/main/input/emm/emm_db/PLTF860_RDB.xlsx
aeo_file='PLTF860_RDB.xlsx'

# Most recent EIA 860M and nems versions:
# Most recent EIA 860M version month:
eia860M_ver_mon='june'
# Most recent EIA 860M version year:                       
eia860M_ver_year=2026
# NEMS version:                           
nems_ver=2023                                   

# Hydro units files:
ornl_hydro_plant_ver='ORNL_EHAHydroPlant_PublicFY2024.xlsx'
hydro_dispatchability='EHA_dispatchability.csv'
hydro_prjtype='EHA_FY22_post2009_prjtype.xlsx'
ornl_hydro_unit_ver='ORNL_EHAHydroUnit_PublicFY2024.xlsx'

# Coal plant retirement file:
coal_plant_retirement='EIA860_2025ER_CoalRetirements.csv'

# Average lithium battery duration:
battery_duration=2.9

############## Run scripts to process fleet ##############
##########################################################
python a_data_cleaning.py "$aeo_file" "$eia860M_ver_mon" "$eia860M_ver_year" "$nems_ver" "$battery_duration"
python b_geospatial_mapping.py "$reeds_path"
python c_hydro_classification.py "$ornl_hydro_plant_ver" "$hydro_dispatchability"
python d_additional_inputs.py "$current_reeds_fleet_ver" "$hydro_prjtype" "$ornl_hydro_unit_ver" "$coal_plant_retirement" "$current_year"
python e_comparison_plotting.py "$current_reeds_fleet_ver" "$reeds_path"
