#! /usr/bin/env sh

############## Input parameters ##############
##############################################
# Specify reeds_path (to get data from ReEDS-2.0 repo, locally or on super computer):
reeds_path='~/Documents/Github/ReEDS/ReEDS-2.0/'

# Most recent ReEDS fleet and AEO versions:
current_reeds_fleet_ver=2024
current_year=2025
# Data source: https://github.com/EIAgov/NEMS/blob/main/input/emm/emm_db/PLTF860_RDB.xlsx
aeo_file='PLTF860_RDB.xlsx'

# Most recent EIA 860M and nems versions:
# Most recent EIA 860M version month:
eia860M_ver_mon='october'
# Most recent EIA 860M version year:                       
eia860M_ver_year=2025
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
python a_inheritance.py "$current_reeds_fleet_ver" "$aeo_file"
python b_aeo_cleaning.py "$eia860M_ver_mon" "$eia860M_ver_year" "$nems_ver" "$battery_duration"
python c_geospatial_mapping.py "$reeds_path"
python d_hydro_classification.py "$ornl_hydro_plant_ver" "$hydro_dispatchability"
python e_additional_inputs.py "$current_reeds_fleet_ver" "$hydro_prjtype" "$ornl_hydro_unit_ver" "$coal_plant_retirement" "$current_year"
python f_comparison_plotting.py "$current_reeds_fleet_ver" "$reeds_path"
