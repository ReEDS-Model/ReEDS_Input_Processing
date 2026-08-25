#! /usr/bin/env sh

############## Input parameters ##############
##############################################
# Specify reeds_path (to get data from ReEDS repo, locally or on HPC):
reeds_path='~/Documents/GitHub/ReEDS/public_ReEDS/ReEDS/'

# Specify current year:
current_year=2026
# Specify AEO-NEMS data file: 
# (source: https://github.com/EIAgov/NEMS/blob/main/input/emm/emm_db/PLTF860_RDB.xlsx)
aeo_file='PLTF860_RDB.xlsx'

# Specify most recent EIA860M version by month and year:
eia860M_ver_mon='june'
eia860M_ver_year=2026                                 

# Specify hydro units data files:
ornl_hydro_plant_ver='ORNL_EHAHydroPlant_PublicFY2024.xlsx'
ornl_hydro_unit_ver='ORNL_EHAHydroUnit_PublicFY2024.xlsx'
hydro_dispatchability='EHA_dispatchability.csv'
hydro_prjtype='EHA_FY22_post2009_prjtype.xlsx'

# Specify coal plant retirement file:
coal_plant_retirement='EIA860_2025ER_CoalRetirements.csv'

# Assumed average duration for planned storage units:
battery_duration=2.9

############## Run scripts to process fleet ##############
##########################################################
python a_data_cleaning.py "$aeo_file" "$eia860M_ver_mon" "$eia860M_ver_year" "$battery_duration" "$current_year"
python b_geospatial_mapping.py "$reeds_path"
python c_hydro_classification.py "$ornl_hydro_plant_ver" "$hydro_dispatchability"
python d_additional_inputs.py "$hydro_prjtype" "$ornl_hydro_unit_ver" "$coal_plant_retirement" "$current_year" "$reeds_path"
python e_comparison_plotting.py "$reeds_path"
