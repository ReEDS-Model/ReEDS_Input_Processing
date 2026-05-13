# -*- coding: utf-8 -*-
"""
Created on Mon May 30 17:37:00 2022

@author: wcole
"""

import pandas as pd
import os
import sys
import numpy as np
from e1_set_retire_years import set_retire_years
from e2_fix_upgrades import fix_upgrades
from e3_merge_psh_dbs import merge_psh_dbs

#%%

current_fleet_yr = int(sys.argv[1])
hydro_prjtype = sys.argv[2]
ornl_hydro_unit_ver = sys.argv[3]
coal_plant_retirement = sys.argv[4]
gdboldname = 'ReEDS_generator_database_final_EIA-NEMS_' + str(current_fleet_yr) + '.csv'
current_year = int(sys.argv[5])
output_changes = 1

# For debugging
#current_fleet_yr=2024
#current_year=2025
#hydro_prjtype='EHA_FY22_post2009_prjtype.xlsx'
#ornl_hydro_unit_ver='ORNL_EHAHydroUnit_PublicFY2024.xlsx'
#coal_plant_retirement='EIA860_2025ER_CoalRetirements.csv'
#gdboldname = 'ReEDS_generator_database_final_EIA-NEMS_2024.csv'

gdbinputname = 'd_to_e.csv'
gdbfinalname = 'ReEDS_generator_database_final_EIA-NEMS.csv'

dfin = pd.read_csv(os.path.join('Outputs',gdbinputname), low_memory=False)
#dfin = pd.read_csv("/Users/apham/Documents/GitHub/ReEDS_Input_Processing/NEMS_database_processing/Outputs/d_to_e.csv")

# Add nuclear retirement bins
# Bin 1 indicates that the plant is at greater risk of retirement, which is due
# to it being a single reactor plant or residing in a restructured market.
# Bin 2 indicates that the plant is has less retirement risk, which is due to
# it being a multi-unit plant in a non-restructured market, or due to
# requesting a license to operate to 80 years.

nukebins = pd.read_csv(os.path.join('Inputs','NuclearBins.csv'))
#nukebins = pd.read_csv(os.path.join('/Users/apham/Documents/GitHub/ReEDS_Input_Processing/NEMS_database_processing/Inputs','NuclearBins.csv'))

nukebins.rename(columns={'PLANT_NAME':'T_PNM'}, inplace=True)

# Remove duplicated values based on plant names
nukebins_short = nukebins[~nukebins.duplicated(['T_PNM'])][['T_PNM','tech','reeds_ba','NukeRetireBin']]

df = dfin.merge(nukebins_short, on = ['T_PNM','tech','reeds_ba'], how = 'left')

df2 = set_retire_years(df,coal_plant_retirement,current_year)

df2b = fix_upgrades(df2)

df3 = merge_psh_dbs(df2b,hydro_prjtype,ornl_hydro_unit_ver)

# Set California Batteries to have a 4-hour duration.  All others have a 2-hour duration.
index_battery_CA = (df3['tech'] == 'battery') & (df3['TSTATE'] == 'CA')
index_battery_notCA = (df3['tech'] == 'battery') & (df3['TSTATE'] != 'CA')
df3.loc[index_battery_notCA,'tech'] = 'battery_li'
df3.loc[index_battery_CA,'tech'] = 'battery_li'

#%% Map water techs to those that do not have any

# Gather as many water techs from the original database as possible
dfold = pd.read_csv(os.path.join('Inputs','Inheritance',gdboldname), low_memory=False)
dfold['merge_id'] = dfold['T_PID'].astype(str).str.strip() + "_" + dfold['T_UID'].astype(str).str.strip()
dfold = dfold[['merge_id','ctt','wst']].copy()
dfold_ct = dfold.groupby(by = 'merge_id').first().reset_index()

# create a list of plant names from the old database
existing_plants = list(dfold['merge_id'].unique())

# Find rows with missing cooling tech data
missing_ct = df3['ctt'].isnull()

df3['merge_id'] = df3['T_PID'].astype(str).str.strip() + "_" + df3['T_UID'].astype(str).str.strip()

# Subest the data for plants that existed in the old database but don't have a cooling tech assigned
df_noCT = df3[missing_ct & df3['merge_id'].isin(existing_plants)][['Unique ID','merge_id']]

# Merge the old data onto the new data
temp = pd.merge(df_noCT, dfold_ct, how = 'left', on = 'merge_id')
temp.drop('merge_id', inplace = True, axis = 1)
df3.drop('merge_id', inplace = True, axis = 1)

# Merge this new mapping onto the new database
df4 = pd.merge(df3, temp, how = 'left', on = 'Unique ID')

# Re-find the rows with missing cooling tech data
missing_ct = df4['ctt_x'].isnull()

# Assign cooling tech data from the old database to the new database and drop merged rows
df4.loc[missing_ct,'ctt_x'] = df4.loc[missing_ct,'ctt_y']
df4.loc[missing_ct,'wst_x'] = df4.loc[missing_ct,'wst_y']
df4.drop(['ctt_y','wst_y'], inplace = True, axis = 1)
df4.rename(columns={'ctt_x':'ctt', 'wst_x':'wst'}, inplace=True)

tech_ct_map = pd.read_csv(os.path.join('Inputs','tech_to_cooling_tech_map.csv'))
tech_ct_map['tech'] = tech_ct_map['tech'].str.lower()

# Find the rows with missing cooling tech data
missing_ct = df4['ctt'].isnull()

# Merge on default cooling tech mapping
df5 = pd.merge(df4, tech_ct_map, how = 'left', on = 'tech')

# Assign cooling tech data from the default mapping to the dataframe and drop merged rows
df5.loc[missing_ct,'ctt_x'] = df5.loc[missing_ct,'ctt_y']
df5.loc[missing_ct,'wst_x'] = df5.loc[missing_ct,'wst_y']
df5.drop(['ctt_y','wst_y'], inplace = True, axis = 1)
df5.rename(columns={'ctt_x':'ctt', 'wst_x':'wst'}, inplace=True)

df5.loc[df5['tech'].str.contains('battery'), 'HeatRate'] = 0
df5.loc[(df5['tech']== 'hydED') | (df5['tech']== 'hydEND') | (df5['tech']== 'hydNPND') | (df5['tech']== 'hydND'), 'ctt'] = 'n'
df5.loc[(df5['tech']== 'hydED') | (df5['tech']== 'hydEND') | (df5['tech']== 'hydNPND') | (df5['tech']== 'hydND'), 'wst'] = 'fsa'
df5['coolingwatertech'] = df5['tech'] + '_' + df5['ctt'] + '_' + df5['wst']

#%% Assign missing FOM/VOM data based on similar units

## This step prevents runs with high or unit-level hintage bins from throwing errors in 
## check_inputs.py due to missing FOM/VOM data
if output_changes:
    df6 = df5.copy()
    print('Locating units with missing FOM/VOM data:\n')
    noFVOM = df6.loc[
        ((~df6['HeatRate'].isnull()) & (df6['HeatRate']>0))
        & ((df6['T_FOM']==0) | (df6['T_VOM']==0) | (df6['T_FOM'].isna()) | (df6['T_VOM'].isna()))
        ]
    fixedFVOM = noFVOM.copy()
    closest_index_df = pd.DataFrame(
        index=noFVOM.index.copy(),
        columns=['from_index_VOM','from_index_FOM']
        )
    for i,row in noFVOM.iterrows():
        # First, find units of similar tech, region:
        print(f'[{i: >5}]   [{row["tech"]}] {row["T_PNM"]}')
        df_i = df6.loc[df6['tech'] == row['tech']]        
        df_ir = df6.loc[(df6['tech'] == row['tech']) & (df6['reeds_ba'] == row['reeds_ba'])]
        ## Fix FOM first, then VOM
        for OM in ['FOM','VOM']:
            T_OM = f'T_{OM}'
            if (row[T_OM] == 0) | (pd.isnull(row[T_OM])):
                ### If there are no units in the target unit's region with FOM/VOM data, search across all regions
                df_use = df_i.copy() if ((sum(df_ir[T_OM] == 0)) | (df_ir[T_OM].isnull().all())) else df_ir.copy()
                ### Only include units that have FOM/VOM data
                df_use = df_use.loc[df_use[T_OM]>0]
                ### Second, find unit closest in Capacity and HeatRate to the target unit using
                ### a distance formula
                ref = noFVOM.loc[i,['summer_power_capacity_MW','HeatRate']].copy()
                dist = (df_use
                        ### Calculate for every row except the target unit (target unit would
                        ### otherwise be calculated with a "distance" == 0 since exact match)
                        .loc[~df_use.index.isin([i]),['summer_power_capacity_MW','HeatRate']]
                        .apply(lambda unit: np.linalg.norm(unit - ref), axis=1)
                        )
                ### Find index of "closest" unit i.e. lowest distance. If there is a tie for 
                ### lowest distance, the first index is selected.
                closest_index = dist.idxmin()
                closest_uniqueID = df_use.loc[closest_index,'Unique ID']
                closest_PNM = df_use.loc[closest_index,'T_PNM']
                closest_OM = df_use.loc[closest_index, T_OM]
                print(f'            └→{OM}: Using [{closest_index}]: {closest_PNM} data == {closest_OM}')
                ### Third, apply FOM/VOM of closest unit to the target unit
                fixedFVOM.loc[i,T_OM] = df_use.loc[closest_index,T_OM]
                ### Lastly, collect Unique ID of "closest unit" 
                closest_index_df.loc[i,f'{OM}_from_Unique_ID'] = closest_uniqueID
            else:
                # No fix necesary for FOM/VOM - just 
                closest_index_df.loc[i,f'{OM}_from_Unique_ID'] = ''
                
    # Replace data in df6 with fixed FOM/VOM data
    df6.loc[fixedFVOM.index] = fixedFVOM.copy()

    # Check that no more units require FOM/VOM assignments.
    # If units with missing FOM/VOM exist, quit script and output list of units 
    checkFVOM = df6.loc[
        ((~df6['HeatRate'].isnull()) & (df6['HeatRate']>0))
        & ((df6['T_FOM']==0) | (df6['T_VOM']==0) | (df6['T_FOM'].isna()) | (df6['T_VOM'].isna()))
        ]
    if len(checkFVOM):
        print('Some units requiring FOM/VOM assignments still exist. Printing list '
            'of units out to Outputs/debug_fix_FOM_VOM.csv')
        checkFVOM.to_csv(os.path.join('Outputs','debug_fix_FOM_VOM.csv'))
        sys.exit()
    else:
        if os.path.exists(os.path.join('Outputs','debug_fix_FOM_VOM.csv')):
            os.remove(os.path.join('Outputs','debug_fix_FOM_VOM.csv'))

# Output changes to FOM/VOM if desired:
if output_changes:
    changes = fixedFVOM[['T_VOM','T_FOM']]
    changes.columns = ['new_VOM','new_FOM']
    dfout_OMchange = df5.loc[noFVOM.index,['summer_power_capacity_MW','T_PID','T_UID','T_PNM','T_VOM','T_FOM']]
    dfout_OMchange = dfout_OMchange.rename(columns={'T_VOM':'old_VOM','T_FOM':'old_FOM'})
    ## Add new OM columns and mapping to closest unit via Unique ID
    dfout_OMchange = pd.concat([dfout_OMchange,changes,closest_index_df],axis=1)
    dfout_OMchange = dfout_OMchange[['summer_power_capacity_MW','T_PID','T_UID','T_PNM',
                                     'old_VOM','new_VOM','VOM_from_Unique_ID',
                                     'old_FOM','new_FOM','FOM_from_Unique_ID']]
    dfout_OMchange.to_csv(os.path.join('Outputs','debug_OM_changes.csv'),
                          index=True)
    
    dfout = df6.copy()
else:
    dfout = df5.copy()
#%%
## Further clean up

# Format years and region ans integers:
dfout['StartYear'] = dfout['StartYear'].astype(int)
dfout['RetireYear'] = dfout['RetireYear'].astype(int)
dfout['StartYear2'] = dfout['StartYear']
dfout['TRFURB'] = dfout['TRFURB'].fillna(dfout.pop('StartYear2')).astype(int)
dfout['NukeRefRetireYear'] = dfout['NukeRefRetireYear'].astype(int)
dfout['Nuke60RetireYear'] = dfout['Nuke60RetireYear'].astype(int)
dfout['Nuke80RetireYear'] = dfout['Nuke80RetireYear'].astype(int)
dfout['NukeEarlyRetireYear'] = dfout['NukeEarlyRetireYear'].astype(int)

dfout['TCOUNT'] = dfout['TCOUNT'].fillna(1)
dfout['nems'] = dfout['nems'].fillna(0)
dfout['eia860'] = dfout['eia860'].fillna(0)

# Replace all battery tech's HR with 0:
dfout.loc[dfout['tech'].str.contains('battery'), 'HeatRate'] = np.nan
dfout['HeatRate'] = dfout['HeatRate'].replace({'0':np.nan, 0:np.nan})

# Replace all geothermal tech with geohydro_allkm:
dfout.loc[dfout['tech'].str.contains('geothermal'), 'tech'] = 'geohydro_allkm'

# Clean up columns names:
dfout.rename(columns={'nems': 'in_nems', 'eia860': 'in_eia860M'}, inplace=True)
dfout['in_nems'] = dfout['in_nems'].astype(int)
dfout['in_eia860M'] = dfout['in_eia860M'].astype(int)

# Replace all character '#' in T_PNM and T_UID as 'no. '
dfout['T_PNM'] = dfout['T_PNM'].str.replace('#', 'no. ')
dfout['T_UID'] = dfout['T_UID'].str.replace('#', 'no. ')

# Remove reeds_ba & resource_region columns:
dfout.drop('reeds_ba', inplace = True, axis = 1)
dfout.drop('resource_region', inplace = True, axis = 1)

# Reorder columns:
dfout_column_list = dfout.columns.tolist()
dfout_energy_cap = dfout_column_list.index('energy_capacity_MWh')
dfout_power_cap = dfout_column_list.index('summer_power_capacity_MW')
dfout_column_list.insert(dfout_power_cap + 1, dfout_column_list.pop(dfout_energy_cap))
dfout_bat_duration = dfout_column_list.index('battery_duration')
dfout_column_list.insert(dfout_power_cap + 2, dfout_column_list.pop(dfout_bat_duration))
dfout = dfout[dfout_column_list]

dfout.to_csv(os.path.join('Outputs',gdbfinalname),index=False)
