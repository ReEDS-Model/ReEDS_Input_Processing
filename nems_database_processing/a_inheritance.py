# -*- coding: utf-8 -*-
"""
Created on Fri Apr  8 15:01:42 2022

This script creates a mapper of various data categories pulled from a previous 
version of the ReEDS generator database and maps this data to a new version
of the generator database. The map merges the data to the new database along 
plant ID and unit ID matches.

Steps:
    0: User Input and Data Specification
        a. Set names of csv files:
            - gdboldname: the old generator database to inherit data from
            - gdbnewname: the new generator database that will inherit old data
            - gdboutname: the updated generator database that will be passed to
                          b_geopy.py for further processing.
        b. COL_RENAME: Dictionary containing the columns that will be used to merge
                       the old database to the new database. Keys are the gdboldname
                       column names, and the values are the gdbnewname column names.
        c. colstomap: column names from the gdboldname that will be inherited by 
                      gdbnewname.
    1: Import the old dataframe
    2: Create the Data Mapper
    3: Import the new dataframe to be updated with data from old df
    4: Apply the mapper to the new dataframe
        a. Map old data to new dataframe. If new df row does not have lat/long 
           data, update with old lat/long
        b. Map counties to new dataframe using lat/long coordinates
            - This step was added to map county info for plants that may not 
              have old plant/unit ID data from old map, but do have lat/long 
              coordinates that match a plant that does have county info
        c. Merge FIPS codes using county column.
            - Now that most rows have county data, merge FIPS codes from old
              dataframe to new dataframe using county column
    5: Export the new updated dataframe
    
Note: If the columns in the new generator database (gdbnewname) are different
than the previous database (gdboldname), then it is possible that this script
will break. If that is the case, then check column names (e.g., the lat and
long columns) to find the name change.

@author: jcarag
"""
import pandas as pd
import numpy as np
import os
import sys

#%%----------------------------------------------------------------------------
# Step 0: User Input and Data Specification:
#------------------------------------------------------------------------------
current_fleet_yr = int(sys.argv[1])
gdbnewname = sys.argv[2]

# debugging:
#current_fleet_yr = 2024
#gdbnewname = 'PLTF860_RDB.xlsx'

gdboldname = 'ReEDS_generator_database_final_EIA-NEMS_' + str(current_fleet_yr) + '.csv'
gdboutname = 'a_to_b.csv'

COL_RENAME = {
        'PLANT_ID' : 'T_PID',
# We originally mapped on unit IDs, but the unit IDs changed from 2021 to 2022
# So we are mappy to the plant ID and technology code. We recommend that
# Future years revert to the unit ID
        'UNIT_ID'  : 'T_UID',
        'NEMS_TYPE'  : 'EFDcd',
        }

colstomap = ['FIPS','county','tech','coolingwatertech','ctt','wst','T_LAT','T_LONG']

mergeonlist = list(COL_RENAME.values())

#%%----------------------------------------------------------------------------
# SCRIPT STEP 1: Import the old dataframe:
#------------------------------------------------------------------------------

dfold = pd.read_csv(os.path.join('Inputs','Inheritance',gdboldname), low_memory=False)
dfold.rename(columns=COL_RENAME, inplace=True)

#---this dataframe collects all the PLANT_ID/UNIT_ID combinations that are
#---repeated in the old dataframe:
duplicates = dfold[dfold.duplicated(['T_PID','T_UID','EFDcd'])][['T_PID','T_UID','EFDcd','tech']]
#duplicates = dfold[dfold.duplicated(['T_PID','EFDcd'])][['T_PID','EFDcd','tech']]


#%%----------------------------------------------------------------------------
# SCRIPT STEP 2: Create the Data Mapper:
#------------------------------------------------------------------------------

data_map = dfold[mergeonlist + colstomap].copy()
data_map = data_map.drop_duplicates(subset=mergeonlist,keep='first').reset_index(drop=True)


#%%----------------------------------------------------------------------------
# SCRIPT STEP 3: Import the new dataframe:
#------------------------------------------------------------------------------

dfnew = pd.read_excel(os.path.join('Inputs','AEO_NEMS',gdbnewname))

#%%----------------------------------------------------------------------------
# SCRIPT STEP 4a: Apply the mapper to the new dataframe:
#------------------------------------------------------------------------------

dfout = dfnew.merge(data_map, on=mergeonlist, how='left')

#---Only map lat/long data if lat/long data is missing in the new database:
newlatcol = 'T_LAT_x'
newlongcol= 'T_LONG_x'

dfout[newlatcol]  = dfout[newlatcol].fillna(0)
dfout[newlongcol] = dfout[newlongcol].fillna(0)

for i, row in dfout.iterrows():
    if (row[newlatcol]==0) and (row['T_LAT_y']!=0):
        dfout.loc[i,newlatcol] = row['T_LAT_y']
    if (row[newlongcol]==0) and (row['T_LONG_y']!=0):
        dfout.loc[i,newlongcol] = row['T_LONG_y']

dfout = dfout.drop(columns=['T_LAT_y','T_LONG_y'])
dfout.rename(columns={'T_LAT_x':'T_LAT','T_LONG_x':'T_LONG'}, inplace = True)
        
#%%----------------------------------------------------------------------------
# SCRIPT STEP 4b: Create new mapper (maps county based on lat/long):
#------------------------------------------------------------------------------

# This step was added to map county info for plants that may not have old plant/
#    unit ID data from old map, but do have lat/long coordinates that match a 
#    plant that does have county info

# In 2022 the CT counties were renamed, so drop them from the mapping
if current_fleet_yr > 2021:
    data_map_2 = dfout.loc[(dfout['TSTATE'] != 'CT'),['county','T_LAT','T_LONG']]
    dfout.loc[dfout['TSTATE']=='CT','county'] = np.nan
    dfout.loc[dfout['TSTATE']=='CT','FIPS'] = np.nan
else:
    data_map_2 = dfout[['county','T_LAT','T_LONG']]
    
data_map_2 = data_map_2.loc[~data_map_2['county'].isna()]
data_map_2 = data_map_2.drop_duplicates(keep='first')

dfout_2 = dfout.merge(data_map_2, on=['T_LAT','T_LONG'], how='left')
dfout_2.rename(columns={'county_y':'county'},inplace=True)
dfout_2.drop(columns=['county_x'],inplace=True)

#%%----------------------------------------------------------------------------
# STEP 4c: Merge FIPS data using new county data column
#------------------------------------------------------------------------------

# data_map_3 maps FIPS to counties and states
data_map_3 = dfold[['FIPS','county','TSTATE']].copy()

# In 2022 the CT counties were renamed, so drop them from the mapping
if current_fleet_yr > 2021:
    data_map_3.loc[data_map_3['TSTATE']=='CT','county'] = np.nan
    data_map_3.loc[data_map_3['TSTATE']=='CT','FIPS'] = np.nan

data_map_3 = data_map_3.drop_duplicates()

duplicates_dm3 = data_map_3[data_map_3.duplicated(['county','TSTATE'])][['FIPS','county','TSTATE']]

dfout_3 = dfout_2.merge(data_map_3, on=['county','TSTATE'], how='left')
dfout_3.rename(columns={'FIPS_y':'FIPS'},inplace=True)
dfout_3.drop(columns=['FIPS_x'],inplace=True)

# rename battery_2,4 to continuous battery_li
dfout_3.loc[dfout_3['tech'].str.contains('battery_', na=False),'tech'] = 'battery_li'
#%%----------------------------------------------------------------------------
# SCRIPT STEP 5: Export the new dataframe:
#------------------------------------------------------------------------------
os.makedirs('Outputs', exist_ok=True)
dfout_3.to_csv(os.path.join('Outputs', gdboutname),index=False)
