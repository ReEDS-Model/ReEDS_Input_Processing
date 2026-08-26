"""
Updated Aug 1 2026

This script cleans the original generator fleet data from AEO and merges it with EIA860M,
"""

import sys
import os
import pandas as pd
import numpy as np

################################### MAIN FUNCTION ###################################
# This script does the following:
## 1. Process AEO-NEMS data
## 2. Process EIA860M files and append operating, planned, and retired EIA860M units
## 3. Merge AEO NEMS and EIA860M together. Only planned units in EIA860M with these 
# planning statuses below are merged:
#### i.   (V) Under construction, more than 50 percent complete
#### ii.  (U) Under construction, less than or equal to 50 percent complete
#### iii. (TS) Construction complete, but not yet in commercial operation
#####################################################################################

# Define parameters
def params():
    # Main directory
    dir = os.getcwd()                                                                   

    # Key parameters:
    #aeo_file = sys.argv[1]
    #eia860M_ver_mon = sys.argv[2]                                                 
    #eia860M_ver_year = int(sys.argv[3]) 
    #battery_duration = float(sys.argv[4])
    #current_year = int(sys.argv[5])                                                

    # For testing:
    aeo_file = 'PLTF860_RDB.xlsx'
    # Most recent EIA 860M version month
    eia860M_ver_mon = 'june'
    # Most recent EIA 860M version year                                                 
    eia860M_ver_year = 2026                                           
    battery_duration = 2.9
    current_year = 2026

    gdbinputname = aeo_file
    gdboutname   = 'a_to_b.csv'

    return (dir, current_year, battery_duration, eia860M_ver_mon, eia860M_ver_year, gdbinputname, gdboutname)

def processAEOandEIA860(dir, current_year, battery_duration, eia860M_ver_mon, eia860M_ver_year, gdbinputname):

    # =============================================================================================
    ## 1. Process AEO file
    (aeo_data, aeo_cols) = cleanAEOData(dir, current_year, gdbinputname)
    # =============================================================================================

    # =============================================================================================
    ## 2. Process EIA860M files and append operating, planned, and retired EIA860M units
    # Operating
    (eia860M_data_operating, eia_cols) = cleanEIA860MData(dir, current_year, eia860M_ver_mon, eia860M_ver_year, 
                                                          battery_duration, status='Operating')
    # Planned
    (eia860M_data_planned, eia_cols) = cleanEIA860MData(dir, current_year, eia860M_ver_mon, eia860M_ver_year, 
                                                        battery_duration, status='Planned')
    # Retired
    (eia860M_data_retired, eia_cols) = cleanEIA860MData(dir, current_year, eia860M_ver_mon, eia860M_ver_year, 
                                                        battery_duration, status='Retired')
    # Append them together
    eia860M_data = pd.concat([eia860M_data_operating,eia860M_data_planned,eia860M_data_retired],
                             ignore_index=True)
    # if summer capacity is missing, replace it with nameplate capacity
    eia860M_data.loc[eia860M_data['Net Summer Capacity (MW)'].isna(),
                     'Net Summer Capacity (MW)'] = eia860M_data['Nameplate Capacity (MW)']
    # =============================================================================================
    
    # =============================================================================================
    ## 3. Merge AEO NEMS and EIA860M together
    nems_eia860 = mergeAEOandEIA860M(aeo_data, eia860M_data, current_year, aeo_cols, eia_cols)

    # =========================================================================
    ## 4. Clean up final merged nems_eia file
    nems_eia860_operating_retired_planned_cleaned = cleanMergedAEOEIA860(aeo_data, nems_eia860, battery_duration)
    # =========================================================================
    
    return nems_eia860_operating_retired_planned_cleaned

################################### CLEANING FUNCTIONS ##############################
# These next two functions clean up the raw EIA 860M data and the NEMS data inherited 
# to get from previous step them ready to be merged with each other
#####################################################################################  

def cleanAEOData(dir, current_year, gdbinputname):
    aeo_data = pd.read_excel(os.path.join(dir,'inputs','aeo_nems',gdbinputname))
    aeo_data = aeo_data.astype({'T_PID':'string','T_UID':'string', 'T_SYR': 'int', 'T_RYR': 'int'})
    aeo_data['T_PID'] = aeo_data['T_PID'].str.replace(" ", "")
    aeo_data['T_UID'] = aeo_data['T_UID'].str.replace(" ", "")
    # Add a 'battery_duration' column
    aeo_data['battery_duration'] = pd.Series(np.nan, dtype=float, index=aeo_data.index)

    # Add techs to match with NEMS:
    aeo_data['tech'] = 'others'
    aeo_reeds_tech_map = pd.read_csv(os.path.join(dir,'inputs','tech_mappings','aeo_reeds_tech_map.csv'))
    aeo_data = aeo_data.merge(aeo_reeds_tech_map, on='EFDcd', how='left')

    # Specify scrubber or unscrubber coal units
    aeo_data.loc[aeo_data['reeds_tech'].notna(), 'tech'] = aeo_data['reeds_tech']
    aeo_data.loc[(aeo_data['tech'].str.contains("coal", na=False)) &
                 (aeo_data['T_SYR']<=1969),'tech'] = 'coalolduns'
    aeo_data.loc[(aeo_data['tech'].str.contains("coal", na=False)) &
                 (aeo_data['T_SYR']>1969),'tech'] = 'coaloldscr'

    # Add wst to match with NEMS:
    cooling_tech = pd.read_csv(os.path.join(dir,'inputs','tech_mappings', 
                                            'tech_to_cooling_tech_map.csv'))
    aeo_data = aeo_data.merge(cooling_tech, on=['tech'], how='left')
    aeo_data = aeo_data.drop(columns='reeds_tech')
    aeo_data['status'] = '(OP) Operating'
    aeo_data.loc[aeo_data['T_RYR']<current_year,'status'] = '(R) Retired'
    aeo_data['sector'] = 'Electric Utility'
    aeo_data['nems'] = 1

    aeo_cols = ['T_PID','T_UID','tech','ctt','wst','T_SYR','T_RYR','THRATE','TCOUNT',
                'TC_SUM','TC_NP','TC_WIN','battery_duration','EFDcd','ECPcd',
                'T_CID','T_PNM','TVIN','T_PCA','TRFURB','T_VOM','T_FOM','T_SMO',
                'T_RMO','T_CCSROV','T_CCSF','T_CCSV','T_CCSHR','T_CAPAD','sector',
                'T_CCSCAPA','T_CCSLOC','T_LONG','T_LAT','status','nems']
    aeo_data = aeo_data[aeo_cols]

    # Handle units with multiple owners:
    # Set aside units with single owners
    aeo_data_single = aeo_data[aeo_data['TCOUNT']==1]
    # Units with multiple owners
    aeo_data_mult = aeo_data[aeo_data['TCOUNT']<1]
    
    # Collapse units that are shared across different owners into ones with single owners
    # First, retain the original online year for upgraded units
    aeo_data_mult_g = aeo_data_mult.groupby(['T_PID','T_UID','TVIN','T_CID'], as_index=False).first()
    # Then, collapse on T_PID and T_UID
    aeo_data_mult_g = aeo_data_mult_g.groupby(['T_PID','T_UID','TVIN'],
                                            as_index=False).agg(
                                                {'ctt':'first','wst':'first','THRATE':'mean','TC_SUM':'sum',
                                                 'TC_NP':'sum','TC_WIN':'sum','battery_duration':'mean','T_SYR':'min',
                                                 'T_RYR':'first','tech':'first','EFDcd':'first','ECPcd':'first',
                                                 'T_PNM':'first','T_PCA':'first','TRFURB':'first','T_VOM':'mean',
                                                 'T_FOM':'mean','T_SMO':'first','T_RMO':'first','T_CCSROV':'first',
                                                 'T_CCSF':'first','T_CCSV':'first','T_CCSHR':'first','T_CAPAD':'first',
                                                 'T_CCSCAPA':'first','T_CCSLOC':'first','sector':'first','TCOUNT':'sum',
                                                 'T_LONG':'first','T_LAT':'first','status':'first','nems':'first'})
    aeo_data_mult_g['TCOUNT'] = 1
    aeo_data_final = pd.concat([aeo_data_single,aeo_data_mult_g])
    return aeo_data_final, aeo_cols
  
def cleanEIA860MData(dir, current_year, ver_mon, ver_year, battery_duration, status):
    
    eia860M_data = pd.read_excel(os.path.join(dir,'inputs','eia860M',
                                              ver_mon+'_generator'+str(ver_year)+'.xlsx'), 
                                              sheet_name=status, header=1, index_col=False)
    # Check first row if it does not have any numeric value 
    # or it is empty, then drop it if that's the case
    if pd.to_numeric(eia860M_data.iloc[0], errors='coerce').isna().all():
        eia860M_data.columns = eia860M_data.iloc[0]
        eia860M_data = eia860M_data[1:]
    
    # Check last two rows if they are just note 
    # (do not have any numeric value), then drop it if that's the case
    if not eia860M_data.tail(2).map(lambda x: isinstance(x, (int, float)) and not pd.isna(x)).any().any():
        eia860M_data.drop(eia860M_data.tail(2).index,inplace = True)

    # Convert nan capacity values to float type:
    eia860M_data['Net Summer Capacity (MW)'] = eia860M_data['Net Summer Capacity (MW)'].replace(r'^\s*$', np.nan, regex=True)
    eia860M_data['Net Summer Capacity (MW)'] = eia860M_data['Net Summer Capacity (MW)'].astype(float)
    eia860M_data['Net Winter Capacity (MW)'] = eia860M_data['Net Winter Capacity (MW)'].replace(r'^\s*$', np.nan, regex=True)
    eia860M_data['Net Winter Capacity (MW)'] = eia860M_data['Net Winter Capacity (MW)'].astype(float)
    eia860M_data['Nameplate Capacity (MW)'] = eia860M_data['Nameplate Capacity (MW)'].replace(r'^\s*$', np.nan, regex=True)
    eia860M_data['Nameplate Capacity (MW)'] = eia860M_data['Nameplate Capacity (MW)'].astype(float)
    if status != 'Planned':
        eia860M_data['Nameplate Energy Capacity (MWh)'] = eia860M_data['Nameplate Energy Capacity (MWh)'].replace(r'^\s*$', np.nan, regex=True)
        eia860M_data['Nameplate Energy Capacity (MWh)'] = eia860M_data['Nameplate Energy Capacity (MWh)'].astype(float)
        if status == 'Operating':
            eia860M_data['Planned Repower Year'] = eia860M_data['Planned Repower Year'].replace(r'^\s*$', np.nan, regex=True)
            eia860M_data['Planned Repower Year'] = eia860M_data['Planned Repower Year'].astype('Int64')
    
    # Assuming all planned storage units have duration defined in battery_duration:
    storage_cats = ['Batteries','Flywheels',
                    'Natural Gas with Compressed Air Storage',
                    'Hydroelectric Pumped Storage']
    if status == 'Planned':
        eia860M_data.loc[eia860M_data['Technology'].isin(storage_cats),
                         'Nameplate Energy Capacity (MWh)'] = eia860M_data['Net Summer Capacity (MW)'] * battery_duration

    # Assign energy capacity to all (planned & operating) storage units that have missing values:
    eia860M_data.loc[(eia860M_data['Technology'].isin(storage_cats)) &
                     (eia860M_data['Nameplate Energy Capacity (MWh)'].isna()),
                     'Nameplate Energy Capacity (MWh)'] = eia860M_data['Net Summer Capacity (MW)'] * battery_duration
    
    eia860M_data['Battery Duration'] = eia860M_data['Nameplate Energy Capacity (MWh)']/eia860M_data['Net Summer Capacity (MW)']
    eia860M_data['Battery Duration'] = eia860M_data['Battery Duration'].round(2)
    eia860M_data = eia860M_data[eia860M_data['Plant ID'].notna()]

    # Only consider units in CONUS and in appropriate sectors
    eia860M_data = eia860M_data[(eia860M_data['Plant State'] != 'AK') & 
                                (eia860M_data['Plant State'] != 'HI') ]
    
    # Matching some columns' names with those in the AEO for merging later:
    eia860M_data = eia860M_data.rename({'Balancing Authority Code': 'T_PCA'}, axis=1)
    if status == 'Operating':
        eia860M_data = eia860M_data.rename({'Planned Retirement Year': 'T_RYR_EIA860', 
                                            'Operating Year': 'T_SYR_EIA860'}, axis=1)
        eia860M_data['T_SYR_EIA860'] = eia860M_data['T_SYR_EIA860'].replace(r'^\s*$', np.nan, regex=True)
        eia860M_data['T_RYR_EIA860'] = eia860M_data['T_RYR_EIA860'].replace(r'^\s*$', np.nan, regex=True)
        # If no retire year is given, give is 9999
        eia860M_data.loc[eia860M_data['T_RYR_EIA860'].isna(),'T_RYR_EIA860'] = 9999
        # Update start year if the unit is repowered at a later year
        eia860M_data['T_SYR_EIA860'] = np.where(eia860M_data['Planned Repower Year'].notna(),
                                                eia860M_data['Planned Repower Year'], 
                                                eia860M_data['T_SYR_EIA860'])
    elif status == 'Planned':
        eia860M_data = eia860M_data.rename({'Planned Operation Year': 'T_SYR_EIA860'}, axis=1)
        eia860M_data['T_SYR_EIA860'] = eia860M_data['T_SYR_EIA860'].replace(r'^\s*$', np.nan, regex=True)
        eia860M_data['T_RYR_EIA860'] = 9999
    elif status == 'Retired':
        eia860M_data = eia860M_data.rename({'Retirement Year': 'T_RYR_EIA860', 
                                            'Operating Year': 'T_SYR_EIA860'}, axis=1)
        eia860M_data['T_SYR_EIA860'] = eia860M_data['T_SYR_EIA860'].replace(r'^\s*$', np.nan, regex=True)
        eia860M_data['T_RYR_EIA860'] = eia860M_data['T_RYR_EIA860'].replace(r'^\s*$', np.nan, regex=True)

    eia860M_data = eia860M_data.reset_index(drop=True)

    # Add techs to match with NEMS:
    eia860M_data['tech'] = eia860M_data['Technology']
    eia860M_data['tech'] = 'others'
    eia_reeds_tech_map = pd.read_csv(os.path.join(dir,'inputs','tech_mappings',
                                                  'eia_reeds_tech_map.csv')).rename(columns={'eia_tech':'Technology'})
    eia860M_data = eia860M_data.merge(eia_reeds_tech_map, on='Technology', how='left')
    # Specify scrubber or unscrubber coal units
    eia860M_data.loc[eia860M_data['reeds_tech'].notna(), 'tech'] = eia860M_data['reeds_tech']
    eia860M_data.loc[(eia860M_data['Technology'].str.contains("Conventional Steam Coal", na=False)) &
                     (eia860M_data['T_SYR_EIA860']<=1969),'tech'] = 'coalolduns'
    eia860M_data.loc[(eia860M_data['Technology'].str.contains("Conventional Steam Coal", na=False)) &
                     (eia860M_data['T_SYR_EIA860']>1969),'tech'] = 'coaloldscr'

    # Add wst to match with NEMS:
    cooling_tech = pd.read_csv(os.path.join(dir,'inputs','tech_mappings', 
                                            'tech_to_cooling_tech_map.csv'))
    eia860M_data = pd.merge(eia860M_data, cooling_tech, on=['tech'], how='left')

    # Clean up:
    # Note: if operating units are out of service but are out of service or on standby, 
    # we still consider them operating and available for dispatch
    if status == 'Retired':
        eia860M_data['Status'] = '(R) Retired'
    
    eia860M_data = eia860M_data.rename(columns={'Plant ID':'T_PID','Generator ID':'T_UID'})
    eia860M_data = eia860M_data.astype({'T_PID':'string','T_UID':'string'})
    eia860M_data['T_PID'] = eia860M_data['T_PID'].str.replace(" ", "")
    eia860M_data['T_UID'] = eia860M_data['T_UID'].str.replace(" ", "")
    
    eia860M_data = eia860M_data.reset_index(drop=True)
    eia860M_data['eia860'] = 1

    eia_cols = ['T_PID','T_UID','Plant Name','T_PCA','Sector','Unit Code',
                'Nameplate Capacity (MW)','Net Summer Capacity (MW)',
                'Net Winter Capacity (MW)','Nameplate Energy Capacity (MWh)',
                'Technology','tech','reeds_tech', 'ctt', 'wst',
                'Energy Source Code','T_SYR_EIA860','T_RYR_EIA860',
                'Latitude', 'Longitude','Battery Duration','Status','eia860']

    eia860M_data_final = eia860M_data[eia_cols]
    return  eia860M_data_final, eia_cols

def mergeAEOandEIA860M(aeo_data, eia860M_data, current_year, aeo_cols, eia_cols):

    aeo_eia_cols = ['tech','TC_SUM','TC_NP','TC_WIN','T_RYR','T_SYR','THRATE',
                    'T_PID','T_UID','T_PNM','TVIN','EFDcd','ECPcd','T_PCA',
                    'TRFURB','T_VOM','T_FOM','T_SMO','T_RMO', 'T_CCSROV','T_CCSF',
                    'T_CCSV','T_CCSHR','T_CAPAD','T_CCSCAPA','T_CCSLOC',
                    'T_LONG','T_LAT','ctt','wst','nems','eia860',
                    'sector','status','Technology','battery_duration']
    nems_eia860 = pd.merge(aeo_data, eia860M_data, on=['T_PID','T_UID'], 
                           how='outer', indicator=True)

    # Units that are perfectly matched between two dataset
    nems_eia860_merged = nems_eia860[nems_eia860['_merge']=='both']
    nems_eia860_merged = nems_eia860_merged.rename(columns={'tech_x':'tech','ctt_x':'ctt', 
                                                            'wst_x':'wst','T_PCA_x':'T_PCA'})

    # Replacing capacities, operating and retirement dates in NEMS with those in EIA860M if the ones in EIA860M are not nan
    nems_eia860_merged['TC_NP'] = np.where(nems_eia860_merged['Nameplate Capacity (MW)'].notna(), 
                                           nems_eia860_merged['Nameplate Capacity (MW)'], 
                                           nems_eia860_merged['TC_NP'])
    nems_eia860_merged['TC_SUM'] = np.where(nems_eia860_merged['Net Summer Capacity (MW)'].notna(), 
                                            nems_eia860_merged['Net Summer Capacity (MW)'], 
                                            nems_eia860_merged['TC_SUM'])
    nems_eia860_merged['TC_WIN'] = np.where(nems_eia860_merged['Net Winter Capacity (MW)'].notna(), 
                                            nems_eia860_merged['Net Winter Capacity (MW)'], 
                                            nems_eia860_merged['TC_WIN'])
    nems_eia860_merged['battery_duration'] = np.where(nems_eia860_merged['Battery Duration'].notna(), 
                                                      nems_eia860_merged['Battery Duration'], 
                                                      nems_eia860_merged['battery_duration'])
    nems_eia860_merged['T_PNM'] = np.where(nems_eia860_merged['Plant Name'].notna(), 
                                           nems_eia860_merged['Plant Name'],
                                           nems_eia860_merged['T_PNM'])
    nems_eia860_merged['T_SYR'] = np.where(nems_eia860_merged['T_SYR_EIA860'].notna(),
                                           nems_eia860_merged['T_SYR_EIA860'],
                                           nems_eia860_merged['T_SYR'])
    nems_eia860_merged['T_RYR'] = np.where(nems_eia860_merged['T_RYR_EIA860'].notna(),
                                           nems_eia860_merged['T_RYR_EIA860'],
                                           nems_eia860_merged['T_RYR'])
    nems_eia860_merged['T_LAT'] = np.where(nems_eia860_merged['Latitude'].notna(), 
                                           nems_eia860_merged['Latitude'], 
                                           nems_eia860_merged['T_LAT'])
    nems_eia860_merged['T_LONG'] = np.where(nems_eia860_merged['Longitude'].notna(),
                                            nems_eia860_merged['Longitude'],
                                            nems_eia860_merged['T_LONG'])
    nems_eia860_merged['sector'] = np.where(nems_eia860_merged['Sector'].notna(),
                                            nems_eia860_merged['Sector'],
                                            nems_eia860_merged['sector'])
    nems_eia860_merged['status'] = np.where(nems_eia860_merged['Status'].notna(),
                                            nems_eia860_merged['Status'],
                                            nems_eia860_merged['status'])
    
    nems_eia860_merged = nems_eia860_merged[aeo_eia_cols].copy()

    # Filter out planned units that are still far from being able to go online ontime
    status_to_keep = ['(OP) Operating',
                      '(OS) Out of service and NOT expected to return to service in next calendar year',
                      '(OA) Out of service but expected to return to service in next calendar year',
                      '(SB) Standby/Backup: available for service but not normally used',
                      '(V) Under construction, more than 50 percent complete',
                      '(U) Under construction, less than or equal to 50 percent complete',
                      '(TS) Construction complete, but not yet in commercial operation',
                      '(R) Retired']
    nems_eia860_merged = nems_eia860_merged[nems_eia860_merged['status'].isin(status_to_keep)]

    # Only keep units in three sectors - 'Electric Utility', 'IPP CHP', and 'IPP Non-CHP'
    sector_to_keep = ['Electric Utility', 'IPP CHP', 'IPP Non-CHP']
    nems_eia860_merged = nems_eia860_merged[nems_eia860_merged['sector'].isin(sector_to_keep)]

    # Units that are unmatched need to be remerged
    nems_eia860_unmerged = nems_eia860[nems_eia860['_merge']!='both']
    nems_eia860_unmerged['status'] = np.where(nems_eia860_unmerged['Status'].notna(),
                                              nems_eia860_unmerged['Status'],
                                              nems_eia860_unmerged['status'])
    nems_eia860_unmerged = nems_eia860_unmerged[nems_eia860_unmerged['status'].isin(status_to_keep)]

    nems_eia860_unmerged['sector'] = np.where(nems_eia860_unmerged['Sector'].notna(),
                                                  nems_eia860_unmerged['Sector'],
                                                  nems_eia860_unmerged['sector'])
    nems_eia860_unmerged = nems_eia860_unmerged[nems_eia860_unmerged['sector'].isin(sector_to_keep)]

    # Units that are in AOE-NEMS or EIA860M only
    nems_only = nems_eia860_unmerged[nems_eia860_unmerged['nems']==1]
    eia860_only = nems_eia860_unmerged[nems_eia860_unmerged['eia860']==1]

    nems_only = nems_only.rename(columns={'tech_x':'tech','ctt_x':'ctt','wst_x':'wst','T_PCA_x':'T_PCA'})
    eia860_only = eia860_only.rename(columns={'tech_x':'tech','ctt_x':'ctt','wst_x':'wst','T_PCA_x':'T_PCA'})
    nems_only = nems_only[aeo_cols]
    eia860_only = eia860_only[eia_cols]
    eia860_only['tech'] = 'others'
    eia860_only.loc[eia860_only['reeds_tech'].notna(), 'tech'] = eia860_only['reeds_tech']

    # Aggregate unmatched units by T_PID, T_SYR, T_RYR, and TVIN
    nems_only = nems_only.groupby(['tech','T_PID','T_SYR','T_RYR','TVIN'], 
                                  as_index=False).agg({'ctt':'first','wst':'first','THRATE':'mean','TC_SUM':'sum',
                                                        'TC_NP':'sum','TC_WIN':'sum','battery_duration':'mean','T_UID':'first',
                                                        'EFDcd':'first','ECPcd':'first','T_CID':'first','T_PNM':'first',
                                                        'T_PCA':'first','TRFURB':'first','T_VOM':'mean','T_FOM':'mean',
                                                        'T_SMO':'first','T_RMO':'first','T_CCSROV':'first','T_CCSF':'first',
                                                        'T_CCSV':'first','T_CCSHR':'first','T_CAPAD':'first','T_CCSCAPA':'first',
                                                        'T_CCSLOC':'first','sector':'first','TCOUNT':'first',
                                                        'T_LONG':'first','T_LAT':'first','status':'first','nems':'first'})
    eia860_only = eia860_only.groupby(['Technology','T_PID','T_SYR_EIA860','T_RYR_EIA860'], 
                                      as_index=False).agg({'ctt':'first','wst':'first','Nameplate Capacity (MW)':'sum',
                                                           'Net Summer Capacity (MW)':'sum','Net Winter Capacity (MW)':'sum',
                                                           'Nameplate Energy Capacity (MWh)':'sum','Technology':'first',
                                                           'T_PCA':'first','Sector':'first','Unit Code':'first',
                                                           'tech':'first','reeds_tech':'first','Battery Duration':'mean',
                                                           'T_UID':'first','Energy Source Code':'first','Longitude':'first',
                                                           'Latitude':'first','Status':'first','eia860':'first', 'Plant Name':'first'})

    nems_eia_remerge = nems_only.merge(eia860_only,on=['tech','T_PID'],how='outer',indicator=True)

    nems_eia_remerge = nems_eia_remerge.rename(columns={'tech_x':'tech','T_UID_x':'T_UID',
                                                        'ctt_x':'ctt','wst_x':'wst','T_PCA_x':'T_PCA'})
    nems_eia_remerge['ctt'] = nems_eia_remerge['ctt'].fillna(nems_eia_remerge['ctt_y'])
    nems_eia_remerge['wst'] = nems_eia_remerge['wst'].fillna(nems_eia_remerge['wst_y'])
    nems_eia_remerge['T_UID'] = nems_eia_remerge['T_UID'].fillna(nems_eia_remerge['T_UID_y'])
    nems_eia_remerge['T_SYR'] = nems_eia_remerge['T_SYR'].fillna(nems_eia_remerge['T_SYR_EIA860'])
    nems_eia_remerge['T_RYR'] = nems_eia_remerge['T_RYR'].fillna(nems_eia_remerge['T_RYR_EIA860'])
    nems_eia_remerge['TC_NP'] = nems_eia_remerge['TC_NP'].fillna(nems_eia_remerge['Nameplate Capacity (MW)'])
    nems_eia_remerge['TC_SUM'] = nems_eia_remerge['TC_SUM'].fillna(nems_eia_remerge['Net Summer Capacity (MW)'])
    nems_eia_remerge['TC_WIN'] = nems_eia_remerge['TC_WIN'].fillna(nems_eia_remerge['Net Winter Capacity (MW)'])
    nems_eia_remerge['battery_duration'] = nems_eia_remerge['battery_duration'].fillna(nems_eia_remerge['Battery Duration'])
    nems_eia_remerge['T_PNM'] = nems_eia_remerge['T_PNM'].fillna(nems_eia_remerge['Plant Name'])
    nems_eia_remerge['T_LAT'] = nems_eia_remerge['T_LAT'].fillna(nems_eia_remerge['Latitude'])
    nems_eia_remerge['T_LONG'] = nems_eia_remerge['T_LONG'].fillna(nems_eia_remerge['Longitude'])
    nems_eia_remerge['status'] = nems_eia_remerge['status'].fillna(nems_eia_remerge['Status'])

    nems_eia_remerge = nems_eia_remerge[aeo_eia_cols].copy()
    nems_eia860_final = pd.concat([nems_eia860_merged, nems_eia_remerge],ignore_index=True)
    nems_eia860_final = nems_eia860_final.rename(columns={'Technology':'Description'})
    nems_eia860_final['nems'] = nems_eia860_final['nems'].fillna(0)
    nems_eia860_final['eia860'] = nems_eia860_final['eia860'].fillna(0)

    return nems_eia860_final

def addHeatrates(nems_eia860):
    # Add in heat rates for planned units (AEO inputs):
    
    nems_eia860.loc[(nems_eia860['THRATE'].isna()) & (nems_eia860['tech']=='biopower'),'THRATE'] = 13500
    nems_eia860.loc[(nems_eia860['THRATE'].isna()) & (nems_eia860['tech']=='coal-igcc'),'THRATE'] = 8700
    nems_eia860.loc[(nems_eia860['THRATE'].isna()) & (nems_eia860['tech']=='coal-new'),'THRATE'] = 8638
    nems_eia860.loc[(nems_eia860['THRATE'].isna()) & (nems_eia860['tech']=='gas-cc'),'THRATE'] = 6400.5
    nems_eia860.loc[(nems_eia860['THRATE'].isna()) & (nems_eia860['tech']=='gas-ct'),'THRATE'] = 9514.5
    nems_eia860.loc[(nems_eia860['THRATE'].isna()) & (nems_eia860['tech']=='geothermal'),'THRATE'] = 8946
    nems_eia860.loc[(nems_eia860['THRATE'].isna()) & (nems_eia860['tech']=='lfill-gas'),'THRATE'] = 8513
    nems_eia860.loc[(nems_eia860['THRATE'].isna()) & (nems_eia860['tech']=='nuclear'),'THRATE'] = 10455
    nems_eia860.loc[(nems_eia860['THRATE'].isna()) & (nems_eia860['tech']=='others'),'THRATE'] = 9271
    nems_eia860.loc[(nems_eia860['THRATE'].isna()) & (nems_eia860['tech']=='o-g-s'),'THRATE'] = 9905
    nems_eia860.loc[(nems_eia860['THRATE'].isna()) & (nems_eia860['tech']=='coalolduns'),'THRATE'] = 25000
    nems_eia860.loc[(nems_eia860['THRATE'].isna()) & (nems_eia860['tech']=='coaloldscr'),'THRATE'] = 10344
    nems_eia860.loc[nems_eia860['THRATE'].isna(),'THRATE'] = 0

    return nems_eia860

def cleanMergedAEOEIA860(aeo_orig, nems_eia860, battery_duration):
    # Handling upgrades
    nems_eia860['TVIN'] = nems_eia860['TVIN'].fillna(1)
    # Set aside units that are not upgrades
    nems_eia_non_upgrades = nems_eia860[nems_eia860['TVIN']<6]
    # For upgraded units, there are two rows in AEO-NEMS (one with TVIN=6 and one with TVIN=7), 
    # but there is only one row with original start year and no TVIN in EIA860M. So during
    # merging, EIA860M might override start years and retire years for these units.
    # As a result, here we put back the start years and retire years of these units to
    # original values in AEO-NEMS
    nems_eia_upgrades = nems_eia860[(nems_eia860['TVIN']==6)| (nems_eia860['TVIN']==7)]
    aeo_orig = aeo_orig[(aeo_orig['TVIN']==6) | (aeo_orig['TVIN']==7)]
    aeo_orig = aeo_orig.rename(columns={'T_SYR':'T_SYR_aeo','T_RYR':'T_RYR_aeo'})
    aeo_orig = aeo_orig[['T_PID','T_UID','TVIN','T_SYR_aeo','T_RYR_aeo']]
    aeo_orig = aeo_orig.drop_duplicates()
    nems_eia_upgrades = nems_eia_upgrades.merge(aeo_orig, on=['T_PID','T_UID','TVIN'], how='left')
    nems_eia_upgrades[['T_SYR','T_RYR']] = nems_eia_upgrades[['T_SYR_aeo','T_RYR_aeo']]
    nems_eia_upgrades = nems_eia_upgrades.drop(columns=['T_SYR_aeo','T_RYR_aeo'])

    nems_eia860 = pd.concat([nems_eia_non_upgrades, nems_eia_upgrades])

    # Rounding
    rounding_cols = ['TC_NP', 'TC_WIN','TC_SUM','T_VOM','T_FOM',
                     'T_CCSROV','T_CCSF','T_CCSV','T_CCSHR','T_CAPAD']
    nems_eia860[rounding_cols] = nems_eia860[rounding_cols].round(2)

    ## Further clean up
    # Add heat rate for unmatched EIA860M units:
    nems_eia860_final = addHeatrates(nems_eia860)
    
    # coal-new technologies are scrubbed coal units with an online data of 1995 of later
    coal_new_filter = (nems_eia860_final['tech'].isin(['coaloldscr'])) & (nems_eia860_final['TRFURB'] >= 1995)
    nems_eia860_final.loc[coal_new_filter, 'tech'] = 'coal-new'
            
    # Assign energy capacity to batteries that are not in EIA860M but are in NEMS
    nems_eia860_final.loc[((nems_eia860_final['tech'].str.contains('battery')) | 
                           (nems_eia860_final['tech'].str.contains('pumped-hydro'))) & 
                          (nems_eia860_final['nems']==1) &
                          (nems_eia860_final['eia860']==0),'battery_duration'] = battery_duration
    nems_eia860_final = nems_eia860_final.reset_index(drop=True)

    # For units that are marked PV in EIA860 but DST (battery) in NEMS, consider them PV 
    nems_eia860_final.loc[((nems_eia860_final['EFDcd']=='DST') & 
                           (nems_eia860_final['Description'].str.contains('Solar'))),'tech'] = 'pv'
    
    # Add energy capacity column:
    nems_eia860_final['energy_capacity_MWh'] = nems_eia860_final['battery_duration'] * nems_eia860_final['TC_SUM']

    ## For pvb units:
    # If pvb units have energy cap (assigned as batteries in EIA860), rename their tech as pvb_battery
    # If pvb units do not have energy cap (assigned as solar PV in EIA860), rename their tech as pvb_pv
    nems_eia860_final.loc[(nems_eia860_final['tech']=='pvb') & (~nems_eia860_final['battery_duration'].isna()), 'tech'] = 'pvb_battery'
    nems_eia860_final.loc[(nems_eia860_final['tech']=='pvb') & (nems_eia860_final['battery_duration'].isna()), 'tech'] = 'pvb_pv'

    # Drop tech = 'others' since they are all Flywheels
    nems_eia860_final = nems_eia860_final[nems_eia860_final['tech']!='others']

    # Rename all "pv" to "upv" and "geothermal" to "geohydro_allkm":
    nems_eia860_final.loc[(nems_eia860_final['tech'] == 'pv'), 'tech'] = 'upv'
    nems_eia860_final.loc[(nems_eia860_final['tech'] == 'geothermal'), 'tech'] = 'geohydro_allkm'    

    return nems_eia860_final

if __name__ == "__main__":
    print("Starting a_data_cleaning.py")

    (dir, current_year, battery_duration, eia860M_ver_mon, eia860M_ver_year, gdbinputname, gdboutname) = params()

    # Add EIA860M planned units, missing operating units, and updated retirement years to NEMS dataset:
    nems_cleaned = processAEOandEIA860(dir, current_year, battery_duration, eia860M_ver_mon, eia860M_ver_year, gdbinputname) 

    # =========================================================================
    # Save output file:
    intermediate_output_path = os.path.join(dir,'outputs','intermediate_outputs')
    os.makedirs(intermediate_output_path, exist_ok=True)

    nems_cleaned.to_csv(os.path.join(intermediate_output_path, gdboutname), index=False)
    # =========================================================================

    print("Finished a_data_cleaning.py")
