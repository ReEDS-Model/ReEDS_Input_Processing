"""
by apham
Updated Dec 1 2025

This script cleans the original generator fleet data from AEO (latest version used is PLTF860_RDB.xlsx) and merges it with EIA860M,
before using it to create 'ReEDS_generator_database_final_EIA-NEMS.csv' in the next few steps.
"""

import sys
import os
import pandas as pd
import numpy as np

def params():
    dir = os.getcwd()                                                                   # Main directory                                            

    # Key parameters:
    eia860M_ver_mon = sys.argv[1]                                                       # Most recent EIA 860M version month
    eia860M_ver_year = int(sys.argv[2])                                                 # Most recent EIA 860M version year
    nems_ver = int(sys.argv[3])                                                         # NEMS version
    battery_duration = float(sys.argv[4])                                                     

    # For debugging:
    #eia860M_ver_mon = 'october'                                                      
    #eia860M_ver_year = 2025                                               
    #nems_ver = 2023
    #battery_duration = 2.9

    append_operating_units = True                                                       # Append missing operating units from EIA860 to NEMS (True) or not (False)

    gdbinputname = 'a_to_b.csv'
    gdboutname   = 'b_to_c.csv'

    return (dir, nems_ver, battery_duration, eia860M_ver_mon, eia860M_ver_year, append_operating_units, gdbinputname, gdboutname)

def main():
    (dir, nems_ver, battery_duration, eia860M_ver_mon, eia860M_ver_year, append_operating_units, gdbinputname, gdboutname) = params()

    # Add EIA860M planned units, missing operating units, and updated retirement years to NEMS dataset:
    nems_cleaned = processAEOandEIA860(dir, nems_ver, battery_duration, eia860M_ver_mon, eia860M_ver_year, append_operating_units, gdbinputname)
        
    # Rename all "pv" to "upv" and "geothermal" to "geohydro_allkm":
    nems_cleaned.loc[(nems_cleaned['tech'] == 'pv'), 'tech'] = 'upv'
    nems_cleaned.loc[(nems_cleaned['tech'] == 'geothermal'), 'tech'] = 'geohydro_allkm'     

    # =========================================================================
    # Save output file:
    nems_cleaned.to_csv(os.path.join(dir,'Outputs', gdboutname), index=False)
    # =========================================================================

################################### MAIN FUNCTION ###################################
# This function does the following:
## 1. Add existing operating units in EIA 860M that are missing in NEMS to NEMS
## 2. Update retirement years of units in NEMS to reflect most current retirement 
## years in EIA 860M
## 3. Add planned units in EIA 860M to NEMS. Only planned units with these planning 
## statuses below are added:
#### i.   (V) Under construction, more than 50 percent complete
#### ii.  (U) Under construction, less than or equal to 50 percent complete
#### iii. (TS) Construction complete, but not yet in commercial operation
#####################################################################################
def processAEOandEIA860(dir, nems_ver, battery_duration, eia860M_ver_mon, eia860M_ver_year, append_operating_units, gdbinputname):

    # Read raw AEO file
    aeo_data = cleanAEOData(dir, gdbinputname)
    
    # =========================================================================
    ## 1. Add existing operating units in EIA 860M that are missing in NEMS to NEMS
    eia860M_data_operating = cleanEIA860MData(dir, eia860M_ver_mon, eia860M_ver_year, battery_duration, status='Operating')
    #eia860M_data = eia860M_data[eia860M_data['T_SYR'] >=nems_ver+1]
    # Merge current NEMS and operating EIA860M:
    nems_eia860_operating = mergeAEOandEIA860M(aeo_data, eia860M_data_operating,battery_duration,status='Operating')
    # Save temp output file:
    nems_eia860_operating.to_csv(os.path.join(dir,'Outputs', 'merged_nems_eia860_operating.csv'), index=False)
    # =========================================================================
    
    # =========================================================================
    ## 2. Update retirement years of units in NEMS to reflect most current retirement
    eia860M_data_retired = cleanEIA860MData(dir, eia860M_ver_mon, eia860M_ver_year, battery_duration, status='Retired')
    # Merge current NEMS and retired EIA860M:
    eia860M_data_retired['Status'] = '(R) Retired'
    nems_eia860_operating_retired = mergeAEOandEIA860M(nems_eia860_operating, eia860M_data_retired, battery_duration, status='Retired')
    nems_eia860_operating_retired = nems_eia860_operating_retired[nems_eia860_operating_retired['nems']==1]
    # Save temp output file:
    nems_eia860_operating_retired.to_csv(os.path.join(dir,'Outputs', 'merged_nems_eia860_operating_retired.csv'), index=False)
    # =========================================================================
    
    # =========================================================================
    ## 3. Add planned units in EIA 860M to NEMS. Only planned units with these planning 
    # Add EIA860M planned units according to conditions (Planning Status = U, V or TS):
    eia860M_planned = cleanEIA860MData(dir, eia860M_ver_mon, eia860M_ver_year, battery_duration, status='Planned')
    # Merge current operating and retired NEMS and planned EIA860M:
    nems_eia860_operating_retired_planned = mergeAEOandEIA860M(nems_eia860_operating_retired, eia860M_planned, battery_duration, status='Planned')
    # Save temp output file:
    nems_eia860_operating_retired_planned.to_csv(os.path.join(dir,'Outputs', 'merged_nems_eia860_operating_retired_planned.csv'), index=False)
    # =========================================================================
    
    # =========================================================================
    ## 4. Clean up final merged nems_eia file
    nems_eia860_operating_retired_planned_cleaned = cleanMergedAEOEIA860(nems_eia860_operating_retired_planned, battery_duration)
    # =========================================================================
    
    return nems_eia860_operating_retired_planned_cleaned

################################### CLEANING FUNCTIONS ##############################
# These next two functions clean up the raw EIA 860M data and the NEMS data inherited 
# to get from previous step them ready to be merged with each other
#####################################################################################  

def cleanAEOData(dir, gdbinputname):
    aeo_data = pd.read_csv(os.path.join(dir,'Outputs',gdbinputname))
    aeo_data = aeo_data.astype({'T_PID':'string','T_UID':'string', 'T_SYR': 'int', 'T_RYR': 'int'})
    aeo_data['T_PID'] = aeo_data['T_PID'].str.replace(" ", "")
    aeo_data['T_UID'] = aeo_data['T_UID'].str.replace(" ", "")
    # Add a 'battery_duration' column
    aeo_data['battery_duration'] = pd.Series(np.nan, dtype=float, index=aeo_data.index)

    aeo_data['nems'] = 1
    return aeo_data
  
def cleanEIA860MData(dir, ver_mon, ver_year, battery_duration, status):
    eia860M_data = pd.read_excel(os.path.join(dir,'Inputs','EIA860M',ver_mon+'_generator'+str(ver_year)+'.xlsx'), 
                                 sheet_name=status, header=1, index_col=False)
    if ver_year >=2020:
        eia860M_data.columns = eia860M_data.iloc[0]
        eia860M_data = eia860M_data[1:]
    
    # Dropping last 2 rows as they are notes
    eia860M_data.drop(eia860M_data.tail(2).index,inplace = True)

    # Convert nan capacity values to float type:
    eia860M_data['Net Summer Capacity (MW)'] = eia860M_data['Net Summer Capacity (MW)'].replace(r'^\s*$', np.nan, regex=True)
    eia860M_data['Net Summer Capacity (MW)'] = eia860M_data['Net Summer Capacity (MW)'].astype(float)
    eia860M_data['Net Winter Capacity (MW)'] = eia860M_data['Net Winter Capacity (MW)'].replace(r'^\s*$', np.nan, regex=True)
    eia860M_data['Net Winter Capacity (MW)'] = eia860M_data['Net Winter Capacity (MW)'].astype(float)
    eia860M_data['Nameplate Capacity (MW)'] = eia860M_data['Nameplate Capacity (MW)'].replace(r'^\s*$', np.nan, regex=True)
    eia860M_data['Nameplate Capacity (MW)'] = eia860M_data['Nameplate Capacity (MW)'].astype(float)
    if (status != 'Planned'):
        eia860M_data['Nameplate Energy Capacity (MWh)'] = eia860M_data['Nameplate Energy Capacity (MWh)'].replace(r'^\s*$', np.nan, regex=True)
        eia860M_data['Nameplate Energy Capacity (MWh)'] = eia860M_data['Nameplate Energy Capacity (MWh)'].astype(float)
    
    # Assuming all planned batteries have duration defined in battery_duration:
    if (status == 'Planned'):
        eia860M_data.loc[((eia860M_data['Technology']=='Batteries') |
                          (eia860M_data['Technology']=='Flywheels') |
                          (eia860M_data['Technology']=='Natural Gas with Compressed Air Storage')),
                          'Nameplate Energy Capacity (MWh)'] = eia860M_data['Net Summer Capacity (MW)'] * battery_duration

    # Assign energy capacity to battery units that have missing energy capacity values:
    eia860M_data.loc[(((eia860M_data['Technology']=='Batteries') |
                          (eia860M_data['Technology']=='Flywheels') |
                          (eia860M_data['Technology']=='Natural Gas with Compressed Air Storage') |
                          (eia860M_data['Technology']=='Hydroelectric Pumped Storage')) &
                          (eia860M_data['Nameplate Energy Capacity (MWh)'].isna())),
                          'Nameplate Energy Capacity (MWh)'] = eia860M_data['Net Summer Capacity (MW)'] * battery_duration
    
    eia860M_data['Battery Duration'] = eia860M_data['Nameplate Energy Capacity (MWh)']/eia860M_data['Net Summer Capacity (MW)']
    eia860M_data['Battery Duration'] = eia860M_data['Battery Duration'].round(2)
    eia860M_data = eia860M_data[eia860M_data['Plant ID'].notna()]
    eia860M_data = eia860M_data[(eia860M_data['Plant State'] != 'AK') & (eia860M_data['Plant State'] != 'HI') ]
    eia860M_data = eia860M_data[(eia860M_data['Sector'] == 'Electric Utility') | 
                                (eia860M_data['Sector'] == 'IPP CHP') | 
                                (eia860M_data['Sector'] == 'IPP Non-CHP') ]
    
    # Matching some columns' names with those in the AEO for merging later:
    eia860M_data = eia860M_data.rename({'County': 'county', 'Plant State': 'state', 'Balancing Authority Code': 'T_PCA'}, axis=1)
    if status == 'Operating':
        eia860M_data = eia860M_data.rename({'Planned Retirement Year': 'T_RYR_EIA860', 'Operating Year': 'T_SYR_EIA860'}, axis=1)
        eia860M_data['T_RYR_EIA860'] = eia860M_data['T_RYR_EIA860'].replace(r'^\s*$', np.nan, regex=True)
        eia860M_data.loc[eia860M_data['T_RYR_EIA860'].isna(),'T_RYR_EIA860'] = 9999
    elif status == 'Planned':
        eia860M_data = eia860M_data.rename({'Planned Operation Year': 'T_SYR_EIA860'}, axis=1)
        eia860M_data['T_RYR_EIA860'] = 9999
    elif status == 'Retired':
        eia860M_data = eia860M_data.rename({'Retirement Year': 'T_RYR_EIA860', 'Operating Year': 'T_SYR_EIA860'}, axis=1)
        eia860M_data['T_RYR_EIA860'] = eia860M_data['T_RYR_EIA860'].replace(r'^\s*$', np.nan, regex=True)

    eia860M_data = eia860M_data.astype({'county':'string','state':'string'})
    eia860M_data.county = eia860M_data.county.str.strip()
    eia860M_data.state = eia860M_data.state.str.strip()
    eia860M_data["county"] = eia860M_data["county"].astype(str) + " County"
    eia860M_data = eia860M_data.reset_index(drop=True)

    # Add techs to match with NEMS:
    eia860M_data['tech'] = eia860M_data['Technology']
    eia860M_data['tech'] = 'others'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Batteries", na=False),'tech'] = 'battery_li'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Coal Integrated Gasification Combined Cycle", na=False),'tech'] = 'coal-igcc'
    eia860M_data.loc[(eia860M_data['Technology'].str.contains("Conventional Steam Coal", na=False)) &
                     (eia860M_data['T_SYR_EIA860']<=1969),'tech'] = 'coalolduns'
    eia860M_data.loc[(eia860M_data['Technology'].str.contains("Conventional Steam Coal", na=False)) &
                     (eia860M_data['T_SYR_EIA860']>1969),'tech'] = 'coaloldscr'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Conventional Hydroelectric", na=False),'tech'] = 'hydro'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Hydroelectric Pump Storage", na=False),'tech'] = 'pumped-hydro'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Geothermal", na=False),'tech'] = 'geothermal'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Offshore Wind", na=False),'tech'] = 'wind-ofs'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Onshore Wind", na=False),'tech'] = 'wind-ons'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Solar Photovoltaic", na=False),'tech'] = 'pv'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Solar Thermal with Energy Storage", na=False),'tech'] = 'csp-ns'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Solar Thermal without Energy Storage", na=False),'tech'] = 'csp-ns'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Natural Gas Fired Combined Cycle", na=False),'tech'] = 'gas-cc'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Natural Gas Fired Combustion Turbine", na=False),'tech'] = 'gas-ct'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Natural Gas Internal Combustion Engine", na=False),'tech'] = 'gas-ct'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Natural Gas Steam Turbine", na=False),'tech'] = 'o-g-s'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Natural Gas with Compressed Air Storage", na=False),'tech'] = 'gas-ct'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Other Natural Gas", na=False),'tech'] = 'o-g-s'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Other Gases", na=False),'tech'] = 'o-g-s'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Petroleum", na=False),'tech'] = 'o-g-s'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Nuclear", na=False),'tech'] = 'nuclear'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Biomass", na=False),'tech'] = 'biopower'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Other Waste Biomass", na=False),'tech'] = 'biopower'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Wood/Wood Waste Biomass", na=False),'tech'] = 'biopower'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Municipal Solid Waste", na=False),'tech'] = 'lfill-gas'
    eia860M_data.loc[eia860M_data['Technology'].str.contains("Landfill", na=False),'tech'] = 'lfill-gas'

    # Add wst to match with NEMS:
    cooling_tech = pd.read_csv(os.path.join(dir,'Inputs','tech_to_cooling_tech_map.csv'))
    eia860M_data = pd.merge(eia860M_data, cooling_tech, on=['tech'], how='left')

    # Clean up:
    if status == 'Operating':
        eia860M_data = eia860M_data[(eia860M_data['Status'] == '(OP) Operating')
                                    #| (eia860M_data['Status'] == '(SB) Standby/Backup: available for service but not normally used') |
                                    #(eia860M_data['Status'] == '(OS) Out of service and NOT expected to return to service in next calendar year')
                                    ]
    elif status == 'Planned':
        eia860M_data = eia860M_data[(eia860M_data['Status'] == '(V) Under construction, more than 50 percent complete') |
                                    (eia860M_data['Status'] == '(U) Under construction, less than or equal to 50 percent complete') |
                                    (eia860M_data['Status'] == '(TS) Construction complete, but not yet in commercial operation')]
    elif status == 'Retired':
        eia860M_data['Status'] = '(R) Retired'
    
    eia860M_data['T_PID'] = eia860M_data['Plant ID']
    eia860M_data['T_UID'] = eia860M_data['Generator ID']
    
    eia860M_data = eia860M_data.astype({'T_PID':'string','T_UID':'string'})
    eia860M_data['T_PID'] = eia860M_data['T_PID'].str.replace(" ", "")
    eia860M_data['T_UID'] = eia860M_data['T_UID'].str.replace(" ", "")
    
    eia860M_data = eia860M_data.reset_index(drop=True)
    eia860M_data['eia860'] = 1

    return  eia860M_data

def mergeAEOandEIA860M(aeo_data, eia860M_data, battery_duration, status):
    if status != 'Retired':
        nems_eia860 = pd.merge(aeo_data, eia860M_data, on=['T_PID','T_UID'], how='outer')
    else:
        nems_eia860 = pd.merge(aeo_data, eia860M_data, on=['T_PID','T_UID'], how='left')
    
    if status == 'Operating':
        nems_eia860[['tech', 'county', 'ctt', 'wst', 'T_PCA']] = nems_eia860[['tech_x', 'county_x', 'ctt_x', 'wst_x', 'T_PCA_x']]
    else:
        nems_eia860[['tech', 'county', 'ctt', 'wst', 'T_PCA', 'eia860']] = nems_eia860[['tech_x', 'county_x', 'ctt_x', 'wst_x', 'T_PCA_x', 'eia860_x']]
        nems_eia860['eia860'] = nems_eia860['eia860'].fillna(nems_eia860.pop('eia860_y'))

    nems_eia860['tech'] = nems_eia860['tech'].fillna(nems_eia860.pop('tech_y'))
    nems_eia860['ctt'] = nems_eia860['ctt'].fillna(nems_eia860.pop('ctt_y'))
    nems_eia860['wst'] = nems_eia860['wst'].fillna(nems_eia860.pop('wst_y'))
    nems_eia860['county'] = nems_eia860['county'].fillna(nems_eia860.pop('county_y'))
    nems_eia860['T_PCA'] = nems_eia860['T_PCA'].fillna(nems_eia860.pop('T_PCA_y'))
    nems_eia860['T_SYR'] = nems_eia860['T_SYR'].fillna(nems_eia860.pop('T_SYR_EIA860'))
    nems_eia860['T_RYR'] = nems_eia860['T_RYR'].fillna(nems_eia860.pop('T_RYR_EIA860'))
    
    # Replacing capacities, operating and retirement dates in NEMS with those in EIA860M if the ones in EIA860M are not nan
    nems_eia860['TC_NP'] = np.where(nems_eia860['Nameplate Capacity (MW)'].notna(), nems_eia860['Nameplate Capacity (MW)'], nems_eia860['TC_NP'])
    nems_eia860['TC_SUM'] = np.where(nems_eia860['Net Summer Capacity (MW)'].notna(), nems_eia860['Net Summer Capacity (MW)'], nems_eia860['TC_SUM'])
    nems_eia860['TC_WIN'] = np.where(nems_eia860['Net Winter Capacity (MW)'].notna(), nems_eia860['Net Winter Capacity (MW)'], nems_eia860['TC_WIN'])
    nems_eia860['battery_duration'] = np.where(nems_eia860['Battery Duration'].notna(), nems_eia860['Battery Duration'], nems_eia860['battery_duration'])
    nems_eia860['T_PNM'] = np.where(nems_eia860['Plant Name'].notna(), nems_eia860['Plant Name'], nems_eia860['T_PNM'])
    nems_eia860['TSTATE'] = np.where(nems_eia860['state'].notna(), nems_eia860['state'], nems_eia860['TSTATE'])
    nems_eia860 = nems_eia860.drop(['Nameplate Capacity (MW)', 'Net Summer Capacity (MW)', 'Net Winter Capacity (MW)','Battery Duration', 'Plant Name', 'state'], axis=1)
    
    # Replacing lon/lat in NEMS with those in EIA860M
    nems_eia860['T_LAT'] = np.where(nems_eia860['Latitude'].notna(), nems_eia860['Latitude'], nems_eia860['T_LAT'])
    nems_eia860['T_LONG'] = np.where(nems_eia860['Longitude'].notna(), nems_eia860['Longitude'], nems_eia860['T_LONG'])
    nems_eia860 = nems_eia860.drop(['Latitude', 'Longitude'], axis=1)

    nems_eia860_final = nems_eia860[list(aeo_data.columns.values)].copy()

    if status == 'Operating':
        nems_eia860_final['status'] = nems_eia860['Status']
    nems_eia860_final.loc[(nems_eia860_final['status'].isna()),'status'] = nems_eia860['Status']
    if status != 'Planned':
        nems_eia860_final.loc[(nems_eia860_final['status'].isna()) &
                            (nems_eia860_final['T_RYR']>2024),'status'] = '(OP) Operating'
        nems_eia860_final.loc[(nems_eia860_final['status'].isna()) &
                            (nems_eia860_final['T_RYR']<=2024),'status'] = '(R) Retired'
        
    nems_eia860_final['nems'] = nems_eia860['nems'].fillna(0)
    nems_eia860_final['eia860'] = nems_eia860['eia860'].fillna(0)

    return nems_eia860_final

def addHeatrates(nems_eia860):
    # Add in heat rates for planned units (AEO inputs):
    nems_eia860.loc[(nems_eia860['nems']!=1),'THRATE'] = 0
    nems_eia860.loc[(nems_eia860['nems']!=1) & (nems_eia860['tech']=='biopower'),'THRATE'] = 13500
    nems_eia860.loc[(nems_eia860['nems']!=1) & (nems_eia860['tech']=='coal-igcc'),'THRATE'] = 8700
    nems_eia860.loc[(nems_eia860['nems']!=1) & (nems_eia860['tech']=='coal-new'),'THRATE'] = 8638
    nems_eia860.loc[(nems_eia860['nems']!=1) & (nems_eia860['tech']=='gas-cc'),'THRATE'] = 6400.5
    nems_eia860.loc[(nems_eia860['nems']!=1) & (nems_eia860['tech']=='gas-ct'),'THRATE'] = 9514.5
    nems_eia860.loc[(nems_eia860['nems']!=1) & (nems_eia860['tech']=='geothermal'),'THRATE'] = 8946
    nems_eia860.loc[(nems_eia860['nems']!=1) & (nems_eia860['tech']=='lfill-gas'),'THRATE'] = 8513
    nems_eia860.loc[(nems_eia860['nems']!=1) & (nems_eia860['tech']=='nuclear'),'THRATE'] = 10455
    nems_eia860.loc[(nems_eia860['nems']!=1) & (nems_eia860['tech']=='others'),'THRATE'] = 9271
    nems_eia860.loc[(nems_eia860['nems']!=1) & (nems_eia860['tech']=='o-g-s'),'THRATE'] = 9905
    nems_eia860.loc[(nems_eia860['nems']!=1) & (nems_eia860['tech']=='coalolduns'),'THRATE'] = 25000
    nems_eia860.loc[(nems_eia860['nems']!=1) & (nems_eia860['tech']=='coaloldscr'),'THRATE'] = 10344

    return nems_eia860

def cleanMergedAEOEIA860(merged_nems_eia860, battery_duration):
    # Correct capacity based on TCOUNT:
    merged_nems_eia860.loc[((merged_nems_eia860['TCOUNT'].isna()) & 
                           (merged_nems_eia860['nems']==0) & 
                           (merged_nems_eia860['eia860']==1)),'TCOUNT'] = 1
    
    # Some units are without capacity values, provide them with capacity values:
    merged_nems_eia860.loc[((merged_nems_eia860['T_PID'] == '7314') &
                           (merged_nems_eia860['T_UID'] == 'NA2') &
                           (merged_nems_eia860['TC_NP'].isna())),['TC_NP','TC_SUM','TC_WIN']] = [60, 49.4, 58.7]
    merged_nems_eia860.loc[((merged_nems_eia860['T_PID'] == '10725') &
                           (merged_nems_eia860['T_UID'] == 'GEN2') &
                           (merged_nems_eia860['TC_SUM'].isna())),
                           ['TC_SUM','TC_WIN']] = [12, 12]

    merged_nems_eia860['TC_NP'] = merged_nems_eia860['TC_NP'] * merged_nems_eia860['TCOUNT']
    merged_nems_eia860['TC_WIN'] = merged_nems_eia860['TC_WIN'] * merged_nems_eia860['TCOUNT']
    merged_nems_eia860['TC_SUM'] = merged_nems_eia860['TC_SUM'] * merged_nems_eia860['TCOUNT']

    merged_nems_eia860['TC_NP'] = merged_nems_eia860['TC_NP'].round(2)
    merged_nems_eia860['TC_WIN'] = merged_nems_eia860['TC_WIN'].round(2)
    merged_nems_eia860['TC_SUM'] = merged_nems_eia860['TC_SUM'].round(2)

    ## Further clean up
    # Add heat rate for EIA860M units:
    nems_eia860_final = addHeatrates(merged_nems_eia860)
    
    # Assign tech to missing tech values:
    techmap = pd.read_excel(os.path.join('Inputs','NEMS to ReEDS Tech Mapping.xlsx'))
    techmap.rename(columns={'EFD Code':'EFDcd', 'ReEDS Tech':'tech'}, inplace=True)
    nems_eia860_final = nems_eia860_final.merge(techmap, on=['EFDcd'], how='left')
    nems_eia860_final['tech'] = np.where(((nems_eia860_final['tech_x'].isnull()) | (nems_eia860_final['tech_x']=='others')),
                                            nems_eia860_final['tech_y'],nems_eia860_final['tech_x'])
    nems_eia860_final.drop(columns=['tech_x','tech_y'], inplace=True)
    
    # coal-new technologies are scrubbed coal units with an online data of 1995 of later
    coal_new_filter = (nems_eia860_final['tech'].isin(['coaloldscr'])) & (nems_eia860_final['TRFURB'] >= 1995)
    nems_eia860_final['tech'][coal_new_filter] = 'coal-new'
            
    # Assign energy capacity to batteries that are not in EIA860M but are in NEMS
    nems_eia860_final.loc[((nems_eia860_final['tech'].str.contains('battery')) | 
                           (nems_eia860_final['tech'].str.contains('pumped-hydro'))) & 
                          (nems_eia860_final['nems']==1) &
                          (nems_eia860_final['eia860']==0),'battery_duration'] = battery_duration
    nems_eia860_final = nems_eia860_final.reset_index(drop=True)

    # For units that are marked PV in EIA860 but DST (battery) in NEMS, consider them battery and and give them duration defined in battery_duration
    nems_eia860_final.loc[((nems_eia860_final['tech'].str.contains('battery')) |
                           (nems_eia860_final['tech'].str.contains('pumped-hydro'))) & 
                          (nems_eia860_final['nems']==1) &
                          (nems_eia860_final['eia860']==0) &
                          (nems_eia860_final['battery_duration'].isna()), 'battery_duration'] = battery_duration

    # Add energy capacity column:
    nems_eia860_final['energy_capacity_MWh'] = nems_eia860_final['battery_duration'] * nems_eia860_final['TC_SUM']

    ## For pvb units:
    # If pvb units have energy cap (assigned as batteries in EIA860), rename their tech as pvb_battery
    # If pvb units do not have energy cap (assigned as solar PV in EIA860), rename their tech as pvb_pv
    nems_eia860_final.loc[(nems_eia860_final['tech']=='pvb') & (~nems_eia860_final['battery_duration'].isna()), 'tech'] = 'pvb_battery'
    nems_eia860_final.loc[(nems_eia860_final['tech']=='pvb') & (nems_eia860_final['battery_duration'].isna()), 'tech'] = 'pvb_pv'

    return nems_eia860_final

main()
