# -*- coding: utf-8 -*-
"""
Created on Mon Mar 25 15:43:20 2019

@author: afrazier
"""

import pandas as pd
import numpy as np
import os

def set_retire_years(nems,coal_plant_retirement,current_year):

# =============================================================================
# Technology naming convention
# =============================================================================

    nems['tech'].replace('csp','csp-ns',inplace=True)
    nems['tech'].replace('hyded','hydED',inplace=True)
    nems['tech'].replace('hydend','hydEND',inplace=True)
    nems['tech'].replace('hydnpnd','hydNPND',inplace=True)
    nems['tech'].replace('hydnd','hydND',inplace=True)
    
    nems.loc[(nems['tech'] == 'pv'), 'tech'] = 'upv'
    
    # =============================================================================
    # Retire years
    # =============================================================================

    #nems.loc[pd.isna(nems['T_RYR']),'RetireYearGiven'] = False
    #nems.loc[nems['T_RYR']==' ','RetireYearGiven'] = False
    #nems.loc[nems['RetireYearGiven']!= False,'RetireYearGiven'] = True

    ### Update retirement dates of coal plants 
    coal_retirement_upd = pd.read_csv(os.path.join('Inputs','Coal_Retirements',coal_plant_retirement))
    coal_retirement_upd = coal_retirement_upd.rename(columns={'State':'TSTATE', 'Plant Name':'T_PNM', 'Generator ID':'T_UID', 'Plant Code':'T_PID'})
    coal_retirement_upd = coal_retirement_upd[['TSTATE', 'T_PNM', 'T_UID', 'T_PID', 'Retirement Year', 'MAT Exemptions']]
    coal_retirement_upd['Retirement Year']  = coal_retirement_upd['Retirement Year'].fillna(9999)
    
    # Update retirement dates to reflect new MAT exception
    # If the plant is set to retired by 2027 >> extend retirement years by two more years
    # If the plant is set to be retired after 2027 >> keep its retirement year
    coal_retirement_upd.loc[(coal_retirement_upd['MAT Exemptions']==1) & 
                            (coal_retirement_upd['Retirement Year']<=2027) & 
                            (coal_retirement_upd['Retirement Year']>=2025),'Retirement Year'] = coal_retirement_upd['Retirement Year'] + 2
    
    nems['T_PNM'] = nems['T_PNM'].apply(lambda x: x.rstrip())
    nems['TSTATE'] = nems['TSTATE'].apply(lambda x: x.rstrip())
    nems = nems.merge(coal_retirement_upd, on=['TSTATE', 'T_PNM', 'T_UID', 'T_PID'], how='left')
    nems['Retirement Year']  = nems['Retirement Year'].fillna(9999)

    for i in list(range(len(nems))):
        if (nems['Retirement Year'][i] != nems['T_RYR'][i]) & ('coal' in nems['tech'][i]):            
            nems.loc[i,'T_RYR'] = nems.loc[i,'Retirement Year']
            nems.loc[i,'NukeRefRetireYear'] = nems.loc[i,'Retirement Year']
            nems.loc[i,'Nuke60RetireYear'] = nems.loc[i,'Retirement Year']
            nems.loc[i,'Nuke80RetireYear'] = nems.loc[i,'Retirement Year']
            nems.loc[i,'NukeEarlyRetireYear'] = nems.loc[i,'Retirement Year']

    nems = nems.drop(['Retirement Year'], axis=1)
    
    nems['T_RYR'].fillna(9999,inplace=True)
    nems['T_RYR'] = nems['T_RYR'].replace(' ',9999)
    nems['T_RYR'] = nems['T_RYR'].replace('',9999)
    nems['T_RYR'] = pd.to_numeric(nems['T_RYR']).astype(int)
    no_retires = nems['T_RYR'] == 9999
    nems.loc[no_retires,'RetireYearGiven'] = False
    nems.loc[~no_retires,'RetireYearGiven'] = True
    
    lifetimes = pd.read_csv(os.path.join('Inputs','maxage.csv'))
    lifetimes.set_index('tech',inplace=True)
    
    for i in range(0,len(nems),1):
        if not nems.loc[i,'RetireYearGiven']:
            tech = nems.loc[i,'tech'].lower()
            size = nems.loc[i,'TC_SUM']
            if size >= 100:
                lifetime = lifetimes.loc[tech,'lifetime_big']
            elif size < 100:
                lifetime = lifetimes.loc[tech,'lifetime_small']
            
            # Assign retirement years to operating units with T_SYR <= current_year
            if nems.loc[i,'status'] == '(OP) Operating':
                
                # if start year is after refurbishment year (or if refurbishment year is null) 
                # and start year + lifetime is before current year, then extend retirement year by 10 years.
                # On the other hand, if start year + lifetime is already after current year,
                # then keep retirement year = start year + lifetime

                if (nems.loc[i,'T_SYR'] >= nems.loc[i,'TRFURB']) or (pd.isnull(nems['TRFURB'][i])):
                    StartYear_temp = nems.loc[i,'T_SYR']
                else:
                    StartYear_temp = nems.loc[i,'TRFURB']
            
            if (nems.loc[i,'T_RYR'] <= current_year) or (nems.loc[i,'T_RYR'] == 9999):
                if StartYear_temp + lifetime <= current_year:
                    nems.loc[i,'T_RYR'] = current_year + 10
                else:
                    nems.loc[i,'T_RYR'] = StartYear_temp + lifetime
                          
        elif nems.loc[i,'RetireYearGiven']:
            pass
            
    nems['Nuke60RetireYear'] = nems['T_RYR']
    nems['Nuke80RetireYear'] = nems['T_RYR']
    nems['NukeRefRetireYear'] = nems['T_RYR']
    nems['NukeEarlyRetireYear'] = nems['T_RYR']
    
    nukes = (nems['tech'] == 'nuclear') & (nems['RetireYearGiven'] == False)
    
    # The default nuclear lifetime is 80 years, so set the 60-year lifetime to be 20 years less
    nems.loc[nukes,'Nuke60RetireYear'] -= 20
    
    nukes1 = (nems['RetireYearGiven'] == False) & (nems['NukeRetireBin'] == 1)
    nukes2 = (nems['RetireYearGiven'] == False) & (nems['NukeRetireBin'] == 2)
    
    # For NukeRefRetireYear set the plants with bin 1 to retire at 60 years
    nems.loc[nukes1,'NukeRefRetireYear'] -= 20
    
    # For NukeEarlyRetireYear set the plants in bin 1 to retire at 50 years and in bin 2 to retire at 60 years
    nems.loc[nukes1,'NukeEarlyRetireYear'] -= 30
    nems.loc[nukes2,'NukeEarlyRetireYear'] -= 20
    
    check = nems[['tech','T_PNM','TC_SUM','NukeRetireBin','T_RYR','Nuke60RetireYear','Nuke80RetireYear','NukeRefRetireYear','NukeEarlyRetireYear','RetireYearGiven']]
    
    nems_cats = list(nems)
    
    exist = nems['T_RYR'] > 2010
    not_exist = nems['T_RYR'] <= 2010
    nems.loc[exist,'IsExistUnit'] = True
    nems.loc[not_exist,'IsExistUnit'] = False

    nems['T_SYR'] = nems['T_SYR'].astype(int)
    nems['NukeRefRetireYear'] = pd.to_numeric(nems['NukeRefRetireYear']).astype(int)
    nems['Nuke60RetireYear'] = pd.to_numeric(nems['Nuke60RetireYear']).astype(int)
    nems['Nuke80RetireYear'] = pd.to_numeric(nems['Nuke80RetireYear']).astype(int)
    nems['NukeEarlyRetireYear'] = pd.to_numeric(nems['NukeEarlyRetireYear']).astype(int)

    # =========================================================================
    # Update retirement years and technology conversion for a few units
    # -------------------------------------------------------------------------
    nems_cleaned = nems.copy()

    ### Monroe: Units 3,4 changed to 2029 and units 1,2 to 2033:
    nems_cleaned.loc[(nems_cleaned['TSTATE']=='MI') & (nems_cleaned['T_PNM'].str.contains('Monroe')) &
                     (nems_cleaned['tech']=='coaloldscr') & (nems_cleaned['T_RYR'] > 2021) &
                     ((nems_cleaned['T_UID']=='3') | (nems_cleaned['T_UID']=='4')),
                     ['T_RYR','NukeRefRetireYear','Nuke60RetireYear','Nuke80RetireYear','NukeEarlyRetireYear']] = [2029,2029,2029,2029,2029]
    
    nems_cleaned.loc[(nems_cleaned['TSTATE']=='MI') & (nems_cleaned['T_PNM'].str.contains('Monroe')) &
                     (nems_cleaned['tech']=='coaloldscr') & (nems_cleaned['T_RYR'] > 2021) & 
                     ((nems_cleaned['T_UID']=='1') | (nems_cleaned['T_UID']=='2')),
                     ['T_RYR','NukeRefRetireYear','Nuke60RetireYear','Nuke80RetireYear','NukeEarlyRetireYear']] = [2033,2033,2033,2033,2033]
    
    ### Belle River: Convert coal units to peakers in 2026:
    df_temp = nems_cleaned[(nems_cleaned['TSTATE']=='MI') & 
                     (nems_cleaned['T_PNM'].str.contains('Belle River')) &
                     (nems_cleaned['EFDcd']=='CSU')].copy()
    df_temp['tech'] = 'o-g-s'
    df_temp['EFDcd'] = 'CTN'
    df_temp['T_SYR'] = 2026
    df_temp[['T_RYR','NukeRefRetireYear','Nuke60RetireYear','Nuke80RetireYear','NukeEarlyRetireYear']] = [2081,2081,2081,2081,2081]

    nems_cleaned.loc[(nems_cleaned['TSTATE']=='MI') & (nems_cleaned['T_RYR'] > 2021) &
                     (nems_cleaned['T_PNM'].str.contains('Belle River')) &
                     (nems_cleaned['EFDcd']=='CSU'),
                     ['T_RYR','NukeRefRetireYear','Nuke60RetireYear','Nuke80RetireYear','NukeEarlyRetireYear']] = [2026,2026,2026,2026,2026]
    
    nems_cleaned = pd.concat([nems_cleaned, df_temp], axis=0)
    nems_cleaned = nems_cleaned.reset_index(drop=True)

    ### Edgewater unit 5: Convert coal unis to gas-cc in 2028:
    df_temp = nems_cleaned[(nems_cleaned['TSTATE']=='WI') & 
                     (nems_cleaned['T_PNM'].str.contains('Edgewater')) &
                     (nems_cleaned['T_UID'].str.contains('5')) &
                     (nems_cleaned['EFDcd']=='CSC')].copy()
    df_temp['tech'] = 'gas-cc'
    df_temp['EFDcd'] = 'CTN'
    df_temp['T_SYR'] = 2028
    df_temp[['T_RYR','NukeRefRetireYear','Nuke60RetireYear','Nuke80RetireYear','NukeEarlyRetireYear']] = [2108,2108,2108,2108,2108]

    nems_cleaned.loc[(nems_cleaned['TSTATE']=='WI') & (nems_cleaned['T_RYR'] > 2021) &
                     (nems_cleaned['T_UID'].str.contains('5')) &
                     (nems_cleaned['T_PNM'].str.contains('Edgewater')) &
                     (nems_cleaned['EFDcd']=='CSC'),
                     ['T_RYR','NukeRefRetireYear','Nuke60RetireYear','Nuke80RetireYear','NukeEarlyRetireYear']] = [2025,2025,2025,2025,2025]    
    nems_cleaned = pd.concat([nems_cleaned, df_temp], axis=0)
    nems_cleaned = nems_cleaned.reset_index(drop=True)

    ### River Rouge and St Clair: Retire diesel fuel units in 2024:
    nems_cleaned.loc[(nems_cleaned['TSTATE']=='MI') & (nems_cleaned['T_RYR'] > 2021) &
                     (nems_cleaned['T_PNM'].str.contains('River Rouge')) &
                     (nems_cleaned['EFDcd']=='CTO'),
                     ['T_RYR','NukeRefRetireYear','Nuke60RetireYear','Nuke80RetireYear','NukeEarlyRetireYear', 'status']] = [2024,2024,2024,2024,2024,'(R) Retired']

    nems_cleaned.loc[(nems_cleaned['TSTATE']=='MI') & (nems_cleaned['T_RYR'] > 2021) &
                     (nems_cleaned['T_PNM'].str.contains('St Clair')) &
                     (nems_cleaned['EFDcd']=='CTO'),
                     ['T_RYR','NukeRefRetireYear','Nuke60RetireYear','Nuke80RetireYear','NukeEarlyRetireYear', 'status']] = [2024,2024,2024,2024,2024,'(R) Retired']
    
    ### Diablo Canyon Nuclear Plant: Retire 1122 MW unit in 2029 and 1118 MW unit in 2030
    nems_cleaned.loc[(nems_cleaned['TSTATE']=='CA') & (nems_cleaned['T_RYR'] > 2021) &
                     (nems_cleaned['T_PNM'].str.contains('Diablo Canyon')) &
                     (nems_cleaned['tech']=='nuclear') & (nems_cleaned['TC_SUM'] == 1122),
                     ['T_RYR','NukeRefRetireYear','Nuke60RetireYear','Nuke80RetireYear','NukeEarlyRetireYear']] = [2029,2029,2029,2029,2029]
    
    nems_cleaned.loc[(nems_cleaned['TSTATE']=='CA') & (nems_cleaned['T_RYR'] > 2021) &
                     (nems_cleaned['T_PNM'].str.contains('Diablo Canyon')) &
                     (nems_cleaned['tech']=='nuclear') & (nems_cleaned['TC_SUM'] == 1118),
                     ['T_RYR','NukeRefRetireYear','Nuke60RetireYear','Nuke80RetireYear','NukeEarlyRetireYear']] = [2030,2030,2030,2030,2030]

    ### Palisades Nuclear: Restart in 2026:
    df_temp = nems_cleaned[(nems_cleaned['TSTATE']=='MI') & 
                     (nems_cleaned['T_PNM'].str.contains('Palisades')) &
                     (nems_cleaned['T_PID']==1715) &
                     (nems_cleaned['T_UID'].str.contains('1')) &
                     (nems_cleaned['EFDcd']=='CNU')].copy()
    df_temp['tech'] = 'nuclear'
    df_temp['T_SYR'] = 2026
    df_temp[['T_RYR','NukeRefRetireYear','Nuke60RetireYear','Nuke80RetireYear','NukeEarlyRetireYear','status']] = [2106,2106,2106,2106,2106,'(OP) Operating']

    nems_cleaned.loc[(nems_cleaned['TSTATE']=='MI') & (nems_cleaned['T_RYR'] > 2025) &
                     (nems_cleaned['T_PNM'].str.contains('Palisades')) &
                     (nems_cleaned['T_PID']==1715) &
                     (nems_cleaned['T_UID'].str.contains('1')) &
                     (nems_cleaned['EFDcd']=='CNU'),
                     ['T_RYR','NukeRefRetireYear','Nuke60RetireYear','Nuke80RetireYear','NukeEarlyRetireYear','status']] = [2022,2022,2022,2022,2022,'(R) Retired']    
    nems_cleaned = pd.concat([nems_cleaned, df_temp], axis=0)
    nems_cleaned = nems_cleaned.reset_index(drop=True)      

    ### Duane Arnold Nuclear: Restart in 2029:
    df_temp = nems_cleaned[(nems_cleaned['TSTATE']=='IA') & 
                     (nems_cleaned['T_PNM'].str.contains('Duane Arnold')) &
                     (nems_cleaned['T_PID']==1060) &
                     (nems_cleaned['T_UID'].str.contains('1')) &
                     (nems_cleaned['EFDcd']=='CNU')].copy()
    df_temp['tech'] = 'nuclear'
    df_temp['T_SYR'] = 2029
    df_temp[['T_RYR','NukeRefRetireYear','Nuke60RetireYear','Nuke80RetireYear','NukeEarlyRetireYear','status']] = [2109,2109,2109,2109,2109,'(OP) Operating']

    nems_cleaned = pd.concat([nems_cleaned, df_temp], axis=0)
    nems_cleaned = nems_cleaned.reset_index(drop=True)        

    ### Three Mile Island Nuclear: Restart in 2027:
    # Three Mile Island is no longer in the AEO database and is not yet in the EIA860M database
    # So use Duane Arnold Nuclear as base then fill in old Three Mile Island Data from AEO 2023 with updated retire year
    df_temp = nems_cleaned[(nems_cleaned['TSTATE']=='IA') & 
                     (nems_cleaned['T_PNM'].str.contains('Duane Arnold')) &
                     (nems_cleaned['T_PID']==1060) &
                     (nems_cleaned['T_UID'].str.contains('1')) &
                     (nems_cleaned['EFDcd']=='CNU') &
                     (nems_cleaned['status']=='(R) Retired')].copy()
    df_temp['tech'] = 'nuclear'
    df_temp['T_SYR'] = 2027
    df_temp['T_PNM'] = 'Three Mile Island'
    df_temp[['TC_SUM','TC_WIN','TC_NP']] = [802.8,829,980.8]
    df_temp[['T_PID','T_CID','T_UID']] = [8011,'55951','1']
    df_temp[['TSTATE','county','FIPS','T_PCA','T_IGRP','T_GRP','T_GRP2']] = ['PA','Dauphin County','p42043','PJM',5266,5266,1]
    df_temp[['TFOWN','T_MRUN','TEFPT','TNOPER','TNOWN','T_CLRG','T_CR','T_GR','TCOUNT']] = [3,0,10,10,10,2,2,2,1]
    df_temp[['TRFURB','T_SMO','T_RMO','T_CF']] = [1974,2019,9,0.99]
    df_temp[['M_CF_JAN','M_CF_FEB','M_CF_MAR','M_CF_APR','M_CF_MAY','M_CF_JUN','M_CF_JUL','M_CF_AUG','M_CF_SEP','M_CF_OCT','M_CF_NOV','M_CF_DEC']] = [1,1,1,1,1,1,1,1,0.879,0.892,1,0.893]
    df_temp[['T_LONG','T_LAT']] = [-76.723,40.152]
    df_temp[['T_VOM','T_FOM','T_CAPAD','TOID']] = [0.077,124.414,17.962,12390]
    df_temp[['T_RYR','NukeRefRetireYear','Nuke60RetireYear','Nuke80RetireYear','NukeEarlyRetireYear','status']] = [2107,2107,2107,2107,2107,'(OP) Operating']

    nems_cleaned = pd.concat([nems_cleaned, df_temp], axis=0)
    nems_cleaned = nems_cleaned.reset_index(drop=True)                                                                       
    # =========================================================================
    
    # =============================================================================
    # Formatting
    # =============================================================================
    
    nems_cleaned.loc[:,'Plant.NAICS.Description'] = 'Utilities'
    
    nems_cats_ordered = ['tech','reeds_ba','resource_region','TC_SUM','T_RYR','NukeRefRetireYear','Nuke60RetireYear','Nuke80RetireYear','NukeEarlyRetireYear','T_SYR',
                         'IsExistUnit','THRATE','FIPS','county']
    
    for cat in nems_cats:
        if cat not in nems_cats_ordered:
            nems_cats_ordered.append(cat)
            
    nems_ordered = nems_cleaned[nems_cats_ordered].copy()
    
    # Note that T_SYR is the online year for the most recent time the unit
    # came online. TRFURB holds the original start date of the plant.
    nems_ordered.rename(columns={'TC_SUM':'summer_power_capacity_MW','T_RYR':'RetireYear','T_SYR':'StartYear','THRATE':'HeatRate'},inplace=True)
    
    #nems_ordered.loc[:,'StartYear'] = nems_ordered.loc[:,'StartYear'].astype(str) + '-1'
    
    nems = nems_ordered.copy()
    
    techs = nems['tech'].drop_duplicates().tolist()
    
    no_hr = ['hydED','hydEND','hydNPND','hydND','pumped-hydro','wind-ons','wind-ofs','csp-ns','dupv','upv','battery_li','pvb_pv','pvb_battery']
    
    nems.loc[nems['tech'].isin(no_hr),'HeatRate'] = np.nan
    nems = nems.reset_index(drop=True)
    
    return nems


