"""
Updated August 19 2026

This sub-script:
    1. Assigns retire years to units without retire years from AEO-NEMS and EIA860M using maxage.csv
    2. Updated retire years for coal plants that are MATS exempted
    3. Revise retire years and technology conversion of a few special units
"""

import pandas as pd
import numpy as np
import os
import math

def set_retire_years(nems,reeds_path,coal_plant_retirement,current_year):

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

    ### Update retirement dates of coal plants 
    coal_retirement_upd = pd.read_csv(os.path.join('inputs','coal_retirements',coal_plant_retirement))
    coal_retirement_upd = coal_retirement_upd.rename(columns={'Plant Name':'T_PNM', 'Generator ID':'T_UID', 'Plant Code':'T_PID'})
    coal_retirement_upd = coal_retirement_upd[['T_PNM', 'T_UID', 'T_PID', 'Retirement Year', 'MATS Exemptions']]
    coal_retirement_upd['Retirement Year']  = coal_retirement_upd['Retirement Year'].fillna(9999)
    
    # Update retirement dates to reflect new MATS exception
    # (source: https://www.epa.gov/system/files/documents/2025-04/regulatory-relief-for-certain-stationary-annex-1.pdf)
    # If the plant is set to retired by 2027 >> extend retirement years by two more years
    # If the plant is set to be retired after 2027 >> keep its retirement year
    coal_retirement_upd.loc[(coal_retirement_upd['MATS Exemptions']==1) & 
                            (coal_retirement_upd['Retirement Year']<=2027) & 
                            (coal_retirement_upd['Retirement Year']>=current_year),
                            'Retirement Year'] = coal_retirement_upd['Retirement Year'] + 2
    
    nems['T_PNM'] = nems['T_PNM'].apply(lambda x: x.rstrip())
    nems = nems.merge(coal_retirement_upd, on=['T_PNM', 'T_UID', 'T_PID'], how='left')
    nems['Retirement Year']  = nems['Retirement Year'].fillna(9999)

    for i in list(range(len(nems))):
        if (nems['Retirement Year'][i] != nems['T_RYR'][i]) and ('coal' in nems['tech'][i]):            
            nems.loc[i,'T_RYR'] = nems.loc[i,'Retirement Year']

    nems = nems.drop(['Retirement Year'], axis=1)
    
    nems['T_RYR'].fillna(9999,inplace=True)
    nems['T_RYR'] = nems['T_RYR'].replace(' ',9999)
    nems['T_RYR'] = nems['T_RYR'].replace('',9999)
    nems['T_RYR'] = pd.to_numeric(nems['T_RYR']).astype(int)
    no_retires = nems['T_RYR'] == 9999
    nems.loc[no_retires,'RetireYearGiven'] = False
    nems.loc[~no_retires,'RetireYearGiven'] = True

    # Update retire year for units based on max age
    lifetimes = pd.read_csv(os.path.join(reeds_path,'inputs','plant_characteristics','maxage.csv'),
                            header=None, names=['tech','lifetime'])
    lifetimes['tech'] = lifetimes['tech'].str.lower()

    # Assign lifetime for lfill-gas and pumped-hydro = 100 years,
    # assign lifetime for csp-ns = that of upv,
    # and assign lifetime for pvb = that of pv
    lifetime_lfill = lifetimes.loc[lifetimes['tech']=='biopower'].copy()
    lifetime_lfill['tech'] = 'lfill-gas'
    lifetime_psh = lifetimes.loc[lifetimes['tech']=='hydro'].copy()
    lifetime_psh['tech'] = 'pumped-hydro'
    lifetime_csp = lifetimes.loc[lifetimes['tech'].str.contains('upv')].copy()
    lifetime_csp['tech'] = 'csp-ns'
    lifetime_pvb_bat = lifetimes.loc[lifetimes['tech'].str.contains('upv')].copy()
    lifetime_pvb_bat['tech'] = 'pvb_battery'
    lifetime_pvb_pv = lifetimes.loc[lifetimes['tech'].str.contains('upv')].copy()
    lifetime_pvb_pv['tech'] = 'pvb_pv'
    lifetimes = pd.concat([lifetimes,lifetime_lfill, lifetime_psh, 
                           lifetime_csp, lifetime_pvb_bat, lifetime_pvb_pv], ignore_index=True)
    lifetimes.loc[lifetimes['tech'].isin(['lfill-gas','pumped-hydro']),'lifetime'] = 100
    lifetimes.loc[lifetimes['tech']=='geothermal','tech'] = 'geohydro_allkm'
    
    for i in range(0,len(nems),1):
        tech = nems.loc[i,'tech'].lower()
        if tech in lifetimes['tech']:
            lifetime = lifetimes.loc[lifetimes['tech']==tech,'lifetime']
        else:
            lifetime = lifetimes.loc[lifetimes['tech'].str.contains(tech),
                                     'lifetime'].values[0]
        # Assign retire year to units that do not have give retire year from both AEO-NEMS and EIA860M
        if not nems.loc[i,'RetireYearGiven']:
            StartYear_temp = nems.loc[i,'T_SYR']
            # Assign retirement years to operating units with T_SYR <= current_year
            operating_cat = ['(OP) Operating',
                             '(OS) Out of service and NOT expected to return to service in next calendar year',
                             '(OA) Out of service but expected to return to service in next calendar year',
                             '(SB) Standby/Backup: available for service but not normally used']
            if nems.loc[i,'status'] in operating_cat: 
            # if start year is after refurbishment year (or if refurbishment year is null) 
            # and start year + lifetime is before current year, then extend retirement year by 
            # increments of 10 years until the new retire year is at least 10 years from current year.
            # On the other hand, if start year + lifetime is already after current year,
            # then keep retirement year = start year + lifetime
                if (nems.loc[i,'T_SYR'] >= nems.loc[i,'TRFURB']) or (pd.isnull(nems['TRFURB'][i])):
                    StartYear_temp = nems.loc[i,'T_SYR']
                else:
                    StartYear_temp = nems.loc[i,'TRFURB']
                
            if (nems.loc[i,'T_RYR'] <= current_year) or (nems.loc[i,'T_RYR'] == 9999):
                if StartYear_temp + lifetime <= current_year:
                    extended_years = math.ceil((current_year + 10 - StartYear_temp - lifetime)/10)*10
                    nems.loc[i,'T_RYR'] = StartYear_temp + lifetime + extended_years
                else:
                    nems.loc[i,'T_RYR'] = StartYear_temp + lifetime

        elif nems.loc[i,'RetireYearGiven']:
            pass

        ## Reset start year based on repower year
        if (pd.notna(nems.at[i,'T_RPYR'])) and (nems.loc[i,'T_RPYR'] > nems.loc[i,'T_RYR']):
            # If repower year is immediately after retire year, keep start year 
            # and update retire year as repower year + lifetime
            # (in this case the unit is not considered a new unit)
            if nems.loc[i,'T_RYR'] + 1 == nems.loc[i,'T_RPYR']:
                nems.loc[i, 'T_RYR'] = nems.loc[i,'T_RPYR'] + lifetime
            # If repower year is not immediately after retire year, the unit is retired 
            # and then restart, so update start year as repower year and update retire year
            # (in this case the unit is considered a new unit with online year = repower year)
            else:
                nems.loc[i,'T_SYR'] = nems.loc[i,'T_RPYR']
                nems.loc[i,'T_RYR'] = nems.loc[i,'T_SYR'] + lifetime
        
    exist = nems['T_RYR'] > 2010
    not_exist = nems['T_RYR'] <= 2010
    nems.loc[exist,'IsExistUnit'] = True
    nems.loc[not_exist,'IsExistUnit'] = False

    nems['T_SYR'] = nems['T_SYR'].astype(int)

    # =========================================================================
    # Update retirement years and technology conversion for a few units
    # -------------------------------------------------------------------------
    nems_cleaned = nems.copy()

    ### Monroe: Units 3,4 changed to 2029 and units 1,2 to 2033:
    nems_cleaned.loc[(nems_cleaned['T_PNM'].str.contains('Monroe \\(MI\\)')) &
                     (nems_cleaned['tech']=='coaloldscr') & (nems_cleaned['T_RYR'] > 2021) &
                     ((nems_cleaned['T_UID']=='3') | (nems_cleaned['T_UID']=='4')),
                     'T_RYR'] = 2029
    
    nems_cleaned.loc[(nems_cleaned['T_PNM'].str.contains('Monroe \\(MI\\)')) &
                     (nems_cleaned['tech']=='coaloldscr') & (nems_cleaned['T_RYR'] > 2021) & 
                     ((nems_cleaned['T_UID']=='1') | (nems_cleaned['T_UID']=='2')),
                     'T_RYR'] = 2033
    
    ### Belle River: Convert coal units to peakers in 2026:
    df_temp = nems_cleaned[(nems_cleaned['FIPS']=='p26147') & 
                           (nems_cleaned['T_PNM'].str.contains('Belle River')) &
                           (nems_cleaned['EFDcd']=='CSU')].copy()
    df_temp['tech'] = 'o-g-s'
    df_temp['EFDcd'] = 'CTN'
    df_temp['T_SYR'] = 2026
    df_temp['T_RYR'] = 2081

    nems_cleaned.loc[(nems_cleaned['FIPS']=='p26147') & (nems_cleaned['T_RYR'] > 2021) &
                     (nems_cleaned['T_PNM'].str.contains('Belle River')) &
                     (nems_cleaned['EFDcd']=='CSU'),
                     'T_RYR'] = 2026
    
    nems_cleaned = pd.concat([nems_cleaned, df_temp], axis=0)
    nems_cleaned = nems_cleaned.reset_index(drop=True)

    ### Edgewater unit 5: Convert coal unis to gas-cc in 2028:
    df_temp = nems_cleaned[(nems_cleaned['T_PNM'].str.contains('Edgewater')) &
                           (nems_cleaned['T_UID'].str.contains('5')) &
                           (nems_cleaned['EFDcd']=='CSC')].copy()
    df_temp['tech'] = 'gas-cc'
    df_temp['EFDcd'] = 'CTN'
    df_temp['T_SYR'] = 2028
    df_temp['T_RYR'] = 2108

    nems_cleaned.loc[(nems_cleaned['T_RYR'] > 2021) &
                     (nems_cleaned['T_UID'].str.contains('5')) &
                     (nems_cleaned['T_PNM'].str.contains('Edgewater')) &
                     (nems_cleaned['EFDcd']=='CSC'),
                     'T_RYR'] = 2025   
    nems_cleaned = pd.concat([nems_cleaned, df_temp], axis=0)
    nems_cleaned = nems_cleaned.reset_index(drop=True)

    ### Diablo Canyon Nuclear Plant: Retire 1122 MW unit in 2029 and 1118 MW unit in 2030
    nems_cleaned.loc[(nems_cleaned['T_RYR'] > 2021) &
                     (nems_cleaned['T_PNM'].str.contains('Diablo Canyon')) &
                     (nems_cleaned['tech']=='nuclear') & (nems_cleaned['TC_SUM'] == 1122),
                     'T_RYR'] = 2029
    
    nems_cleaned.loc[(nems_cleaned['T_RYR'] > 2021) &
                     (nems_cleaned['T_PNM'].str.contains('Diablo Canyon')) &
                     (nems_cleaned['tech']=='nuclear') & (nems_cleaned['TC_SUM'] == 1118),
                     'T_RYR'] = 2030  

    ### Duane Arnold Nuclear: Restart in 2029:
    df_temp = nems_cleaned[(nems_cleaned['T_PNM'].str.contains('Duane Arnold')) &
                           (nems_cleaned['T_PID']==1060) &
                           (nems_cleaned['T_UID'].str.contains('1')) &
                           (nems_cleaned['EFDcd']=='CNU')].copy()
    df_temp['tech'] = 'nuclear'
    df_temp['T_SYR'] = 2029
    df_temp[['T_RYR','status']] = [2109,'(P) Planned']

    nems_cleaned = pd.concat([nems_cleaned, df_temp], axis=0)
    nems_cleaned = nems_cleaned.reset_index(drop=True)        

    ### Three Mile Island Nuclear: Restart in 2027:
    # Three Mile Island is no longer in the AEO database and is not yet in the EIA860M database
    # So use Duane Arnold Nuclear as base then fill in old Three Mile Island Data from AEO 2026 with updated retire year
    df_temp = nems_cleaned[(nems_cleaned['T_PNM'].str.contains('Duane Arnold')) &
                           (nems_cleaned['T_PID']==1060) &
                           (nems_cleaned['T_UID'].str.contains('1')) &
                           (nems_cleaned['EFDcd']=='CNU') &
                           (nems_cleaned['status']=='(R) Retired')].copy()
    df_temp['tech'] = 'nuclear'
    df_temp['T_SYR'] = 2027
    df_temp['T_PNM'] = 'Three Mile Island'
    df_temp[['TC_SUM','TC_WIN','TC_NP']] = [802.8,829,980.8]
    df_temp[['T_PID','T_CID','T_UID']] = [8011,'55951','1']
    df_temp[['FIPS','T_PCA','T_IGRP','T_GRP','T_GRP2']] = ['p42043','PJM',5266,5266,1]
    df_temp[['TRFURB','T_SMO','T_RMO']] = [1974,2019,9]
    df_temp[['T_LONG','T_LAT']] = [-76.723,40.152]
    df_temp[['T_VOM','T_FOM','T_CAPAD']] = [0.077,124.414,17.962]
    df_temp[['T_RYR','status']] = [2107,'(P) Planned']

    nems_cleaned = pd.concat([nems_cleaned, df_temp], axis=0)
    nems_cleaned = nems_cleaned.reset_index(drop=True)                                                                       
    # =========================================================================
    
    # =============================================================================
    # Formatting
    # =============================================================================
    
    # Note that T_SYR is the online year for the most recent time the unit
    # came online. TRFURB holds the original start date of the plant.
    nems_cleaned.rename(columns={'TC_SUM':'summer_power_capacity_MW','T_RYR':'RetireYear',
                                 'T_SYR':'StartYear','THRATE':'HeatRate'},inplace=True)
            
    no_hr = ['hydED','hydEND','hydNPND','hydND','pumped-hydro','wind-ons','wind-ofs',
             'csp-ns','dupv','upv','battery_li','pvb_pv','pvb_battery']
    
    nems_cleaned.loc[nems_cleaned['tech'].isin(no_hr),'HeatRate'] = np.nan
    nems_cleaned = nems_cleaned.reset_index(drop=True)
    
    return nems_cleaned


