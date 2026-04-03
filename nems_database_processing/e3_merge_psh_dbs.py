# -*- coding: utf-8 -*-
"""
Created on Fri Jun 23 17:13:14 2023

This script merges the two EHA hydro databases from Stuart/ORNL and uses data
from the merged database to reclassify various hydro 'hyd*' units in the ReEDS 
EIA generator database.
---------------------
Data Inputs:
- 2022 EHA Plant Database: A database containing hydropower units with operating
  start years of 2010 or later. This database contains a PrjType column that is
  used to reclassify hydro units by the following:
    - {NPD : hydNPND, NSD : hydND}
- 2023 EHA Plant Database: A database containing the same hydropower units as the 
  2022 EHA Plant Database along with the necessary Plant ID/Unit ID data that is
  used to merge with the ReEDS EIA generator database
- ReEDS EIA-NEMS generator database: a copy of the most recent generator database 
  used by the ReEDS model
---------------------
Output:
- 'updated_gendb.csv': a copy of the ReEDS database with updated hydro tech 
  classifications. The main output of this script
- 'updated_generators.csv': a csv containing all hydro units that were reclassified,
  including columns that show the old ReEDS tech, EHA PrjType, and new reclassified 
  ReEDS tech. 
- 'unaccounted_generators.csv': a csv containing all hydro units from the 2022 EHA
  Plant Database that are not accounted for in the ReEDS EIA-NEMS generator database.

@author: jcarag
"""

import os
import math
import numpy as np
import pandas as pd

def merge_psh_dbs(gendb,hydro_prjtype,ornl_hydro_unit_ver):

    # DICTIONARIES
    hydreclassify = {
        'NPD' : 'hydNPND',
        'NSD' : 'hydND'
        }

    #%% PROCEDURE

    # IMPORT DATA FILES
    db1 = pd.read_excel(os.path.join('Inputs','ORNL_EHA',hydro_prjtype))
    db2 = pd.read_excel(os.path.join('Inputs','ORNL_EHA',ornl_hydro_unit_ver),
                        sheet_name='Operational')
    # gendb = pd.read_csv(os.path.join(reedsdir,'inputs','capacitydata',
    #                                  'ReEDS_generator_database_final_EIA-NEMS.csv'),
    #                     header=0)
    #gendb = pd.read_csv(os.path.join(projectdir,'ReEDS_generator_database_final_EIA-NEMS.csv'),
    #                    header=0)

    # Filter for NPD and NSD techs, downsize db2 to only contain necessary info
    db1 = (db1
        .loc[db1['PrjType'].isin(['NPD','NSD'])]
        .rename(columns={'PtName':'T_PNM','EIA_PtID':'T_PID'})
        .drop(['ReEDSPCA'], axis=1)
        )
    db1['db1'] = 1
    db2 = (db2[['PtName','EIA_PtID','EIA_GnID','MW','OpYear','ReEDSPCA']]
        .rename(columns={'PtName':'T_PNM','EIA_PtID':'T_PID','EIA_GnID':'T_UID','ReEDSPCA':'reeds_ba'})
        )
    db2['db2'] = 1

    # Merge
    dfmerge = db1.merge(db2, on=['T_PNM'], how='left')
    dfmerge['tech'] = dfmerge['PrjType'].apply(lambda x: hydreclassify[x])

    dfmerge.drop('EHA_PtID',axis=1, inplace=True)

    # Get T_PID from either df1 or df2
    for i,row in dfmerge.iterrows():
        if not math.isnan(row['T_PID_x']):
            dfmerge.loc[i,'T_PID'] = int(row['T_PID_x'])
        elif not math.isnan(row['T_PID_y']):
            dfmerge.loc[i,'T_PID'] = int(row['T_PID_y'])
        else:
            dfmerge.loc[i, 'T_PID'] = np.nan


    for col in ['T_UID','T_PID','PrjType','tech']:
        dfmerge.insert(0,col,dfmerge.pop(col))
    dfmerge.insert(dfmerge.columns.get_loc('State'),'MW',dfmerge.pop('MW'))
    dfmerge.insert(dfmerge.columns.get_loc('State'),'reeds_ba',dfmerge.pop('reeds_ba'))
    dfmerge.drop(['T_PID_x','T_PID_y'], axis=1,inplace=True)


    dfsmall = dfmerge[['tech','PrjType','T_PID','T_UID','CH_OpYear','T_PNM','County','reeds_ba',
                    'State','MW','Lat','Lon']].copy()

    #%%

    # Remove leading/trailing spaces from Unit ID strings
    gendb['T_UID'] = gendb['T_UID'].str.replace(' ','')
    dfsmall['T_UID'] = dfsmall['T_UID'].str.replace(' ','')

    # Separate generators based on whether or not they exist in the gendb
    merge2gendb = pd.DataFrame()
    notingendb = pd.DataFrame()
    count = 0
    print('Generators not in the EIA-NEMS Generator Database:')
    for i,row in dfsmall.iterrows():
        # PID exists
        if not math.isnan(row['T_PID']):
            if (int(row['T_PID']) not in gendb['T_PID'].tolist()) and (row['T_PNM'] not in gendb['T_PNM'].tolist()):
                count += 1
                print(f' [{count:>2}] {row.T_PNM}')
                notingendb = pd.concat([notingendb,pd.DataFrame(row).T],ignore_index=True)
            else:
                merge2gendb = pd.concat([merge2gendb,pd.DataFrame(row).T],ignore_index=True)
        # PID doesn't exist
        elif row['T_PNM'] not in gendb['T_PNM'].tolist():
            count += 1
            print(f' [{count:>2}] {row.T_PNM}')
            notingendb = pd.concat([notingendb,pd.DataFrame(row).T],ignore_index=True)

    # Reclassify hydro units in gendb using data from merge2gendb
    check = pd.DataFrame()
    print('\n')
    print('Reclassifying tech for the following generators in gendb:')
    print('  PID     UID     Plant Name')
    for i,row in merge2gendb.iterrows():
        # Skip if StartYear in gendb <= 2010
        startyear = gendb.loc[(gendb['T_PID']==int(row['T_PID'])) & (gendb['T_UID']==str(row['T_UID'])),'StartYear'].iloc[0]
        startyear = int(startyear)
        if startyear <= 2010:
            print(f'  {int(row.T_PID): >5}   {str(row.T_UID): >5}   {row.T_PNM}')
            print(f'                  Skipped because Start Year of {startyear} <= 2010')
            print('')
            continue
        print(f'  {int(row.T_PID): >5}   {str(row.T_UID): >5}   {row.T_PNM}')
        gendb_tech = gendb.loc[(gendb['T_PID']==int(row['T_PID'])) & (gendb['T_UID']==str(row['T_UID'])),'tech'].iloc[0]
        gendb_year = gendb.loc[(gendb['T_PID']==int(row['T_PID'])) & (gendb['T_UID']==str(row['T_UID'])),'StartYear'].iloc[0]
        print(f'                  {gendb_tech} ---> {row.tech}')
        print(f'                  {gendb_year} ---> {int(row.CH_OpYear)}')
        print(f'                  p{str(int(row.reeds_ba))}')
        print('')
        gendb.loc[(gendb['T_PID']==row['T_PID']) & (gendb['T_UID']==row['T_UID']),'tech'] = row['tech']
        check = pd.concat([check,gendb.loc[(gendb['T_PID']==row['T_PID']) & (gendb['T_UID']==row['T_UID'])]],ignore_index=True)
        check.loc[(check['T_PID']==row['T_PID']) & (check['T_UID']==row['T_UID']),'old_tech'] = gendb_tech
        check.loc[(check['T_PID']==row['T_PID']) & (check['T_UID']==row['T_UID']),'PrjType'] = row['PrjType']

    check.rename(columns={'tech':'new_tech'},inplace=True)
    check.insert(0,'PrjType',check.pop('PrjType'))
    check.insert(0,'old_tech',check.pop('old_tech'))
    check.to_csv(os.path.join('Outputs','updated_generators.csv'),     
                header=True,index=False)
    notingendb.to_csv(os.path.join('Outputs','unaccounted_generators.csv'),
                    header=True,index=False)
    #gendb.to_csv(os.path.join('Outputs','updated_gendb.csv'), header=True,index=False)
    return gendb