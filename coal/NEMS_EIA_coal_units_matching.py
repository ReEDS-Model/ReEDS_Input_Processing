"""
by apham
Updated Feb 11 2026

This script merges ReEDS's NEMS unit database to EIA923 to assign coal types to each ba.
"""

import sys
import os
import pandas as pd

# Get NEMS database from ReEDS repo
reeds_path = os.path.expanduser('~/Documents/Github/ReEDS/public_ReEDS/ReEDS')
sys.path.append(reeds_path)

# Read in EIA 923
dir = os.getcwd()
df_EIA923 = pd.read_excel(os.path.join(dir, "inputs", 
                                       "EIA923_Schedules_2_3_4_5_M_10_2025_17DEC2025.xlsx"),
                                       sheet_name = "Page 3 Boiler Fuel Data")

df_EIA923.columns = df_EIA923.columns.str.replace('\n', ' ', regex=True)
df_EIA923 = df_EIA923.rename(columns={'Plant Id':'T_PID','Boiler Id':'T_UID', 
                                      'Reported Fuel Type Code': 'Fuel Code'})

# Filter out plants from Electric Utility sector
#df_EIA923 = df_EIA923[(df_EIA923['Sector Name'] == 'Electric Utility')]

# Filter out coal plants from EIA923
coal_type_EIA923 = ['ANT', 'BIT', 'LIG', 'RC', 'SUB', 'WC']
df_EIA923_coal = df_EIA923[df_EIA923['Fuel Code'].isin(coal_type_EIA923)]
#df_EIA923_coal = df_EIA923_coal[df_EIA923_coal['Total Fuel Consumption Quantity']>0]

df_EIA923_coal = df_EIA923_coal[['T_PID','T_UID','Fuel Code','Plant Name',
                                 'Plant State','Census Region','BA_CODE','YEAR', 
                                 'Total Fuel Consumption Quantity']]
df_EIA923_coal['in_EIA'] = 1
df_EIA923_coal['EIA ID'] = df_EIA923_coal.reset_index().index

# Read in NEMS database
df_NEMS = pd.read_csv(os.path.join(reeds_path, "inputs", "capacity_exogenous", "ReEDS_generator_database_final_EIA-NEMS.csv"))
coal_type_NEMS = ['coalolduns', 'coal-igcc', 'coal-new', 'coaloldscr']
df_NEMS_coal = df_NEMS[df_NEMS['tech'].isin(coal_type_NEMS)]
df_NEMS_coal = df_NEMS_coal[df_NEMS_coal['RetireYear']>=2025]
df_NEMS_coal = df_NEMS_coal.drop_duplicates(subset=['T_PID','T_UID'])
df_NEMS_coal['NEMS ID'] = df_NEMS_coal.reset_index().index
df_NEMS_coal['in_NEMS'] = 1

# Merge NEMS and EIA923
df_NEMS_EIA923 = pd.merge(df_NEMS_coal, df_EIA923_coal, on=['T_PID','T_UID'],how='left')

df_NEMS_EIA923 = df_NEMS_EIA923[['NEMS ID','EIA ID', 'in_NEMS', 'in_EIA','tech','Fuel Code', 
                                 'T_PNM', 'Plant Name', 'T_PID', 'T_UID',
                                 'StartYear', 'RetireYear', 'Plant State', 
                                 'summer_power_capacity_MW', 'T_LAT','T_LONG','FIPS',
                                 'Census Region','T_PCA','Total Fuel Consumption Quantity']]

# Save matched units:
df_NEMS_EIA923_matched = df_NEMS_EIA923[(df_NEMS_EIA923['in_NEMS']==1) & (df_NEMS_EIA923['in_EIA']==1)]
df_NEMS_EIA923_matched.to_csv(os.path.join(dir, "inputs", "NEMS_EIA_matched1.csv"),index=False)

# Get unmatched units to rematch second round:
df_NEMS_unmatched = df_NEMS_EIA923[~((df_NEMS_EIA923['in_NEMS']==1) & (df_NEMS_EIA923['in_EIA']==1))]
#df_NEMS_unmatched = df_NEMS_unmatched[['T_PID','T_UID', 'EIA ID']]
df_NEMS_coal_unmatched = df_NEMS_coal.merge(df_NEMS_unmatched,on=['T_PID','T_UID'],how='right')
df_NEMS_coal_unmatched = df_NEMS_coal_unmatched.rename(columns={'NEMS ID_x':'NEMS ID', 
                                                                'in_NEMS_x':'in_NEMS',
                                                                'tech_x':'tech',
                                                                'T_PNM_x':'T_PNM',
                                                                'StartYear_x':'StartYear', 
                                                                'RetireYear_x':'RetireYear', 
                                                                'summer_power_capacity_MW_x':'summer_power_capacity_MW', 
                                                                'T_PCA_x':'T_PCA',
                                                                'Total Fuel Consumption Quantity_x': 'Total Fuel Consumption Quantity',
                                                                'T_LAT_x':'T_LAT',
                                                                'T_LONG_x':'T_LONG',
                                                                'FIPS_x':'FIPS'})
df_NEMS_coal_unmatched = df_NEMS_coal_unmatched[df_NEMS_EIA923.columns.to_list()]
df_NEMS_coal_unmatched.to_csv(os.path.join(dir, "inputs", "NEMS_EIA_unmatched1.csv"),index=False)
print("\n For all the units that are in NEMS but not EIA923 (NEMS_EIA_unmatched1.csv), " \
        "please manually enter the coal types for those units and save as NEMS_EIA_unmatched1_manually_cleaned.csv " \
        "before moving to next step")

