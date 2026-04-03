# This script creates the demand projection files for AEO scenarios
# It uses historical retail sales and behind-the-meter PV generation
# data from EIA to calibrate historical state-level demand, and then
# carries that forward using demand ratios calculated from the
# AEO scenarios.

# The original capability was created by Anna Schleifer in 2023, and
# then was adapted by Wesley Cole in 2025.

import os
import pandas as pd
from _eia_api_functions import api_key, create_EIA_url, create_SEDS_url, retrieve_EIA_data
import matplotlib.pyplot as plt

### Set up information

# lastyear is the last year that historical data are available
lastyear = 2023
AEO_year = 2025

### Load and process data

# Import state to census division mapping file
st_cendiv = pd.read_csv(os.path.join('inputs', 'st_cendiv.csv'))
# Rename columns to match EIA data
st_cendiv.rename(columns={'st':'stateid'}, inplace=True)

# Load the "Demand Ratios" sheet from the Excel file
demand_ratios = pd.read_excel(
    'Electricity Demand Preprocessing for AEO Inputs.xlsx',
    sheet_name='Demand Ratios'
)
# Melt years into long format
demand_ratios = demand_ratios.melt(id_vars=['scenario','cendiv'], var_name='year', value_name='ratio')
demand_ratios['year'] = demand_ratios['year'].astype(int)

# Normalize ratios to 1 in lastyear
demand_ratios = demand_ratios.merge(
    demand_ratios[demand_ratios['year'] == lastyear][['scenario', 'cendiv', 'ratio']],
    on=['scenario', 'cendiv'],
    suffixes=('', '_lastyear')
)
demand_ratios['ratio'] = demand_ratios['ratio'] / demand_ratios['ratio_lastyear']
demand_ratios.drop(columns=['ratio_lastyear'], inplace=True)

# Set values before lastyear to 1.0
demand_ratios.loc[demand_ratios['year'] < lastyear, 'ratio'] = 1.0

# Collect state-level retail sales data from EIA API
url_retail = create_EIA_url(api_key, 'retail-sales', ['sales'], 
                            {'sectorid':['ALL']}, freq='annual', start=2010)
df_retail = retrieve_EIA_data(url_retail)
df_retail = df_retail[['year', 'stateid', 'sales']].copy()
df_retail['sales'] = pd.to_numeric(df_retail['sales'], errors='coerce').fillna(0)
df_retail.replace({'stateid': 'DC'}, 'MD', inplace=True)
df_retail = df_retail.groupby(by=['year', 'stateid']
                              ).agg('sum').reset_index(drop=False)
# There is more recent retail sales data than PV generation data, so drop rows with years greater than lastyear
df_retail = df_retail[df_retail['year'] <= lastyear]

# Collect state-level behind-the-meter PV generation data from SEDS via EIA API
# SOR7P = Residential PV
# SOCCP = Commercial PV
# SOICP = Industrial PV
url_seds = create_SEDS_url(api_key,
                           series_IDs=['SOR7P','SOCCP','SOICP'],
                           freq='annual', start=2009)
df_pv = retrieve_EIA_data(url_seds)
# Rename stateId to stateid to match retail sales df
df_pv.rename(columns={'stateId':'stateid'}, inplace=True)
df_pv2 = df_pv[['year', 'stateid', 'value']].copy()
df_pv2.replace({'stateid': 'DC'}, 'MD', inplace=True)
# Convert value column to numbers
df_pv2['value'] = pd.to_numeric(df_pv2['value'], errors='coerce').fillna(0)
df_pv3 = df_pv2.groupby(by=['year', 'stateid']
                      ).agg('sum').reset_index(drop=False)
# Rename value column to generation
df_pv3.rename(columns={'value':'pvgen'}, inplace=True)

# Combine retail sales and residential PV generation 
df_combined = df_retail.merge(df_pv3, on=['year', 'stateid'], how='left').fillna(0)

# Add load column (sales + pv generation)
df_combined['load'] = df_combined['sales'] + df_combined['pvgen']
# Drop sales and pvgen columns
df_combined.drop(columns=['sales', 'pvgen'], inplace=True)

# Normalize load by 2010 load
df_load_2010 = df_combined[df_combined['year'] == 2010][['stateid', 'load']].rename(columns={'load':'load_2010'})
df_load = df_combined.merge(df_load_2010, on='stateid', how='left')
df_load['loadmult'] = df_load['load'] / df_load['load_2010']
df_load.drop(columns=['load_2010','load'], inplace=True)

# Fill in future years using a value of 1.0 for loadmult
future_years = pd.DataFrame({
    'year': range(2024, 2051)
}).assign(key=1)

unique_states = df_load[['stateid']].drop_duplicates().assign(key=1)

df_future = pd.merge(future_years, unique_states, on='key').drop(columns=['key'])

# Fill loadmult with with the value from lastyear
lastyear_loadmult = df_load[df_load['year'] == lastyear][['stateid', 'loadmult']]
df_future = df_future.merge(lastyear_loadmult, on='stateid', how='left')

df_load2 = pd.concat([df_load, df_future], ignore_index=True)

# Map states to census divisions
df_load3 = df_load2.merge(st_cendiv, on='stateid', how='left').dropna()

# Remove spaces from cendiv names and make lower case for merging
df_load3['cendiv'] = df_load3['cendiv'].str.strip().str.lower()
demand_ratios['cendiv'] = demand_ratios['cendiv'].str.strip().str.lower()

# Merge with demand ratios
df_loadtot = df_load3.merge(demand_ratios, left_on=['year', 'cendiv'], right_on=['year', 'cendiv'], how='left')

# Check stateid = "ND" for verification
df_check = df_loadtot[df_loadtot['stateid'] == 'ND'].copy()

# Combine loadmult and ratio to get final ratio
df_loadtot['multiplier'] = df_loadtot['loadmult'] * df_loadtot['ratio']
df_loadtot.drop(columns=['loadmult','ratio','cendiv'], inplace=True)
# Rename stateid to r
df_loadtot.rename(columns={'stateid':'r'}, inplace=True)
# Reorder columns
df_loadtot = df_loadtot[['r', 'year', 'scenario', 'multiplier']].copy()


# Split out into different scenarios
df_low = df_loadtot[df_loadtot['scenario']=='Low Economic Growth'].copy()
df_ref = df_loadtot[df_loadtot['scenario']=='Reference Case'].copy()
df_high = df_loadtot[df_loadtot['scenario']=='High Economic Growth'].copy()

# Drop scenario column
df_low.drop(columns=['scenario'], inplace=True)
df_ref.drop(columns=['scenario'], inplace=True)
df_high.drop(columns=['scenario'], inplace=True)

# Plot the multipliers by r for each scenario (optional)
for scenario, df in zip(['Low', 'Reference', 'High'], [df_low, df_ref, df_high]):
    plt.figure(figsize=(10,6))
    for r in df['r'].unique():
        df_r = df[df['r'] == r]
        plt.plot(df_r['year'], df_r['multiplier'], label=r)
    plt.title(f'Demand Multipliers by Region - {scenario} Scenario')
    plt.xlabel('Year')
    plt.ylabel('Demand Multiplier')
    plt.legend(title='Region', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid()
    plt.tight_layout()
    plt.show()

# Save to csv files

# Create Outputs directory if it doesn't exist
Output_folder = 'Outputs'
if not os.path.exists(Output_folder):
    os.makedirs(Output_folder)

df_low.to_csv(os.path.join(Output_folder, 'demand_AEO_{}_low.csv').format(AEO_year), index=False)
df_ref.to_csv(os.path.join(Output_folder, 'demand_AEO_{}_reference.csv').format(AEO_year), index=False)
df_high.to_csv(os.path.join(Output_folder, 'demand_AEO_{}_high.csv').format(AEO_year), index=False)