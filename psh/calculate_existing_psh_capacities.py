# -*- coding: utf-8 -*-
"""
Calculating Existing Pumped Storage Hydropower Capacities for ReEDS

This script takes in raw data received from Oak Ridge National Laboratory (ORNL) and 
calculates the county-level opeartional and pump capacities [MW] and max energy [MWh].

Most of the ORNL data is sourced from the Global Energy Storage Database (GESDB),
containing Rated Power, Energy, Duration, and lat/lon data. Counties are mapped to the 
ORNL data using a county-level CONUS shapefile before aggregating the data to county-level.

Procedures are adapted from the original version of this script developed by @kodiobika

@author: jvcarag
@date: 20260618 12:21
"""
#%%
import pandas as pd
import os
import geopandas as gpd
import sys
from shapely.geometry import Point
reeds_path = os.path.expanduser('~/github/ReEDS')
sys.path.append(reeds_path)
import reeds

#%% Paths
outdir_path = os.path.join(reeds_path, 'inputs', 'storage')

# Ingest county-level CONUS shapefile 
dfcounty = reeds.spatial.get_map('county').reset_index()
# Ingest operational data for existing PSH fleet
psh_data_in = pd.read_excel(
    os.path.join('data','GESDB_Projects_complete RS_v3_fromORNL.xlsx'),
    sheet_name='Summary'
)

#%% Convert site-level PSH data to geopandas dataframe
psh_data = (
    gpd.GeoDataFrame(
        psh_data_in,
        geometry=[
            Point(xy)
            for xy
            in zip(psh_data_in['Longitude'], psh_data_in['Latitude'])
        ]
    )
    .set_crs(epsg=4326)
)
# Spatially join with counties to determine each plant's county
psh_data = (
    psh_data.to_crs(dfcounty.crs)
    .sjoin(dfcounty[['GEOID', 'geometry']])
    .rename(columns={'GEOID': 'r'})
    .drop(
        columns=[
            'Latitude',
            'Longitude',
            'geometry',
            'index_right'
        ]
    )
)
# Add 'p' prefix to all FIPS in 'r' column
psh_data['r'] = 'p' + psh_data['r']

# Add tech and tech vintage columns (needed later in ReEDS), 
psh_data.insert(0, '*i', 'pumped-hydro')
psh_data.insert(0, 'v', 'init-1')
# Convert Rated Power to MW
psh_data['Rated Power (MW)'] = psh_data['Rated Power(kW)'] * 1e-3
# Create a duplicate column as a placeholder for pump capacity 
psh_data['Pump Capacity (MW)'] = psh_data['Rated Power (MW)'].copy()

# Calculate county-level operational capacity, pump capacity, and max energy
psh_data_out = (
        psh_data.rename(columns={
        'Title': 'station',
        'Rated Power (MW)': 'operational_capacity_MW',
        'Pump Capacity (MW)': 'pump_capacity_MW',
        'Energy (MWh)': 'max_energy_MWh'
    })
    [[
        '*i',
        'v',
        'r',
        'station',
        'operational_capacity_MW',
        'pump_capacity_MW',
        'max_energy_MWh'
    ]]
    .groupby(['*i', 'v', 'r'])
    .sum(numeric_only=True)
    .round(1)
)
#%% Output data to ReEDS inputs folder
psh_data_out.to_csv(os.path.join(outdir_path, 'cap_existing_psh.csv'))

print(f"Run complete. See {outdir_path} for outputs.")