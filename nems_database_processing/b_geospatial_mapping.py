"""
Updated Aug 1 2026

This script matches the units' long/lats to the FIPS that contain them.
For units that are unmatched (typically offshore wind), they are mapped to the nearest FIPS.
"""
import sys
import os
import pandas as pd
import numpy as np
import geopandas as gpd

#%%
dir = os.getcwd()
reeds_path = sys.argv[1]

# For testing
#reeds_path = '~/Documents/GitHub/ReEDS/public_ReEDS/ReEDS/'               # local
#reeds_path = '//kfs2/projects/stdscen/apham/ReEDS/'                       # kestrel

reeds_path = os.path.expanduser(reeds_path)
sys.path.append(reeds_path)
import reeds

gdbinputname = 'a_to_b.csv'
gdboutname   = 'b_to_c.csv'

#%%--------------------------------------------------------------------------------
#  Mapping County to Generator using lat/lon coordinates - geopandas
#----------------------------------------------------------------------------------
print("Starting b_geopsatial_mapping.py")

data_raw = pd.read_csv(os.path.join('outputs','intermediate_outputs',gdbinputname), low_memory=False)
data_raw_columns = list(data_raw.columns.values).copy()
merge_columns = data_raw_columns.copy()
merge_columns.append('FIPS')

# If long is positive, make it negative
data_raw['T_LONG'] = data_raw['T_LONG'].replace(r'^\s*$', np.nan, regex=True)
data_raw['T_LONG'] = data_raw['T_LONG'].astype(float)
data_raw['T_LAT'] = data_raw['T_LAT'].replace(r'^\s*$', np.nan, regex=True)
data_raw['T_LAT'] = data_raw['T_LAT'].astype(float)
data_raw.loc[(data_raw['T_LONG']>0),'T_LONG'] = -data_raw['T_LONG']

# Check if all units have long/lats
data_raw_no_lon_lat = data_raw[(data_raw['T_LONG'].isna()) | (data_raw['T_LAT'].isna()) |
                               (data_raw['T_LONG']==0) | (data_raw['T_LAT']==0)]

# If some units do not have long/lats, manually add the long/lat for these units 
# in user_adjusted_units_missing_lon_lats.csv
if (len(data_raw_no_lon_lat) > 0):
    print("Some units do not have lon/lat, so matching them to user-adjusted lon/lat data:")
    if (os.path.isfile(os.path.join('inputs','user_adjusted_units_missing_lon_lats.csv'))):
        adjusted_missing_unit = pd.read_csv(os.path.join('inputs','user_adjusted_units_missing_lon_lats.csv'))
        data_raw_w_long_lat = data_raw[(data_raw['T_LONG'].notna()) & (data_raw['T_LAT'].notna()) & 
                                       (data_raw['T_LONG']!=0) & (data_raw['T_LAT']!=0)]
        data_raw = pd.concat([data_raw_w_long_lat, adjusted_missing_unit],ignore_index=True)
        print('Finish matching user-defined lon/lats to units without them')
        # Check again if all units have long/lats
        data_raw_no_lon_lat = data_raw[(data_raw['T_LONG'].isna()) | (data_raw['T_LAT'].isna()) |
                                       (data_raw['T_LONG']==0) | (data_raw['T_LAT']==0)]
        if (len(data_raw_no_lon_lat) > 0):
            raise ValueError("Some units still have missing long/lat data, " \
            "please manually add their long/lat to user_adjusted_units_missing_lon_lats.csv")
            sys.exit()
    # If user_adjusted_units_missing_lon_lats does not exist
    else:
        data_raw_no_lon_lat.to_csv(os.path.join(dir,'inputs', 'user_adjusted_units_missing_lon_lats.csv'), index=False)
        raise ValueError("Some units are missing long/lat data, " \
        "please manually add their long/lat to user_adjusted_units_missing_lon_lats.csv")
        sys.exit()

## Map long/lat to county and FIPS
# read county shapefile directly from census
print("Mapping long/lat to FIPS")
crs = 'EPSG:5070'
county_data = reeds.spatial.get_map('county', source='tiger').to_crs(crs)
## Format for ReEDS
county_data['FIPS'] = county_data.index.values
county_data['rb'] = 'p' + county_data['FIPS']
# Spatial join units' long/lat with county and FIPS:
data_raw_geo = reeds.plots.df2gdf(
    data_raw,
    lat='T_LAT',
    lon='T_LONG',
    crs=crs)
nems_county_merged = gpd.sjoin(data_raw_geo, county_data, how="left", predicate="within")
nems_county_merged['FIPS'] = nems_county_merged['rb']
nems_county_merged = nems_county_merged[merge_columns]

# Filter out rows that still do not have a county:
nems_county_merged_matched = nems_county_merged[nems_county_merged['FIPS'].notna()].copy()
nems_county_merged_unmatched = nems_county_merged[nems_county_merged['FIPS'].isna()].copy()
# For long/lat points that do not match to any county (typically offshore wind units),
# assign them to the nearest counties 
# (using EPSG:5070 for nearest distance calculation)
nems_county_merged_unmatched_geo = reeds.plots.df2gdf(
        nems_county_merged_unmatched,
        lat='T_LAT',
        lon='T_LONG',
        crs=crs)
county_data_5070 = county_data.to_crs(crs)
nems_county_unmatched = gpd.sjoin_nearest(nems_county_merged_unmatched_geo, county_data_5070, how='left')
nems_county_unmatched['FIPS'] = nems_county_unmatched['rb']
nems_county_unmatched = nems_county_unmatched[merge_columns]
# Check to make sure there is no unmatched long/lat
if len(nems_county_unmatched[nems_county_unmatched['FIPS'].isna()]) > 0:
    raise ValueError("Some long/lats are still not matched to their nearest counties")
    sys.exit()

## Concating matched and unmatched nems files:
nems_county_final = pd.concat([nems_county_merged_matched,nems_county_unmatched],ignore_index=True)

## Some Manual fixes of FIPS for a few units to avoid infeasibility in county-run
# Move all units from p01077 to p01033 
nems_county_final.loc[(nems_county_final['FIPS']=='p01077'),
                        ['FIPS','T_LAT','T_LONG']] = ['p01033',34.744,-87.849]
# Move hydro units from from p30055 (McCone county) to p30105 (Valley county)
nems_county_final.loc[((nems_county_final['T_PID']==6623) | (nems_county_final['T_PID']=='6623')) & 
                        (nems_county_final['tech']=='hydro'),
                        ['FIPS','T_LAT','T_LONG']] = ['p30105',48.3498496,-106.6704238]
# Move hydro units from McLean to Mercer county
nems_county_final.loc[((nems_county_final['T_PID']==2815) | (nems_county_final['T_PID']=='2815')) & 
                        (nems_county_final['tech']=='hydro'),
                        ['FIPS','T_LAT','T_LONG']] = ['p38057',47.3071473,-101.8333279]
# Move hydro units from Jackson to Gadsden county
nems_county_final.loc[((nems_county_final['T_PID']==690) | (nems_county_final['T_PID']=='690')) & 
                        (nems_county_final['tech']=='hydro'), 
                        ['FIPS','T_LAT','T_LONG']] = ['p12039',30.5786866,-84.6126072]
nems_county_final["Unique ID"] = nems_county_final.index

# =========================================================================
# Save output file:
# Check if all entries in the database have been mapped with appropriate FIPS codes
# If some entries in the database do not have matching FIPS, print out message to fix this issue
nems_no_FIPS = nems_county_final[nems_county_final['FIPS'].isna()]
if len(nems_no_FIPS) > 0:
    raise ValueError(f"{len(nems_no_FIPS)} entries in the unit database still do not have matching FIPS codes.")
    
# Otherwise, if all units in the database have been mapped with FIPS,
# proceed to save the final database
else:
    print('All entries in the unit database have been mapped to appropriate FIPS codes.')
    # =========================================================================
    # Save output file:
    print('Unit database updated')
    nems_county_final.to_csv(os.path.join(dir,'outputs','intermediate_outputs', gdboutname), index=False)
    # =========================================================================

    print("Finished b_geopsatial_mapping.py")
