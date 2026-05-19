# -*- coding: utf-8 -*-
"""
This script:
1. Matches the units' long/lats to their counties and FIPS that contain them.
For units that are unmatched (typically offshore wind), they are mapped to the nearest counties.
2. Assign classes to pv, wind, and geothermal units.

@author: apham
"""
import sys
import os
import pandas as pd
import numpy as np
import math
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely.geometry import Point

#%%
dir = os.getcwd()
reeds_path = gdbnewname = sys.argv[1]

# For debugging
# reeds_path = '~/Documents/Github/ReEDS/'              # local
# reeds_path = '/kfs2/projects/stdscen/stdscens_fy25/ReEDS/' # kestrel

reeds_path = os.path.expanduser(reeds_path)
sys.path.append(reeds_path)
import reeds

gdbinputname = 'b_to_c.csv'
gdboutname   = 'c_to_d.csv'

def main():
    #%%--------------------------------------------------------------------------------
    #  Mapping County to Generator using lat/lon coordinates - geopandas
    #----------------------------------------------------------------------------------

    data_raw = pd.read_csv(os.path.join('Outputs',gdbinputname), low_memory=False)
    data_raw_columns = list(data_raw.columns.values).copy()
    merge_columns = data_raw_columns.copy()
    merge_columns.append("NAME")
    
    # If long is positive, make it negative
    data_raw.loc[(data_raw['T_LONG']>0),'T_LONG'] = -data_raw['T_LONG']
    
    # Check if all units have long/lats
    data_raw_no_lon_lat = data_raw[(data_raw['T_LONG'].isna()) | (data_raw['T_LAT'].isna())]

    if (len(data_raw_no_lon_lat) > 0) & (~os.path.isfile(os.path.join('Inputs','user_adjusted_units_missing_lon_lats.csv'))):
        print('\n ERROR: Some units are missing long/lat data, \n please manually add their long/lat to "user_adjusted_units_missing_lon_lats.csv"\n')
        data_raw_no_lon_lat.to_csv(os.path.join(dir,'Inputs', 'user_adjusted_units_missing_lon_lats.csv'), index=False)
        sys.exit()
    elif (len(data_raw_no_lon_lat) > 0) & (os.path.isfile(os.path.join('Inputs','user_adjusted_units_missing_lon_lats.csv'))):
        adjusted_missing_unit = pd.read_csv(os.path.join('Inputs','user_adjusted_units_missing_lon_lats.csv'))
        data_raw_w_long_lat = data_raw[(data_raw['T_LONG'].notna()) & (data_raw['T_LAT'].notna())]
        data_raw = pd.concat([data_raw_w_long_lat, adjusted_missing_unit])

        # Check again if all units have long/lats
        data_raw_no_lon_lat = data_raw[(data_raw['T_LONG'].isna()) | (data_raw['T_LAT'].isna())]
        if (len(data_raw_no_lon_lat) > 0):
            data_raw_no_lon_lat.to_csv(os.path.join(dir,'Inputs', 'user_adjusted_units_missing_lon_lats.csv'), index=False)
            print('\n ERROR: Some units are still missing long/lat data, \n please manually add ALL units with missing long/lat to "user_adjusted_units_missing_lon_lats.csv"\n')
            sys.exit()

    ## Map long/lat to county and FIPS
    # read county shapefile directly from census
    county_data = reeds.spatial.get_map('county', source='tiger').to_crs("EPSG:4326")
    ## Format for ReEDS
    county_data['FIPS'] = county_data.index.values
    county_data['rb'] = 'p' + county_data['FIPS']
    state_fips = pd.read_csv(
        os.path.join(reeds_path, 'inputs', 'shapefiles', 'state_fips_codes.csv'),
        dtype={'state_fips': str},
        index_col='state_fips',
    ).rename(columns={'state':'STATE', 'state_code':'STCODE'})[['STATE', 'STCODE']]
    county_data = county_data.merge(state_fips, left_on='STATEFP', right_index=True, how='left')
    #county_data.plot("FIPS", cmap="Set1")
    
    # Spatial join units' long/lat with county and FIPS:
    geometry = [Point(xy) for xy in zip(data_raw['T_LONG'], data_raw['T_LAT'])]
    data_raw_geo = GeoDataFrame(data_raw, crs='EPSG:4326', geometry=geometry)
    nems_county_merged = gpd.sjoin(data_raw_geo, county_data, how="inner", predicate="within")  
    nems_county_merged['FIPS'] = nems_county_merged['rb']
    nems_county_merged['TSTATE'] = nems_county_merged['STCODE']
    nems_county_merged['county'] = nems_county_merged['NAMELSAD']
    nems_county_merged = nems_county_merged[merge_columns].copy()

    # Filter out rows that still do not have a county:
    nems_county_merged_matched = nems_county_merged[nems_county_merged['county'].notna()]
    nems_county_merged_unmatched = nems_county_merged[nems_county_merged['county'].isna()]

    # For long/lat points that do not match to any county (typically offshore wind units),
    # assign them to the nearest counties:
    geometry = [Point(xy) for xy in zip(nems_county_merged_unmatched['T_LONG'], nems_county_merged_unmatched['T_LAT'])]
    nems_county_merged_unmatched = GeoDataFrame(nems_county_merged_unmatched, crs='EPSG:4326', geometry=geometry)
    nems_county_merged_unmatched = gpd.sjoin_nearest(nems_county_merged_unmatched, county_data, how='left')
    
    nems_county_merged_unmatched['FIPS'] = nems_county_merged_unmatched['rb']
    nems_county_merged_unmatched['TSTATE'] = nems_county_merged_unmatched['STCODE']
    nems_county_merged_unmatched['county'] = nems_county_merged_unmatched['NAMELSAD']
    nems_county_merged_unmatched['NAME'] = nems_county_merged_unmatched['NAME_right']
    nems_county_merged_unmatched = nems_county_merged_unmatched[merge_columns].copy()

    # Merge nems-county with reeds-ba:
    reeds_ba =  pd.read_csv(os.path.join('Inputs','county_to_reeds_region.csv'), low_memory=False).rename(columns={'county': 'NAME', 'state':'TSTATE'})
    nems_county_merged_matched = pd.merge(nems_county_merged_matched, reeds_ba, on=['NAME','TSTATE'], how='left')
    nems_county_merged_unmatched = pd.merge(nems_county_merged_unmatched, reeds_ba, on=['NAME','TSTATE'], how='left')
    data_raw_columns+=['reeds_ba','resource_region']
    nems_county_merged_matched = nems_county_merged_matched[data_raw_columns].copy()
    nems_county_merged_unmatched = nems_county_merged_unmatched[data_raw_columns].copy()

    ## Concating matched and unmatched nems files:
    nems_county_final = pd.concat([nems_county_merged_matched,nems_county_merged_unmatched])
    
    ## Some Manual Fix of FIPS codes for a few units to avoid infeasibility in county-run
    # Move all units from p01077 to p01033 
    nems_county_final.loc[(nems_county_final['FIPS']=='p01077') & (nems_county_final['county']=='Lauderdale County') &
                     (nems_county_final['TSTATE']=='AL'),
                     ['FIPS','county','T_LAT','T_LONG','reeds_ba']] = ['p01033','Colbert County',34.744,-87.849,'p89']
    # Move hydro units from p35 to p19
    nems_county_final.loc[((nems_county_final['T_PID']==6623) | (nems_county_final['T_PID']=='6623')) & (nems_county_final['county']=='McCone County') &
                     (nems_county_final['TSTATE']=='MT') & (nems_county_final['tech']=='hydro') & (nems_county_final['reeds_ba']=='p35'),
                     ['FIPS','county','reeds_ba']] = ['p30105','Valley County','p19']
    # Move hydro units from p37 to p36
    nems_county_final.loc[((nems_county_final['T_PID']==2815) | (nems_county_final['T_PID']=='2815')) & (nems_county_final['county']=='McLean County') &
                     (nems_county_final['TSTATE']=='ND') & (nems_county_final['tech']=='hydro') & (nems_county_final['reeds_ba']=='p37'),
                     ['FIPS','county','reeds_ba']] = ['p38057','Mercer County','p36']
    # Move hydro units from p91 to p101
    nems_county_final.loc[((nems_county_final['T_PID']==690) | (nems_county_final['T_PID']=='690')) & (nems_county_final['county']=='Jackson County') &
                     (nems_county_final['TSTATE']=='FL') & (nems_county_final['tech']=='hydro') & (nems_county_final['reeds_ba']=='p91'),
                     ['FIPS','county','reeds_ba']] = ['p12039','Gadsden County','p101']
    nems_county_final["Unique ID"] = nems_county_final.index

    # Check if all entries in the database have been mapped with appropriate FIPS codes
    # In this case, proceed to update tech classes
    nems_no_FIPS = nems_county_final[nems_county_final['FIPS'].isna()]
    if len(nems_no_FIPS) == 0:
        print('\nAll {} entries in the unit database have been mapped with FIPS codes!'\
            .format(len(nems_county_final)))
        
        #%%--------------------------------------------------------------------------------
        #  Update upv, wind, and geothermal classes
        #----------------------------------------------------------------------------------
        print('Begin mapping upv, wind, and geothermal units to their appropriate capacity factors/temps')
        nems_county_final['reV_mean_resource_temp'] = np.nan
        nems_county_final['sc_point_gid'] = np.nan
        # Get directory for supply curve data from either nrelnas (only option for now) or zenodo (coming soon)
        if sys.platform == 'win32':
            remotepath = '/nrelnas01/ReEDS/Supply_Curve_Data'
        elif sys.platform == 'darwin':
            remotepath = '/Volumes/ReEDS/Supply_Curve_Data'         #TODO: Move supply curves to zenodo

        # match directory between reV and ReEDS technologies
        tech_match = {
            "upv": ["upv","pvb_pv","csp-wp","csp-ns"],      # upv matches upv, pv, pvb_pv, csp-ns and csp-wp
            "wind-ons": ["wind-ons"], 
            "wind-ofs": ["wind-ofs"],        
            "geohydro": ["geohydro_allkm", "geothermal"]
        }

        # Assign resource classes for each technology in tech_list
        for tech_reV in list(tech_match.keys()):
            for tech_element in tech_match[tech_reV]:
                nems_county_tech_class = assign_class_tech(nems_county_final,tech_reV,tech_element,remotepath)
                nems_county_tech_class.to_csv((os.path.join(dir, "Outputs","df_assigned_"+tech_element+".csv"))) 

        # =========================================================================
        # Save output file:
        nems_county_tech_class.drop(columns=['Unique ID'],inplace=True)
        print('Unit database updated:')
        nems_county_tech_class.to_csv(os.path.join(dir,'Outputs', gdboutname), index=False)
        # =========================================================================

    # If some entries in the database do not have matching FIPS, print out message to fix this issue
    else:
        print('\nSome {} entries in the unit database still do not have matching FIPS codes.'\
            .format(len(nems_no_FIPS)))
        print('Please fix this issue by adding these units to user_adjusted_units_missing_lon_lats.csv')


#%% ===========================================================================
### --- FUNCTIONS ---
### ===========================================================================

## Calculate the great-circle distance between two points on the Earth (in kilometers) using the Haversine formula.
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

## Find the nearest point in points_df to the reference latitude and longitude.
## Returns the point's ID and the distance.
def find_nearest_point(reference_lat, reference_lon, points_df):
    points_df['distance'] = points_df.apply(
        lambda row: haversine(reference_lat, reference_lon, row['latitude'], row['longitude']),
        axis=1
    )
    nearest_point = points_df.loc[points_df['distance'].idxmin()]
    return nearest_point['sc_point_gid'], nearest_point['distance']
## Prepare the supply curve data for a given technology.
## Loads the appropriate supply curve file, assigns nearest supply curve id, and returns a capacity factor AC for all technologies except,
## geohydro, which use a resource temperature with point IDs, class, latitude, and longitude
def prep_supply_curve(tech,tech_element,remotepath):
    rev_file = pd.read_csv(os.path.join(reeds_path,'inputs/supply_curve/rev_paths.csv'))
    print('Preparing supply curves for ' + tech_element)
    # For geohydro and egs, the access case are reference and class definition based on site mean resource temperature.
    if tech in ['geohydro']:
        rev_file_part=rev_file[(rev_file['tech'] == tech) & (rev_file['access_case'] == 'reference')]
        class_def='mean_resource_temp'
    # For upv and onshore wind, the access case is open and class definition is based on resource.
    else:
        rev_file_part=rev_file[(rev_file['tech'] == tech) & (rev_file['access_case'] == 'open')]

    # For geohydro and egs, the access case are reference and class definition based on site mean resource temperature.
    if tech in ['geohydro']:
        class_def='mean_resource_temp'
    # For upv and onshore wind, the access case is open and class definition is based on resource.
    else:
        class_def='capacity_factor_ac'

    # Load the supply curve file for the technology
    df = pd.read_csv(os.path.join(remotepath,rev_file_part['sc_path'].iloc[0],f"{tech}_{rev_file_part['access_case'].iloc[0]}_ba","results",f"{tech}_supply_curve_raw.csv" ))

    # Select relevant columns and convert longitude to negative if needed
    df_sc=df[['sc_point_gid','latitude','longitude','state','county',class_def]].copy()
    df_sc['longitude'] = df_sc['longitude'] * -1  # Convert longitude to negative if needed
    # Assign resource class to each point
    df_sc.to_csv(os.path.join(dir, "Outputs","df_sc_"+tech+".csv"))
    return(df_sc)

def assign_class_tech(df,tech_reV,tech_element,remotepath):
    # Filter generators for the given technology
    df_exist = df[df['tech'].str.contains(tech_element, case=False, na=False)]
    df_exist = df_exist[['tech','TSTATE', 'county', 'T_LAT', 'T_LONG', 'T_PID', 'Unique ID']].reset_index(drop=True).copy()

    df_exist['T_LONG'] = df_exist['T_LONG'].abs()  # Ensure longitude is positive for distance calculation
    df_sc_point = prep_supply_curve(tech_reV,tech_element,remotepath)
    for i in range(len(df_exist)):
        print('{} out of {} {} units'.format(i, len(df_exist), tech_reV))  # Progress indicator
        # Find unique ID of this point:
        unique_id = df_exist['Unique ID'][i]
        print(unique_id)
        # Find nearest supply curve point
        nearest_id, nearest_distance = find_nearest_point(df_exist['T_LAT'][i], abs(df_exist['T_LONG'][i]), df_sc_point)

        if tech_reV in ['geohydro', 'egs']:
            # Find the mean temp of the nearest_id:
            mean_temp = df_sc_point[df_sc_point['sc_point_gid']==nearest_id]['mean_resource_temp'].iloc[0]
            # Assign the mean temp to the unit in NEMS database using unique ID:
            df.loc[df['Unique ID'] == unique_id, 'reV_mean_resource_temp'] = mean_temp

        # Make sure the right tech_rev is matched and the right T_PID is matched:
        assert(tech_element in df[df['Unique ID'] == df_exist.loc[i, 'Unique ID']]['tech'].iloc[0])
        assert(df_exist[df_exist['Unique ID'] == unique_id]['T_PID'].iloc[0] == df[df['Unique ID']==unique_id]['T_PID'].iloc[0])
        
        # Assign sc_point_gid as the nearest_id:
        df.loc[df['Unique ID'] == unique_id, 'sc_point_gid'] = nearest_id
    return df    

 # %% ===========================================================================

main()

