
import sys
import os
import pandas as pd
import numpy as np
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely.geometry import Point

# Get NEMS database from ReEDS repo
reeds_path = os.path.expanduser('~/Documents/GitHub/ReEDS/public_ReEDS/ReEDS')
sys.path.append(reeds_path)
import reeds

# Main switches
data_to_generate = 'emission_rate'                # Generate cost multiplier by fip ('cost_multiplier')
                                                    # or coal emission rate by fip ('emission_rate')
recent_years_used = 5                              # Only for 'cost_multiplier', the number of EIA923 years used
                                                    # to estimate post-2024 cost multiplier                                     
AEO_ver = 2025                                      # Only for 'cost_multiplier', AEO version to estimate cost multiplier

dir = os.getcwd()
savepath = os.path.join(dir,"outputs")
os.makedirs(savepath, exist_ok=True)

# Read county map:
county_data = reeds.spatial.get_map('county', source='tiger')
county_data['FIPS'] = county_data.index.values
county_data['rb'] = 'p' + county_data['FIPS']
state_fips = pd.read_csv(
    os.path.join(reeds_path, 'inputs', 'shapefiles', 'state_fips_codes.csv'),
    dtype={'state_fips': str},
    index_col='state_fips',
).rename(columns={'state':'STATE', 'state_code':'STCODE'})[['STATE', 'STCODE']]
county_data = county_data.merge(state_fips, left_on='STATEFP', right_index=True, how='left')
county_data2 = county_data.to_crs("EPSG:5070")

def main(data_to_generate, recent_years_used, AEO_ver):

    # Read eGrid emission data by type
    emission_rate = pd.read_excel(os.path.join('inputs','egrid2023_data_rev2.xlsx'),sheet_name='PLNT23')
    emission_rate.columns = emission_rate.iloc[0]       # lb/MMBtu
    emission_rate = emission_rate[1:]
    emission_rate = emission_rate[emission_rate['PLFUELCT']=='GAS']
    emission_rate = emission_rate[['PLNOXRA','PLSO2RA','PLCO2RA','PLCH4RA','PLN2ORA','LAT','LON']]
    emission_rate = emission_rate.rename(columns={'PLNOXRA':'NOx',
                                                    'PLSO2RA':'SO2',
                                                    'PLCH4RA':'CH4',
                                                    'PLCO2RA':'CO2',
                                                    'PLN2ORA':'N2O'})
    # Convert lb/MMBtu to metric ton/MMBtu (ReEDS emission rate input unit)
    metric_ton_to_lb = 2205
    emission_rate['NOx'] /= metric_ton_to_lb
    emission_rate['SO2'] /= metric_ton_to_lb
    emission_rate['CH4'] /= metric_ton_to_lb
    emission_rate['CO2'] /= metric_ton_to_lb
    emission_rate['N2O'] /= metric_ton_to_lb
    
    # Drop rows with emission rate of 0 or NaN
    cols = ['NOx','SO2','CH4','CO2','N2O']
    emission_rate.loc[emission_rate[cols].eq(0).all(axis=1)] = np.nan
    emission_rate = emission_rate.dropna()
    emission_rate = emission_rate[emission_rate['CO2']>0]
    emission_rate = emission_rate[emission_rate['NOx']>0]
    emission_rate = emission_rate[emission_rate['SO2']>0]
    emission_rate = emission_rate[emission_rate['CH4']>0]
    emission_rate = emission_rate[emission_rate['N2O']>0]

    gdf_emission_rate = df2gdf(emission_rate.assign(T_LONG=-emission_rate.LON.abs()), lat='LAT', lon='LON')
    gdf_emission_rate = gdf_emission_rate.to_crs("EPSG:5070")

    fips_emission_rate = gpd.sjoin_nearest(gdf_emission_rate, county_data2, how='right')
    fips_emission_rate = fips_emission_rate.drop_duplicates(subset=['rb'], keep='first')
    fips_emission_rate['etype'] = 'process'
    fips_emission_rate = fips_emission_rate[['rb','etype','SO2','NOx','CO2','N2O','CH4']]

    # Read emission rate data from ReEDS repo
    emitrate_reeds = pd.read_csv(os.path.join(reeds_path,'inputs','emission_constraints','emitrate.csv'))
    emitrate_reeds = emitrate_reeds[emitrate_reeds['etype']=='process']
    emitrate_coal_reeds = emitrate_reeds[(emitrate_reeds['i'].str.contains('coal')) |
                                            (emitrate_reeds['i'].str.contains('Coal')) |
                                            (emitrate_reeds['i'].str.contains('Cofire'))]
    
    # Use CoalOldUns as reference coal to calculate emission rate multiplier
    # (this is based on comparisons of fips_emission_rate['SO2'].mean() and
    # fips_emission_rate['NOx'].mean() to each coal type emission rate in emitrate_coal_reeds
    emitrate_coal_reeds_ref = emitrate_coal_reeds[emitrate_coal_reeds['i']=='CoalOldUns']
    emitrate_coal_reeds_CoalOldScr_mult = emitrate_coal_reeds[emitrate_coal_reeds['i']=='CoalOldScr']
    emitrate_coal_reeds_coal_new_mult = emitrate_coal_reeds[emitrate_coal_reeds['i']=='coal-new']
    emitrate_coal_reeds_coal_IGCC_mult = emitrate_coal_reeds[emitrate_coal_reeds['i']=='coal-IGCC']
    emitrate_coal_reeds_CofireOld_mult = emitrate_coal_reeds[emitrate_coal_reeds['i']=='CofireOld']
    emitrate_coal_reeds_CofireNew_mult = emitrate_coal_reeds[emitrate_coal_reeds['i']=='CofireNew']

    for e in ['SO2','NOx','CO2','N2O','CH4']:
        emitrate_coal_reeds_CoalOldScr_mult[e] /= emitrate_coal_reeds_ref[e].iloc[0]
        emitrate_coal_reeds_coal_new_mult[e] /= emitrate_coal_reeds_ref[e].iloc[0]
        emitrate_coal_reeds_coal_IGCC_mult[e] /= emitrate_coal_reeds_ref[e].iloc[0]
        emitrate_coal_reeds_CofireOld_mult[e] /= emitrate_coal_reeds_ref[e].iloc[0]
        emitrate_coal_reeds_CofireNew_mult[e] /= emitrate_coal_reeds_ref[e].iloc[0]

    # Calculate coal emission rate multiplier by FIPS
    fips_emission_rate_mult = fips_emission_rate
    for r in fips_emission_rate_mult['rb'].unique().tolist():
        for e in ['SO2','NOx','CO2','N2O','CH4']:
            fips_emission_rate_mult.loc[fips_emission_rate_mult['rb']==r,e] /= emitrate_coal_reeds_ref[e].iloc[0]
    fips_emission_rate_mult['i'] = 'CoalOldUns'
    fips_emission_rate_mult = fips_emission_rate_mult.reset_index()
    
    fips_emission_rate_CoalOldScr_mult = fips_emission_rate_mult.copy()
    fips_emission_rate_CoalOldScr_mult['i'] = 'CoalOldScr'
    fips_emission_rate_coal_new_mult = fips_emission_rate_mult.copy()
    fips_emission_rate_coal_new_mult['i'] = 'coal-new'
    fips_emission_rate_coal_IGCC_mult = fips_emission_rate_mult.copy()
    fips_emission_rate_coal_IGCC_mult['i'] = 'coal-IGCC'
    fips_emission_rate_CofireOld_mult = fips_emission_rate_mult.copy()
    fips_emission_rate_CofireOld_mult['i'] = 'CofireOld'
    fips_emission_rate_CofireNew_mult = fips_emission_rate_mult.copy()
    fips_emission_rate_CofireNew_mult['i'] = 'CofireNew'

    # Apply factors to other coal types
    for r in fips_emission_rate_mult['rb'].unique().tolist():
        for e in ['SO2','NOx','CO2','N2O','CH4']:
            fips_emission_rate_CoalOldScr_mult.loc[fips_emission_rate_CoalOldScr_mult['rb']==r,e] *= emitrate_coal_reeds_CoalOldScr_mult[e].iloc[0]
            fips_emission_rate_coal_new_mult.loc[fips_emission_rate_coal_new_mult['rb']==r,e] *= emitrate_coal_reeds_coal_new_mult[e].iloc[0]
            fips_emission_rate_coal_IGCC_mult.loc[fips_emission_rate_coal_IGCC_mult['rb']==r,e] *= emitrate_coal_reeds_coal_IGCC_mult[e].iloc[0]
            fips_emission_rate_CofireOld_mult.loc[fips_emission_rate_CofireOld_mult['rb']==r,e] *= emitrate_coal_reeds_CofireOld_mult[e].iloc[0]
            fips_emission_rate_CofireNew_mult.loc[fips_emission_rate_CofireNew_mult['rb']==r,e] *= emitrate_coal_reeds_CofireNew_mult[e].iloc[0]
    
    fips_emission_rate = pd.concat([fips_emission_rate_mult,fips_emission_rate_CoalOldScr_mult,fips_emission_rate_coal_new_mult,
                                    fips_emission_rate_coal_IGCC_mult,fips_emission_rate_CofireOld_mult,fips_emission_rate_CofireNew_mult])
    fips_emission_rate = fips_emission_rate.reindex()
    fips_emission_rate = fips_emission_rate[['i','rb','etype','SO2','NOx','CO2','N2O','CH4']]
    fips_emission_rate = fips_emission_rate.rename(columns={'rb':'r'})
    
    # Save emission rate data by FIPS
    os.makedirs(os.path.join(dir,'outputs'), exist_ok=True)
    fips_emission_rate.to_csv(os.path.join(dir,'outputs', 'emitrate_coal_mult.csv'), index=False)



def get_latlonlabels(df, lat=None, lon=None, columns=None):
    """Try to find latitude and longitude column names in a dataframe"""
    ### Specify candidate column names to look for
    lat_candidates = ['latitude', 'lat']
    lon_candidates = ['longitude', 'lon', 'long']
    if columns is None:
        columns = df.columns

    latlabel = None
    lonlabel = None

    if lat is not None:
        latlabel = lat
    else:
        for col in columns:
            if col.lower().strip() in lat_candidates:
                latlabel = col
                break

    if lon is not None:
        lonlabel = lon
    else:
        for col in columns:
            if col.lower().strip() in lon_candidates:
                lonlabel = col
                break

    return latlabel, lonlabel

def df2gdf(dfin, crs='EPSG:5070', lat=None, lon=None):
    """Convert a pandas dataframe with lat/lon columns to a geopandas dataframe of points"""
    ### Imports
    import os
    import geopandas as gpd
    import shapely
    os.environ['PROJ_NETWORK'] = 'OFF'

    ### Convert
    df = dfin.copy()
    latlabel, lonlabel = get_latlonlabels(df, lat=lat, lon=lon)
    df['geometry'] = df.apply(
        lambda row: shapely.geometry.Point(row[lonlabel], row[latlabel]), axis=1)
    df = gpd.GeoDataFrame(df, crs='EPSG:5070').to_crs(crs)

    return df

main(data_to_generate, recent_years_used, AEO_ver)

