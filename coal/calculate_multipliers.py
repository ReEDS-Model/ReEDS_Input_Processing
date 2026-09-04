
import sys
import os
import pandas as pd
import numpy as np
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely.geometry import Point

# Get NEMS database from ReEDS-2.0 repo
#reeds_path = os.path.expanduser('~/Documents/Github/ReEDS/ReEDS-2.0')
reeds_path = os.path.expanduser('~/Documents/GitHub/ReEDS/public_ReEDS/ReEDS')
sys.path.append(reeds_path)
import reeds

# Main switches
data_to_generate = 'emission_rate'                # Generate cost multiplier by cendiv ('cost_multiplier')
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

    if data_to_generate == 'cost_multiplier':
        # read current coal price from AEO:
        if AEO_ver == 2025:
            aeo_filename = 'coal_AEO_2025_reference.csv'
        elif AEO_ver == 2023:
            aeo_filename = 'coal_AEO_2023_reference.csv'
        else:
            raise ValueError(f"Version {AEO_ver} is not available")

        aeo_coal_price_all = pd.read_csv(os.path.join(reeds_path,'inputs','fuelprices',aeo_filename))

        # Read in fuel cost data:
        df_fuel_cost = pd.read_csv(os.path.join(dir, "outputs", "coal_prices_by_fips_mmbtu_all.csv"))
        
        if recent_years_used == 5:
            yearset = [2020,2021,2022,2023,2024]
        elif recent_years_used == 10:
            yearset = [2015,2016,2017,2018,2019,2020,2021,2022,2023,2024]
        elif recent_years_used == 15:
            yearset = [2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023,2024]

        # Join existing coal units with county level and 
        # assign counties without coal units to the units closest to them
        fips_cendiv_fuel_price_list = []
        for t in df_fuel_cost['Year'].unique().tolist():
            print(t)
            dfunits = df_fuel_cost[df_fuel_cost['Year']==t]
            gdfunits = df2gdf(dfunits.assign(T_LONG=-dfunits.T_LONG.abs()), lat='T_LAT', lon='T_LONG')

            dfunits_joint = gpd.sjoin(county_data, gdfunits, predicate="contains")
            
            # Assign remaining FIPS to nearest coal plant
            fips_with_coal_plants = dfunits_joint.to_crs("EPSG:5070")

            # Assign remaining FIPS to nearest coal plant
            fips_with_coal_plants = fips_with_coal_plants.rename(columns={'index_left': 'old_left_index', 'index_right': 'old_right_index'})
            fips_without_coal_plants = gpd.sjoin_nearest(fips_with_coal_plants, county_data2, how='right')
            fips_without_coal_plants = fips_without_coal_plants.rename(columns={'rb_right': 'rb'})
            fips_without_coal_plants = fips_without_coal_plants.drop_duplicates(subset=['rb'], keep='first')
            fips_without_coal_plants = fips_without_coal_plants[['Year','rb','T_LAT','T_LONG','Total_Fuel_Cost','Quantity']]
            fips_without_coal_plants = fips_without_coal_plants.rename(columns={'rb': 'FIPS'})

            # Merge county2zone and hierarchy:
            county2zone = pd.read_csv(os.path.join(reeds_path,'inputs','county2zone.csv'))
            hierarchy = pd.read_csv(os.path.join(reeds_path,'inputs','hierarchy.csv'))

            fips_cendiv = county2zone.merge(hierarchy,on='ba',how='left')
            fips_cendiv = fips_cendiv[['FIPS','ba','cendiv']]
            fips_cendiv['FIPS'] = fips_cendiv['FIPS'].astype(str).str.zfill(5)
            fips_cendiv['FIPS'] = 'p' + fips_cendiv['FIPS']

            # Merge fuel price:
            fips_cendiv_fuel_price = fips_cendiv.merge(fips_without_coal_plants,on='FIPS',how='left')
            fips_cendiv_fuel_price = fips_cendiv_fuel_price.groupby(['cendiv'], as_index=False).agg({'Total_Fuel_Cost': 'sum', 'Quantity': 'sum'})
            fips_cendiv_fuel_price['Weighted_Fuel_Cost'] = fips_cendiv_fuel_price['Total_Fuel_Cost']/fips_cendiv_fuel_price['Quantity']

            # read current coal price from AEO:
            aeo_coal_price = aeo_coal_price_all[aeo_coal_price_all['year']==t]

            fips_cendiv_fuel_price['AEO_Cost'] = 0
            for c in fips_cendiv_fuel_price['cendiv'].unique().tolist():
                fips_cendiv_fuel_price.loc[fips_cendiv_fuel_price['cendiv']==c,'AEO_Cost'] = aeo_coal_price[c].iloc[0]

            fips_cendiv_fuel_price['multiplier'] = fips_cendiv_fuel_price['Weighted_Fuel_Cost']/fips_cendiv_fuel_price['AEO_Cost']  
            fips_cendiv_fuel_price['year'] = t  
            fips_cendiv_fuel_price_list = fips_cendiv_fuel_price_list + [fips_cendiv_fuel_price] 
        
        fips_cendiv_historical = pd.concat(fips_cendiv_fuel_price_list, ignore_index=False, sort=False) 
        
        # Post-2024:
        yearset_post2024 = list(np.arange(2025, 2051, 1))
        fips_cendiv_post2024 = fips_cendiv_historical[fips_cendiv_historical['year'].isin(yearset)]
        fips_cendiv_post2024 = fips_cendiv_post2024.groupby(['cendiv'], as_index=False).agg({'Total_Fuel_Cost': 'sum', 'Quantity': 'sum'})
        
        aeo_coal_price_2025 = aeo_coal_price_all[aeo_coal_price_all['year']==AEO_ver]

        fips_cendiv_post2024['AEO_Cost'] = 0
        for c in fips_cendiv_post2024['cendiv'].unique().tolist():
            fips_cendiv_post2024.loc[fips_cendiv_post2024['cendiv']==c,'AEO_Cost'] = aeo_coal_price_2025[c].iloc[0]

        fips_cendiv_post2024['Weighted_Fuel_Cost'] = fips_cendiv_post2024['Total_Fuel_Cost']/ fips_cendiv_post2024['Quantity']
        fips_cendiv_post2024['multiplier'] = fips_cendiv_post2024['Weighted_Fuel_Cost']/fips_cendiv_post2024['AEO_Cost'] 
        
        fips_cendiv_post2024_list = []
        for t in yearset_post2024:
            fips_cendiv_post2024_t = fips_cendiv_post2024.copy()
            fips_cendiv_post2024_t['year'] = t
            fips_cendiv_post2024_list = fips_cendiv_post2024_list + [fips_cendiv_post2024_t]
        
        fips_cendiv_post2024 = pd.concat(fips_cendiv_post2024_list, ignore_index=False, sort=False)   

        fips_cendiv_historical_final = fips_cendiv_historical[['cendiv','year','multiplier']]  
        fips_cendiv_post2024_final = fips_cendiv_post2024[['cendiv','year','multiplier']]  

        fips_cendiv = pd.concat([fips_cendiv_historical_final,fips_cendiv_post2024_final])
        fips_cendiv_pivot = fips_cendiv.pivot_table(values='multiplier', index='year',
                                                                columns='cendiv').fillna(0.0)
        fips_cendiv_pivot.index.name = 'year'
        fips_cendiv_pivot.to_csv(os.path.join('outputs','coal_AEO_'+str(AEO_ver)+'_reference_multiplier_'+str(recent_years_used)+'.csv'))

    elif data_to_generate == 'emission_rate':
        # Read eGrid emission data by type
        emission_rate = pd.read_excel(os.path.join('inputs','egrid2023_data_rev2.xlsx'),sheet_name='PLNT23')
        emission_rate.columns = emission_rate.iloc[0]       # lb/MMBtu
        emission_rate = emission_rate[1:]
        emission_rate = emission_rate[emission_rate['PLFUELCT']=='COAL']
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

