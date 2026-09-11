import pandas as pd
import os
import sys
import geopandas as gpd
#reeds_path = os.path.expanduser('~/github/ReEDS')
reeds_path = os.path.expanduser('~/Documents/Github/ReEDS/ReEDS/')
sys.path.append(reeds_path)
import reeds

def calculate_h2_county_share(dfdemand, dfcounty):
    dfdemand = gpd.GeoDataFrame(
        dfdemand,
        geometry=gpd.points_from_xy(dfdemand.longitude, dfdemand.latitude),
        crs="EPSG:4326"
    )

    h2_county_share = (
        gpd.overlay(
            dfcounty,
            dfdemand.to_crs(dfcounty.crs),
            how='intersection',
            keep_geom_type=False
        )
        .groupby('rb', as_index=False)
        ['total_demand_kg']
        .sum()
        .rename(columns={'rb': '*r'})
    )
    # Only consider locations with at least 1 gram of H2 demand
    h2_county_share = (
        h2_county_share.loc[h2_county_share['total_demand_kg'] >= 1e-3]
        .copy()
        .reset_index(drop=True)
    )
    h2_county_share['fraction'] = (
        h2_county_share['total_demand_kg']
        / h2_county_share['total_demand_kg'].sum()
    )
    h2_county_share = h2_county_share.drop(columns='total_demand_kg')

    return h2_county_share

def main():
    dfcounty = reeds.spatial.get_map('county', source='tiger')
    dfcounty['FIPS'] = dfcounty.index.values
    dfcounty['rb'] = 'p' + dfcounty['FIPS']
    state_fips = pd.read_csv(
        os.path.join(reeds_path, 'inputs', 'shapefiles', 'state_fips_codes.csv'),
        dtype={'state_fips': str},
        index_col='state_fips',
    ).rename(columns={'state':'STATE', 'state_code':'STCODE'})[['STATE', 'STCODE']]
    dfcounty = dfcounty.merge(state_fips, left_on='STATEFP', right_index=True, how='left')

    # H2 demand estimates by geographic location
    # (source: https://data.openei.org/submissions/5655)
    h2_demand = pd.read_csv(
        'data/Hydrogen_Economic_Potential_Supply_Demand.csv'
    )
    h2_demand_2021 = (
        h2_demand.loc[h2_demand['scenario'] == 'Reference']
        .copy()
        .reset_index(drop=True)
    )
    # For 2021, we don't consider light-duty vehicles,
    # biofuels, or methanol demand
    h2_demand_2021['total_demand_kg'] -= (
        h2_demand_2021['demand_ldv_kg']
        + h2_demand_2021['demand_biofuel_kg']
        + h2_demand_2021['demand_methanol_kg']
    )
    h2_demand_2050 = (
        h2_demand.loc[h2_demand['scenario'] == 'Low Cost Electrolysis']
        .copy()
        .reset_index(drop=True)
    )

    h2_county_share_2021 = calculate_h2_county_share(h2_demand_2021, dfcounty)
    h2_county_share_2050 = calculate_h2_county_share(h2_demand_2050, dfcounty)
    h2_county_share = pd.concat([
        h2_county_share_2021.assign(t=2021),
        h2_county_share_2050.assign(t=2050)
    ])
    h2_county_share = h2_county_share[['*r', 't', 'fraction']]

    out_fpath = 'outputs/h2_county_share.csv'
    if not os.path.exists(out_fpath):
        os.makedirs(out_fpath)

    h2_county_share.to_csv(out_fpath)
    print(f"Run complete. See {out_fpath} for outputs.")

if __name__ == "__main__":
    main()