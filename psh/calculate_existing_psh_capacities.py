import pandas as pd
import os
import geopandas as gpd
from shapely.geometry import Point

# Locations were recorded manually using
# https://hydroreform.org/on-your-river/ as a reference
station_name_location_map = {
    'Edward C Hyatt': '39.5388,-121.4855',
    'Hiwassee': '35.152,-84.178',
    'San Luis (W R Gianelli)': '37.069173,-121.077265',
    'Rocky River': '41.5725,-73.4445',
    'Wallace Dam': '33.3502,-83.1574',
    'Olivenhain-Hodges Storage Project': '33.058037,-117.118823',
    'Horse Mesa': '33.590552,-111.343907',
    'Helms': '37.0385,-118.9661',
    'Jocassee': '34.9606,-82.9183',
    'Mormon Flat': '33.55356,-111.442849',
    'John W Keys III (Grand Coulee)': '47.957511,-118.977323',
    'Bad Creek': '35.0075,-83.0181',
    'Lewiston Niagara': '43.1422,-79.0402',
    'Raccoon Mountain': '35.045,-85.40833',
    'Mount Elbert': '39.094197,-106.352331',
    'Castaic': '34.6443,-118.7643',
    'Flatiron': '40.418428,-105.374757',
    'ONeill': '37.098671,-121.047718',
    'Richard B Russell': '34.026023,-82.593974',
    'Thermalito Hydro Power Plant': '39.5252,-121.6012',
    'Muddy Run': '39.8169,-76.3011',
    'Blenheim Gilboa': '42.4451,-74.4384',
    'Salina': '36.2661,-95.1056',
    'Northfield Mountain': '42.6102,-72.4388',
    'Ludington': '43.895,-86.4283',
    'Seneca': '41.8388,-79.0059',
    'Big Creek (J S Eastwood)': '37.1596,-119.2449',
    'Bath County': '38.2266,-79.8237',
    'Fairfield': '34.3122,-81.3281',
    'Waddell': '33.8464,-112.2663',
    'Yards Creek': '41.0015,-75.0445',
    'Bear Swamp (Jack Cockwell)': '42.6838,-72.9603',
    'Taum Sauk': '37.5333,-90.8167',
    'Smith Mountain': '37.041,-79.5359',
    'Rocky Mountain': '34.355642,-85.304002',
    'Clarence Cannon': '39.524417,-91.643946',
    'Cabin Creek': '39.65,-105.7189',
    'Carters': '34.616677,-84.673061',
    'Degray': '34.22,-93.11'
}

def main():
    reeds_path = os.path.expanduser('~/github/ReEDS-2.0')
    outdir_path = os.path.join(reeds_path, 'inputs', 'storage')
    dfcounty = gpd.read_file(
        os.path.join(
            reeds_path,
            'inputs',
            'shapefiles',
            'US_COUNTY_2022'
        )
    )

    # Read operational data for existing PSH fleet
    psh_data = pd.read_excel(
        'data/IHA US PSH for NREL.xlsx',
        sheet_name='Operational'
    )
    # Add location information
    psh_data['location'] = (
        psh_data['Station Name\xa0'].map(station_name_location_map)
    )
    psh_data[['latitude', 'longitude']] = (
        psh_data['location'].str.split(',', expand=True)
    )
    psh_data = (
        gpd.GeoDataFrame(
            psh_data,
            geometry=[
                Point(xy)
                for xy
                in zip(psh_data['longitude'], psh_data['latitude'])
            ]
        )
        .set_crs(epsg=4326)
    )
    # Spatially join with counties to determine each plant's county
    psh_data = (
        psh_data.to_crs(dfcounty.crs)
        .sjoin(dfcounty[['rb', 'geometry']])
        .rename(columns={'rb': 'r'})
        .drop(
            columns=[
                'location',
                'latitude',
                'longitude',
                'geometry',
                'index_right'
            ]
        )
    )
    # Add tech and tech vintage columns (needed later in ReEDS)
    # and get operational capacities, pump capacities,
    # and max energies, and calculate totals for each county
    psh_data.insert(0, '*i', 'pumped-hydro')
    psh_data.insert(0, 'v', 'init-1')
    psh_data = (
            psh_data.rename(columns={
            'Station Name\xa0': 'station',
            'Operational Capacity (MW)\xa0': 'operational_capacity_MW',
            'Pump Capacity': 'pump_capacity_MW',
            'Gen Cap * Duration (MWh)': 'max_energy_MWh'
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
    psh_data.to_csv(os.path.join(outdir_path, 'cap_existing_psh.csv'))

    print(f"Run complete. See {outdir_path} for outputs.")

if __name__ == "__main__":
    main()