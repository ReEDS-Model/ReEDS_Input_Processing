#%% Imports
import sys
import pandas as pd
import geopandas as gpd
from pathlib import Path
sys.path.append(str(Path('~/github/ReEDS').expanduser()))
import reeds

#%% Inputs
hurdlepath = (
    '/Volumes/ReEDS/FY24-StandardScenarios-PJG/'
    'hurdle_rates/ba_needs_map/ba_and_needs_regions.gpkg'
)
GSw_ZoneSet = 'z90'
crs = 'EPSG:5070'

#%%### Procedure
#%% Get zones
dfzones = reeds.io.get_zones(GSw_ZoneSet=GSw_ZoneSet).to_crs(crs)
#%% Get hurdle regions
dfhurdle = gpd.read_file(hurdlepath)
dfhurdle.geometry = dfhurdle.geometry.buffer(0.)
dfhurdle = dfhurdle.dissolve('Control_Ar').to_crs(crs)
## TODO: Align these names with the names currently used
dfhurdle['name'] = dfhurdle.index.str.replace(' ','_')
dfhurdle = dfhurdle.reset_index().set_index('name')

#%% Map each zone to the hurdle region it overlaps with the most
rmap = {}
for r, row in dfzones.iterrows():
    rmap[r] = dfhurdle.intersection(row.geometry).area.nlargest(1).index[0]
dfzones['hurdlereg'] = pd.Series(rmap)

#%% Clean up
replace = {
    'Midcontinent_ISO_(Balancing_Authority)': 'MISO',
    'Southwest_Power_Pool_(Balancing_Authority)': 'SPP',
    'California_Independent_System_Operator_(Balancing_Authority)': 'CAISO',
    'ERCOT_ISO_(Balancing_Authority)': 'ERCOT',
    'New_York_ISO_(Balancing_Authority)': 'NYISO',
    'New_England_ISO_(Balancing_Authority)': 'ISONE',
    'PJM_Interconnection': 'PJM',
}
dfzones['hurdlereg'] = (
    dfzones['hurdlereg'].replace(replace)
    .str.replace('&','and')
)
sorted(dfzones.hurdlereg.unique())

#%% Write it
hpath = Path(reeds.io.reeds_path, 'inputs', 'zones', GSw_ZoneSet, 'hierarchy.csv')
hierarchy = pd.read_csv(hpath, index_col='r')
hierarchy['hurdlereg'] = dfzones['hurdlereg']
hierarchy.to_csv(hpath)
