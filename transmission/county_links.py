#%%### Imports
import os
import sys
import shapely
import numpy as np
import pandas as pd
from tqdm import tqdm
import geopandas as gpd

sys.path.append(os.path.expanduser('~/github/ReEDS'))
import reeds
sys.path.append(os.path.expanduser('~/github/DLR'))
import dlr.helpers
sys.path.append(os.path.expanduser('~/github/TSC'))
import tsc

reeds_path = reeds.io.reeds_path

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(repo_path)
import preprocessing


#%%### Inputs
crs = 'EPSG:5070'
min_kv = 0
dc_voltage = 500

#%%### Procedure
#%% Get the county shapefile and downselect to land area
dfcounty = gpd.read_file(
    os.path.join(reeds_path, 'inputs', 'shapefiles', 'US_county_2022'),
).set_index('rb').to_crs(crs)

dfmap = reeds.io.get_dfmap()
for key in dfmap:
    dfmap[key] = dfmap[key].to_crs(crs)
dfcounty.geometry = dfcounty.intersection(dfmap['country'].loc['USA','geometry'])

#%% Get the county nodes
step_meters = 100
# dfcounty['node'] = dfcounty.geometry.map(preprocessing.spatial.get_node)
nodes = {}
for r, row in tqdm(dfcounty.iterrows(), total=len(dfcounty)):
    try:
        nodes[r] = preprocessing.spatial.get_node(
            row.geometry,
            step_meters=step_meters,
        )
    except ValueError as err:
        raise ValueError(f'{r}: {err}')

#%% Get lat/lon
dfnodes = gpd.GeoSeries(nodes, crs=dfcounty.crs).rename('geometry').to_frame()
dflatlon = dfnodes.to_crs('EPSG:4326')
dflatlon['lat'] = dflatlon.geometry.y
dflatlon['lon'] = dflatlon.geometry.x
dfnodes = dfnodes.merge(dflatlon[['lat','lon']], left_index=True, right_index=True)

#%% Take a look
dfplot = dfcounty.copy()
dfplot['geometry'] = dfplot.geometry.simplify(1000)
m = dfplot.explore(style_kwds={'color':'black', 'fillOpacity':0.1, 'weight':2})
dfnodes.explore(m=m, color='red')
m.save(os.path.expanduser('~/Desktop/county_nodes.html'))



# #%% Debug
# plt.close()
# f,ax = plt.subplots(figsize=(10,10))
# dfcounty.loc[['p12087']].plot(ax=ax, facecolor='none', edgecolor='k')
# geom = dfcounty.loc[['p12087']].copy()
# geom.geometry = geom.geometry.buffer(-25000)
# geom.plot(ax=ax, facecolor='0.8', edgecolor='none')
# point = geom.copy()
# point.geometry = point.centroid
# point.plot(ax=ax, facecolor='none', edgecolor='C3', markersize=3)
# plt.show()


#%% Get the counties to connect
county_links = pd.concat(
    {
        polarity: pd.read_csv(
            os.path.join(
                reeds_path, 'inputs', 'transmission',
                f'transmission_distance_cost_500kV{polarity}_county.csv'
            )
        )[['r','rr']]
        for polarity in ['ac', 'dc']
    },
    names=('polarity', ),
).reset_index(level=1, drop=True).reset_index().sort_values(['polarity','r','rr'])

### Only keep counties
county_links = county_links.loc[
    county_links.r.isin(dfcounty.index)
    & county_links.rr.isin(dfcounty.index)
].copy()

### Only keep one copy of each interface
county_links['interface'] = county_links.apply(
    lambda row: '|'.join(sorted([row.r, row.rr])),
    axis=1,
)
county_links = (
    county_links.drop_duplicates(['polarity','interface'], keep='first')
    .drop(columns=['r','rr'])
)
county_links['start'] = county_links['interface'].map(lambda x: x.split('|')[0])
county_links['end'] = county_links['interface'].map(lambda x: x.split('|')[1])

#%% Get the starting and ending points
for end in ['start', 'end']:
    for dat in ['lat', 'lon']:
        county_links[f'{end}_{dat}'] = county_links[end].map(dfnodes[dat])


#%%### Now the trickier part: Get the highest-voltage line connecting each pair of counties
#%% Get the HIFLD lines
dfhifld = dlr.helpers.get_lines(min_kv=min_kv, max_miles=np.inf, min_miles=0).to_crs(crs)
dfhifld['voltage_rounded'] = dfhifld.VOLTAGE.fillna(0).map(tsc.costmodel_line.round_to_allowed)
print(dfhifld.voltage_rounded.value_counts())

#%% Boil each line down to just start and endpoints
def get_endpoints(geom):
    """
    Turn a linestring into a MultiPoint, keeping only the start and end
    IDs for testing:
    - MultiLineString:
        - 304506
    """
    if isinstance(geom, shapely.LineString):
        multipoint = shapely.geometry.MultiPoint(
            [geom.coords[0], geom.coords[-1]]
        )
    elif isinstance(geom, shapely.MultiLineString):
        ## Take the endpoints of every line in the MultiLineString
        pointlist = []
        for line in list(geom.geoms):
            pointlist.extend([line.coords[0], line.coords[-1]])
        multipoint = shapely.geometry.MultiPoint(pointlist)
    else:
        raise NotImplementedError(type(geom))
    return multipoint
    # try:
    #     return multipoint
    # except NotImplementedError as err:
    #     print(type(geom))
    #     raise NotImplementedError(err)


dflines = dfhifld[['VOLTAGE','VOLT_CLASS','voltage_rounded','geometry']].copy()
dflines['geometry'] = dflines.geometry.map(get_endpoints)

#%% Run it
## county_start = dfcounty.loc['p08035','geometry']
## county_end = dfcounty.loc['p08041','geometry']
numlinks = 1000
numlinks = len(county_links)
highest_voltage = {}
for i, row in tqdm(county_links.iloc[:numlinks].iterrows(), total=numlinks):
    if i in highest_voltage:
        continue
    ## Always use same assumption for DC
    if row.polarity.lower() == 'dc':
        highest_voltage[i] = dc_voltage
        continue
    county_start = dfcounty.loc[row.start, 'geometry']
    county_end = dfcounty.loc[row.end, 'geometry']
    county_both = county_start.union(county_end)
    ## Get lines with points in start, end, or both
    dflines_both = dflines.loc[~dflines.intersection(county_both).is_empty]
    ## If empty, stop here
    if dflines_both.empty:
        highest_voltage[i] = min_kv
        continue
    ## Get the lines that intersect each (to filter out lines within each zone)
    dflines_start = dflines.loc[~dflines.intersection(county_start).is_empty]
    dflines_end = dflines.loc[~dflines.intersection(county_end).is_empty]
    ## Spanning lines intersect both geometries
    dflines_spanning = dflines_both.loc[
        dflines_both.index.isin(dflines_start.index) & dflines_both.index.isin(dflines_end.index)
    ]
    ## Keep the highest voltage
    if dflines_spanning.empty:
        highest_voltage[i] = min_kv
        continue
    highest_voltage[i] = int(dflines_spanning.voltage_rounded.max())

county_links['voltage'] = pd.Series(highest_voltage)

#%% Write it
county_links[[
    'start','end','start_lat','start_lon','end_lat','end_lon','voltage','polarity'
]].round(9).to_csv(f'county_links-{numlinks}.csv.gz', index=False)


#%%### Postprocessing
#%% Take a look
county_links = pd.read_csv('county_links-398512.csv.gz')
county_links.voltage.value_counts()

#%% Clip the voltage to 138 kV on the low end and rewrite
min_kv = 138
dfwrite = county_links.copy()
dfwrite['voltage'] = dfwrite['voltage'].clip(lower=min_kv)
dfwrite['offshore'] = 0
dfwrite.voltage.value_counts()
dfwrite.to_csv(f'county_links-{min_kv}kv_floor.csv.gz', index=False)

# #%% Take a look
# county_links.loc[county_links.voltage > 0].sort_values('voltage')

# #%%
# row = 2
# row = 49
# row = 57
# plt.close()
# f,ax = plt.subplots(figsize=(10,10))
# dfcounty.loc[
#     [county_links.loc[row,'start'], county_links.loc[row,'end']]
# ].plot(ax=ax, facecolor='C7', edgecolor='k', lw=0.25)
# ax.set_xlim(*ax.get_xlim())
# ax.set_ylim(*ax.get_ylim())
# dfhifld.loc[dfhifld.voltage_rounded==county_links.loc[row,'voltage']].plot(
#     ax=ax, color='C3', lw=2,
# )

# plt.show()

#%%
# geom = dflines.loc['304506', 'geometry']

# out = []
# for line in list(geom.geoms):
#     out.extend([line.coords[0], line.coords[-1]])
# out = shapely.geometry.MultiPoint(out)


# #%%
# type(dflines.sample().geometry)
# dflines.geometry.map(lambda x: str(type(x))).value_counts()

# multilines = dflines.loc[
#     dflines.geometry.map(
#         lambda x: str(type(x)) == str(shapely.geometry.multilinestring.MultiLineString)
#     )
# ]

# #%% Take a look
# plt.close()
# for i, row in multilines.iterrows():
#     plt.close()
#     f,ax = plt.subplots()
#     dflines.loc[[i]].plot(ax=ax)
#     ax.set_title(i)
#     plt.show()



#%% For each pair of zones, get the highest-voltage connection between the zones;
### if not connected, use min_kv
