#%%### Imports
import os
import sys
import shapely
import datetime
import pandas as pd
from glob import glob
# import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
import geopandas as gpd

sys.path.append(os.path.expanduser('~/github/ReEDS'))
import reeds
# sys.path.append(os.path.expanduser('~/github/DLR'))
# import dlr.helpers

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(repo_path)
import preprocessing


#%%### Inputs
crs = 'EPSG:5070'

#%%### Procedure
#%% Get the offshore zones
offshore_zones = gpd.read_file(
    os.path.join(reeds.io.reeds_path, 'inputs', 'shapefiles', 'offshore_zones.gpkg')
).set_index('zone').to_crs(crs).drop(columns=['zone_old'], errors='ignore')
## Get node x/y for consistency with land-based zones
xy = reeds.plots.df2gdf(
    offshore_zones.drop(columns='geometry'),
    lat='node_latitude',
    lon='node_longitude',
    crs=crs,
)
offshore_zones['node_x'] = xy.geometry.x
offshore_zones['node_y'] = xy.geometry.y

#%% Get the default zones and all the aggregations
dfba = reeds.io.get_zonemap().to_crs(crs)

hierarchy_paths = [
    i for i in
    sorted(glob(os.path.join(reeds.io.reeds_path, 'inputs', 'hierarchy*.csv')))
    if 'offshore' not in i
]
hierarchies = {}
for hierarchy_path in hierarchy_paths:
    hierarchy = (
        pd.read_csv(hierarchy_path, index_col=0)
        .drop(columns=['st_interconnect'], errors='ignore')
    )
    hierarchy = hierarchy.loc[hierarchy.country.str.lower() == 'usa'].copy()
    key = (
        os.path.splitext(os.path.basename(hierarchy_path))[0]
        .replace('hierarchy','').strip('_')
    )
    key = ('agg132' if key == '' else key)
    hierarchies[key] = hierarchy
## Also include the base
hierarchies['base'] = hierarchies['agg132']
hierarchies['base']['aggreg'] = hierarchies['base'].index.values


#%% Aggregate and take a look
dfzoneses = {}
for key, hierarchy in hierarchies.items():
    ## Aggregate it
    df = dfba.to_crs(crs).copy()
    df['aggreg'] = df.index.map(hierarchy.aggreg)
    df = df.dissolve('aggreg')
    ## Get the node
    dfnodes = gpd.GeoSeries(
        {
            r: preprocessing.spatial.get_node(row.geometry)
            for r, row in df.iterrows()
        },
        crs=dfba.crs
    )
    df['node_x'] = dfnodes.x
    df['node_y'] = dfnodes.y
    dflatlon = dfnodes.to_crs('EPSG:4326')
    df['node_lat'] = dflatlon.geometry.y
    df['node_lon'] = dflatlon.geometry.x
    ## Get the old nodes
    dfold = gpd.GeoSeries(
        df.apply(lambda row: shapely.Point(row.x, row.y), axis=1),
        crs='ESRI:102008',
    ).to_crs(crs)
    df['old_node_x'] = dfold.x
    df['old_node_y'] = dfold.y

    dfzoneses[key] = df.copy()
    ## Plot it
    plt.close()
    f,ax = plt.subplots(figsize=(12,9))
    df.plot(ax=ax, facecolor='0.95', edgecolor='k')
    for r, row in df.iterrows():
        highlight = True if not r.startswith('p') else False
        ax.annotate(
            r, (row.node_x, row.node_y), ha='center', va='center',
            fontsize=9,
            color=('C3' if highlight else 'k'),
            weight=('bold' if highlight else 'normal'),
            path_effects=[pe.withStroke(linewidth=1.5, foreground='w', alpha=0.9)]
        )
    ax.set_title(key, weight='bold', fontsize=14, y=0.9)
    ax.axis('off')
    plt.show()

### TX_MISO is different between agg54 and agg69, so can't just use the zone name
### Instead, label with {hierarchy key}|{zone name} and then drop duplicates
### (including node location)

#%% Look at the shift in node location
for key, dfzones in dfzoneses.items():
    plt.close()
    f,ax = plt.subplots(figsize=(12,9))
    dfzones.dissolve().plot(ax=ax, facecolor='none', edgecolor='k', lw=1, zorder=1e6)
    dfzones.plot(ax=ax, facecolor='0.9', edgecolor='w')
    ax.plot(dfzones.old_node_x, dfzones.old_node_y, lw=0, marker='s', color='C0', label='old')
    ax.plot(dfzones.node_x, dfzones.node_y, lw=0, marker='o', color='C3', label='new')
    for r, row in dfzones.iterrows():
        ax.plot(
            [row.old_node_x, row.node_x],
            [row.old_node_y, row.node_y],
            c='k', lw=0.5,
        )
        # highlight = True if not r.startswith('p') else False
        # ax.annotate(
        #     r, (row.node_x, row.node_y), ha='center', va='center',
        #     fontsize=9,
        #     color=('C3' if highlight else 'k'),
        #     weight=('bold' if highlight else 'normal'),
        #     path_effects=[pe.withStroke(linewidth=1.5, foreground='w', alpha=0.9)]
        # )
    ax.legend(
        frameon=False, loc='lower left', bbox_to_anchor=(0.1, 0.1), fontsize='large',
        handletextpad=0.3, handlelength=0.7,
    )
    ax.set_title(key, weight='bold', fontsize=14, y=0.9)
    ax.axis('off')
    plt.show()


#%% Get states with offshore zones
offshore_zones['state'] = offshore_zones.index.map(lambda x: x.replace('o','').split('_')[0])
offshore_states = offshore_zones.state.unique()
## Specify offshore backbone links
offshore_links = [
    ['oWA', 'oOR_N'],
    ['oOR_N', 'oOR_S'],
    ['oOR_S', 'oCA_NNN'],
    ['oCA_NNN', 'oCA_NN'],
    ['oCA_NN', 'oCA_N'],
    ['oCA_N', 'oCA_S'],
    ['oTX', 'oLA'],
    ['oLA', 'oMS'],
    ['oMS', 'oAL'],
    ['oAL', 'oFL_W'],
    ['oFL_E', 'oGA'],
    ['oGA', 'oSC'],
    ['oSC', 'oNC_S'],
    ['oNC_S', 'oNC_N'],
    ['oNC_N', 'oVA'],
    ['oVA', 'oMD'],
    ['oMD', 'oDE'],
    ['oDE', 'oNJ'],
    ['oNJ', 'oNY'],
    ['oNY', 'oCT'],
    ['oCT', 'oRI'],
    ['oRI', 'oMA'],
    ['oMA', 'oNH'],
    ['oNY', 'oRI'],
    ['oNY', 'oMA'],
    ['oNH', 'oME'],
]

#%% For each zone, get all other zones
columns = [
    'start', 'end', 'start_lat', 'start_lon', 'end_lat', 'end_lon', 'voltage', 'polarity',
    'offshore',
]
zone_links = []
keys = ['base', 'agg132', 'agg125', 'agg69', 'agg54']
for key in keys:
    dfzones = dfzoneses[key]
    hierarchy = hierarchies[key].drop_duplicates().set_index('aggreg')
    for r in dfzones.index:
        state = hierarchy.loc[r, 'st']
        for rr in dfzones.index:
            zone_links.append({
                'key': key,
                'r': r,
                'rr': rr,
                'start': f'{key}|{r}',
                'end': f'{key}|{rr}',
                'start_lat': dfzones.loc[r, 'node_lat'],
                'start_lon': dfzones.loc[r, 'node_lon'],
                'end_lat': dfzones.loc[rr, 'node_lat'],
                'end_lon': dfzones.loc[rr, 'node_lon'],
                'offshore': 0,
            })

        ### Offshore zones
        ## Include connections from each offshore zone to each land-based zone in the same state
        if state in offshore_states:
            for rr in offshore_zones.loc[offshore_zones.state == state].index:
                zone_links.append({
                    'key': key,
                    'r': r,
                    'rr': rr,
                    'start': f'{key}|{r}',
                    'end': f'{key}|{rr}',
                    'start_lat': dfzones.loc[r, 'node_lat'],
                    'start_lon': dfzones.loc[r, 'node_lon'],
                    'end_lat': offshore_zones.loc[rr, 'node_latitude'],
                    'end_lon': offshore_zones.loc[rr, 'node_longitude'],
                    'offshore': 1,
                })

### Include offshore backbone connections
for r, rr in offshore_links:
    zone_links.append({
        'key': 'base',
        'r': r,
        'rr': rr,
        'start': f'base|{r}',
        'end': f'base|{rr}',
        'start_lat': offshore_zones.loc[r, 'node_latitude'],
        'start_lon': offshore_zones.loc[r, 'node_longitude'],
        'end_lat': offshore_zones.loc[rr, 'node_latitude'],
        'end_lon': offshore_zones.loc[rr, 'node_longitude'],
        'offshore': 2,
    })

zone_links = pd.DataFrame(zone_links)


#%% Drop duplicates
keep_links = zone_links.drop_duplicates(
    subset=['r','rr','start_lat','start_lon','end_lat','end_lon','offshore'],
    keep='first',
).copy()
keep_links = keep_links.loc[keep_links.r < keep_links.rr].copy()
keep_links['voltage'] = 500
keep_links = pd.concat(
    {polarity: keep_links for polarity in ['ac', 'dc']},
    names=('polarity','drop'),
).reset_index().drop(columns='drop')
keep_links = keep_links.drop(
    keep_links.loc[(keep_links.offshore >= 1) & (keep_links.polarity == 'ac')].index
).sort_values(['offshore','polarity'])

assert len(keep_links.count().unique()) == 1
keep_links.loc[keep_links.offshore > 0]
keep_links
keep_links.groupby('key').r.count()

#%% Write it
subset = [
    # 'base|p1',
    # 'base|p33',
    # 'base|p101',
    # 'base|p103',
    # 'base|p134',
]

today = datetime.datetime.now().strftime('%Y%m%d')
for polarity in ['ac', 'dc']:
    for offshore in range(3):
        dfwrite = keep_links.loc[
            (keep_links.offshore==offshore) & (keep_links.polarity == polarity),
            columns
        ].drop(columns='offshore')
        if len(subset):
            dfwrite = dfwrite.loc[dfwrite.start.isin(subset) | dfwrite.end.isin(subset)]
        savename = f'zone_links-{polarity}-offshore{offshore}-{today}.csv'
        print(f'{savename} ({len(dfwrite)})')
        dfwrite.to_csv(savename, index=False)
