#%%### Imports
import os
import sys
import cmocean
import numpy as np
import pandas as pd
from tqdm import tqdm
import geopandas as gpd
import matplotlib as mpl
import matplotlib.pyplot as plt
## Local imports
sys.path.append(os.path.expanduser('~/github/TSC'))
import tsc
sys.path.append(os.path.expanduser('~/github/ReEDS'))
import reeds

## https://github.nrel.gov/cobika/DLR
dlrpath = os.path.expanduser('~/github/DLR')
sys.path.append(dlrpath)
import dlr.helpers


#%%### Functions
def get_naris_nodes():
    """Copied from tsc.network.get_network_naris()"""
    ### Filepaths
    filepaths = tsc.network.get_filepaths()
    buspath = filepaths['NARIS', 'bus']
    nodetypepath = filepaths['NARIS', 'nodetype']
    ercotloadnodepath = filepaths['NARIS', 'ercotloadnodes']

    ### Buses
    dfin_nodes = (
        pd.read_csv(buspath, na_values=['#REF!','#NAME?'])
        .dropna(subset=['PLEXOS_Node'])
        .rename(columns={
            'Voltage':'kV',
            'Longitude':'longitude',
            'Latitude':'latitude',
        })
        .set_index('PLEXOS_Node')
    )

    ##### Node types
    dfnodetype = pd.read_csv(nodetypepath, index_col='Node')
    ## Clean up zone names for GAMS
    dfnodetype['Region'] = dfnodetype['Region'].str.replace('.','')
    ### ERCOT has a separate file
    ercotloadnodes = pd.read_csv(ercotloadnodepath).Name.values
    dfnodetype.loc[dfnodetype.index.isin(ercotloadnodes),'Load Flag'] = True

    ### Add types to buses
    def gettype(row):
        if row['Load Flag'] and (not row['Generator Flag']):
            return 'load'
        elif (not row['Load Flag']) and row['Generator Flag']:
            return 'gen'
        elif (not row['Load Flag']) and (not row['Generator Flag']):
            return 'trans'
        else:
            return 'genload'

    dfnodetype['bus_type'] = dfnodetype.apply(gettype, axis=1)
    dfin_nodes['bus_type'] = dfin_nodes.index.map(dfnodetype['bus_type']).fillna('genload')
    dfin_nodes['LPF'] = dfin_nodes.index.map(dfnodetype['Load Participation Factor'])
    dfin_nodes['LPF_region'] = dfin_nodes.index.map(dfnodetype['Region'])

    ### Get PLEXOS regional load
    load_plexos = {
        i:
        pd.read_csv(
            filepaths.loc['NARIS', f'load_{i}'],
            index_col='DATETIME', parse_dates=True,
        )
        for i in ['EI','WI','TI']
    }
    load_plexos = pd.concat(load_plexos, axis=1)
    load_plexos.columns = load_plexos.columns.get_level_values(1)

    ### Get the peak
    peak_hour = load_plexos.sum(axis=1).nlargest(1).index[0]
    peakload_plexos = load_plexos.loc[peak_hour]

    ### Get load by node
    dfin_nodes['MW'] = dfin_nodes.LPF_region.map(peakload_plexos) * dfin_nodes.LPF

    ### Now do it for ERCOT
    ### All texas load is in peakload_plexos['ERCOT_ERC']
    lpf_ercot = pd.read_csv(
        filepaths['NARIS', 'ercotlpf'],
        index_col='Name',
    ### Get the LPF for the same month used in the other regions
    )[f"M{peak_hour.month:02}"]
    # print(lpf_ercot.sum())
    load_ercot = lpf_ercot * peakload_plexos['ERCOT_ERC']

    dfin_nodes.loc[dfin_nodes.index.isin(load_ercot.index), 'MW'] = (
        dfin_nodes.loc[dfin_nodes.index.isin(load_ercot.index)].index.map(load_ercot)
    )

    dfin_nodes.loc[dfin_nodes.index.isin(load_ercot.index), 'LPF'] = (
        dfin_nodes.loc[dfin_nodes.index.isin(load_ercot.index)].index.map(lpf_ercot)
    )

    ### Add geographic information
    dfnodes = dfin_nodes.copy()
    dfnodes = tsc.tscplots.df2gdf(dfnodes)

    return dfnodes


#%%### Procedure
#%% Get counties
dfcounty = gpd.read_file(
    os.path.join(reeds.io.reeds_path, 'inputs', 'shapefiles', 'US_county_2022'),
).set_index('FIPS').drop(columns=['rb'], errors='ignore')

### Get states (just for plots)
dfmap = reeds.io.get_dfmap()

#%% Get nodes
dfnodes = get_naris_nodes()

#%% Assign nodes to counties
assert dfnodes.crs == dfcounty.crs
## Drop nodes with missing location
dfclean = dfnodes.loc[
    (~dfnodes.latitude.isnull())
    & (~dfnodes.longitude.isnull())
    & (dfnodes.longitude < 0)
    & (dfnodes.latitude > 0)
].copy()

## Only match nodes up to 100m away from county boundaries (to help with shorelines but
## not include much of Canada)
dfclean = (
    dfclean
    .sjoin_nearest(dfcounty[['STCODE','geometry']], how='left', max_distance=100)
    .rename(columns={'index_right':'FIPS'})
)

#%%
fips_load = dfclean.groupby('FIPS').MW.sum()
dfcounty['MW'] = fips_load
print(f'matched: {dfcounty.MW.sum():.0f}\ntotal: {dfclean.MW.sum():.0f}')


#%% Take a look
plt.close()
f,ax = plt.subplots(figsize=(12,9))
dfcounty.plot(ax=ax, column='MW', cmap=cmocean.cm.rain, vmin=0)
# dfcounty.plot(ax=ax, facecolor='none', edgecolor='k', lw=0.1, zorder=1e6)
ax.axis('off')
plt.show()

#%% Get county fraction by state
stateload = dfcounty.groupby('STCODE').MW.sum()
dfcounty['state_fraction'] = dfcounty.MW / dfcounty.STCODE.map(stateload)

#%% Take a look
plt.close()
f,ax = plt.subplots(figsize=(12,9))
dfcounty.plot(ax=ax, column='state_fraction', cmap=cmocean.cm.rain, vmin=0)
# dfcounty.plot(ax=ax, facecolor='none', edgecolor='k', lw=0.1, zorder=1e6)
ax.axis('off')
plt.show()

#%% Counties with no LPF
plt.close()
f,ax = plt.subplots(figsize=(12,9))
dfcounty.loc[dfcounty.state_fraction == 0].plot(ax=ax, facecolor='C3', edgecolor='w', lw=0.4)
dfcounty.plot(ax=ax, facecolor='none', edgecolor='C7', lw=0.05, zorder=1e6)
dfmap['st'].plot(ax=ax, facecolor='none', edgecolor='k', lw=0.4, zorder=1e7)
ax.axis('off')
plt.show()

#%% Write it
dfwrite = (
    dfcounty[['STCODE','state_fraction']]
    .rename(columns={'STCODE':'state'})
    .round(6)
    .fillna(0)
    .sort_index()
)
## Make sure it sums to 1
assert dfwrite.groupby('state').state_fraction.sum().map(lambda x: np.isclose(x, 1)).all()
## Write it
dfwrite.to_csv(os.path.expanduser('~/Desktop/LPF_county_state_NARIS.csv'))


#%%### Counties with no NARIS transmission

#%%### Counties with no HIFLD transmission
dfhifld = dlr.helpers.get_lines(
    min_kv=0, max_miles=np.inf,
    remove_dc=False, remove_underground=False,
)

#%% Any overlap
### Group HIFLD by voltage
voltage_counts = dfhifld.VOLTAGE.value_counts()
dfhifld['voltage_rounded'] = dfhifld.rep_voltage.astype(int).astype(str)
dfhifld.loc[dfhifld.VOLTAGE < 69, 'voltage_rounded'] = '<69'
print(dfhifld.voltage_rounded.value_counts())

dfhifld_voltage = dfhifld.dissolve('voltage_rounded').loc[
    ['<69', '69', '115', '230', '345', '500', '765']
].copy()

colors = reeds.plots.rainbowmapper(dfhifld_voltage.index.tolist())

#%% Take a look
lw = {
    '<69': 0.1,
    '69': 0.25,
    '115': 0.5,
    '230': 1,
    '345': 1.5,
    '500': 2,
    '765': 3,
}
plt.close()
f,ax = plt.subplots(figsize=(12,9))
dfmap['st'].plot(ax=ax, facecolor='none', edgecolor='k', lw=0.4, zorder=1e7)
ax.set_xlim(*ax.get_xlim())
ax.set_ylim(*ax.get_ylim())
for kv in lw:
    dfhifld_voltage.loc[[kv]].plot(
        ax=ax, color=colors[kv], lw=lw[kv], label=kv,
    )
handles = [
    mpl.patches.Patch(facecolor=color, label=key)
    for key, color in colors.items()
]
ax.legend(
    handles=handles, loc='lower right', bbox_to_anchor=(0.95, 0.05),
    fontsize='large', frameon=False,
    handletextpad=0.3, handlelength=0.7,
    title='kV', title_fontsize='large',
)
ax.axis('off')
plt.savefig(os.path.expanduser(f"~/Desktop/HIFLD-{','.join(lw.keys())}"))
plt.show()


#%% Get overlap between voltage groups and counties
county_voltage = {}
for kv, row in tqdm(dfhifld_voltage.iterrows(), total=len(dfhifld_voltage)):
    county_voltage[kv] = (~dfcounty.intersection(row.geometry).is_empty).astype(int)

county_voltage = pd.concat(county_voltage, axis=1)

#%%
highest_voltage = county_voltage.replace(0,np.nan).copy()
highest_voltage = highest_voltage.apply(
    lambda row: row.dropna().index[-1] if row.sum() else 'none', axis=1
)

highest_voltage.value_counts()

#%% Only counties with missing data
plt.close()
f,ax = plt.subplots(figsize=(12,9))
dfcounty.loc[highest_voltage.loc[highest_voltage=='none'].index].plot(
    ax=ax, facecolor='C3', edgecolor='w', lw=0.4,
)
dfcounty.plot(ax=ax, facecolor='none', edgecolor='C7', lw=0.05, zorder=1e6)
dfmap['st'].plot(ax=ax, facecolor='none', edgecolor='k', lw=0.4, zorder=1e7)
ax.axis('off')
plt.show()

#%% Colored by highest-voltage line
plt.close()
f,ax = plt.subplots(figsize=(12,9))
for kv, color in colors.items():
    dfcounty.loc[highest_voltage.loc[highest_voltage==kv].index].plot(
        ax=ax, facecolor=color, edgecolor='w', lw=0.4,
    )
dfcounty.plot(ax=ax, facecolor='none', edgecolor='C7', lw=0.05, zorder=1e6)
dfmap['st'].plot(ax=ax, facecolor='none', edgecolor='k', lw=0.4, zorder=1e7)
handles = [
    mpl.patches.Patch(facecolor=color, label=key, edgecolor='C7' if color=='w' else 'none')
    for key, color in {**{'none':'w'}, **colors}.items()
]
ax.legend(
    handles=handles, loc='lower right', bbox_to_anchor=(0.95, 0.05),
    fontsize='large', frameon=False,
    handletextpad=0.3, handlelength=0.7,
    title='Max kV', title_fontsize='large',
)
ax.axis('off')
plt.show()


#%%### Counties with no OSM transmisssion
