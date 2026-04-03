#%% Imports
import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import os, sys, math, site, shutil, importlib
from tqdm import tqdm, trange
from glob import glob

import h5py
import geopandas as gpd
import shapely
os.environ['PROJ_NETWORK'] = 'OFF'

reedspath = os.path.expanduser('~/github/ReEDS-2.0/')
reedspath2 = os.path.expanduser('~/github2/ReEDS-2.0/')
remotepath = '/Volumes/ReEDS'
scpath = os.path.join(remotepath,'Supply_Curve_Data')
repopath = os.path.dirname(os.path.dirname(__file__))

pd.options.display.max_rows = 20
pd.options.display.max_columns = 200

site.addsitedir(reedspath)
site.addsitedir(os.path.join(reedspath,'input_processing'))
import reeds
from reeds import plots
plots.plotparams()
import hourly_writetimeseries

#%% ReEDS inputs
endpoints = (
    gpd.read_file(os.path.join(reedspath,'inputs','shapefiles','transmission_endpoints'))
    .set_index('ba_str'))
endpoints['x'] = endpoints.centroid.x
endpoints['y'] = endpoints.centroid.y

dfba = gpd.read_file(os.path.join(reedspath,'inputs','shapefiles','US_PCA')).set_index('rb')
dfba['labelx'] = dfba.geometry.centroid.x
dfba['labely'] = dfba.geometry.centroid.y
dfba['x'] = dfba.index.map(endpoints.x)
dfba['y'] = dfba.index.map(endpoints.y)
dfba.st = dfba.st.str.upper()

### Aggregate to states
dfstates = dfba.dissolve('st')

#%%### Supply curve inputs
cspsc_in = gpd.read_file(
    os.path.join(
        repopath,'csp','vision_sn2_csp_conus_2012-sc_point_gid.gpkg'))
# ### No-exclusions 11.5 x 11.5 km grid cells
# revsites = pd.read_csv(
#     os.path.join(
#         remotepath,'Users','pbrown','reV','transmission_run',
#         'transmission_run_sc.csv'))
# revsites = plots.df2gdf(revsites)
# revsites['x'] = revsites.geometry.x
# revsites['y'] = revsites.geometry.y

# ### CSP profiles points
# cspprofiles = pd.read_csv(os.path.join(scpath,'CSP','reV','csp_meta.csv.gz'))
# cspprofiles = plots.df2gdf(cspprofiles)
# cspprofiles['x'] = cspprofiles.geometry.x
# cspprofiles['y'] = cspprofiles.geometry.y


#%%### Profiles
cspcf = pd.read_hdf(
    os.path.expanduser('~/Projects/reV/csp/cspcf_est.h5')
)

#%%### Procedure
### Get average CF
sc_point_gid_cf = cspcf.mean()
cspsc = cspsc_in.copy()
cspsc['cf'] = cspsc.sc_point_gid.map(sc_point_gid_cf)

#%% Take a look
plt.close()
f,ax = plt.subplots()
ax.hist(cspsc.cf, weights=cspsc.capacity_mw/1e3, bins=100)
ax.set_ylabel('CSP capacity [GW]')
ax.set_xlabel('Capacity factor [fraction]')
plt.show()

#%% Try some cutoffs
# classes = {3:0.31, 2:0.27, 1:0.23}
# classes = {3:0.305, 2:0.275, 1:0.245}
# classes = {3:0.305, 2:0.27, 1:0.24}
# classes = {3:0.305, 2:0.275, 1:0.25}
# classes = {3:0.305, 2:0.275, 1:0.24}
## Decided-upon cutoff
classes = {3:0.315, 2:0.26, 1:0.23}

cspsc['newclass'] = 0
for k,v in classes.items():
    cspsc.loc[cspsc.cf<v, 'newclass'] = k

#%% Plot it
cmap = plt.cm.viridis_r
cmap = plt.cm.gist_earth_r
vmin = 0.5
vmax = max(classes.keys())
col = 'newclass'

plt.close()
f,ax = plt.subplots(figsize=(12,9))
dfstates.plot(ax=ax, facecolor='none', edgecolor='k', lw=0.5, zorder=1e6)
cspsc.plot(
    ax=ax, column=col, lw=0, marker='s', markersize=0.7,
    cmap=cmap, vmin=vmin, vmax=vmax)

plots.addcolorbarhist(
    f=f, ax0=ax, data=cspsc[col].values,
    title=col, cmap=cmap,
    vmin=vmin, vmax=vmax,
    orientation='horizontal', labelpad=2.1, cbarbottom=-0.06,
    cbarheight=0.3, log=False,
    nbins=21, histratio=2,
    ticklabel_fontsize=20, title_fontsize=24,
    extend='neither',
)

ax.axis('off')
plt.show()

### CF histogram
plt.close()
f,ax = plt.subplots()
ax.hist(cspsc.cf, weights=cspsc.capacity_mw/1e3, bins=100)
for k,v in classes.items():
    if 0 < v < 1:
        ax.axvline(v, c='k', ls=':', lw=1.5)
ax.set_ylabel('CSP capacity [GW]')
ax.set_xlabel('Capacity factor [fraction]')
# ax.xaxis.set_major_locator(mpl.ticker.MultipleLocator(0.02))
plt.show()

### Capacity in resulting bins
plt.close()
f,ax = plt.subplots()
ax.hist(
    cspsc.cf, weights=cspsc.capacity_mw/1e3,
    bins=[0.15]+sorted(list(classes.values())),
    edgecolor='k', lw=0.5,
)
ax.set_ylabel('CSP capacity [GW]')
ax.set_xlabel('Capacity factor [fraction]')
ax.xaxis.set_major_locator(mpl.ticker.MultipleLocator(0.02))
plt.show()

print(cspsc.groupby('newclass').capacity_mw.sum()/1e3)

print('class: number of r134 regions')
for c in classes.keys():
    print('{}: {}'.format(c, len(cspsc.loc[cspsc.newclass==c,'rb'].unique())))
print('(class,region) combos:', cspsc[['rb','newclass']].drop_duplicates().shape[0])

# ### Ratio compared to CF in SC file
# plt.close()
# f,ax = plt.subplots()
# dfstates.plot(ax=ax, facecolor='none', edgecolor='k', lw=0.1, zorder=1e6)
# # cspsc.loc[cspsc['cf'] / cspsc['capacity_factor'] < 0.5].plot(ax=ax, column='cf')
# df = cspsc.copy()
# df['ratio'] = cspsc['cf'] / cspsc['capacity_factor']
# df.plot(
#     ax=ax, column='ratio', lw=0, marker='s', markersize=0.3,
#     cmap=plt.cm.seismic, vmin=0.5, vmax=1.5)
# ax.axis('off')
# plt.show()
# cspsc.sc_point_gid.value_counts().value_counts()

#%% Get supply-curve costs from open-access PV
scpaths = pd.read_csv(
    os.path.join(reedspath,'inputs','supply_curve','rev_paths.csv'),
    index_col=['tech','access_case'],
)
pvscen = os.path.join(
    scpath,
    scpaths.loc[('upv','open'), 'sc_path'],
    'reV',
    scpaths.loc[('upv','open'), 'original_sc_file'],
)
pvsc = pd.read_csv(pvscen, index_col='sc_point_gid')
pvsc = plots.df2gdf(pvsc)
pvsc['x'] = pvsc.geometry.x
pvsc['y'] = pvsc.geometry.y

#%% Get nearest sc_point_gid in cases with no match
missing = list(set([i for i in cspsc.sc_point_gid if i not in pvsc.index]))
lookup = (
    cspsc.loc[cspsc.sc_point_gid.isin(missing)]
    .drop_duplicates('sc_point_gid').set_index('sc_point_gid'))
out = []
for sc_point_gid, row in tqdm(lookup.iterrows(), total=len(lookup)):
    x, y = row.x, row.y
    ## Calculate squared distance to each PV SC site
    sqdistances = (pvsc.x - x)**2 + (pvsc.y - y)**2
    ## Save the index
    out.append(np.argmin(sqdistances))
nearest = {
    **dict(zip(lookup.index, pvsc.iloc[out].index)),
    **dict(zip(pvsc.index, pvsc.index))
}
### Overwrite with nearest sc_point_gid
cspsc.sc_point_gid = cspsc.sc_point_gid.map(nearest)

#%% Get the costs and distances
pvdatacols = [
    'cost_reinforcement_usd_per_mw',
    'dist_reinforcement_km',
    'cost_total_trans_usd_per_mw',
    'dist_spur_km',
]
for col in pvdatacols:
    cspsc[col] = cspsc.sc_point_gid.map(nearest).map(pvsc[col])

# cspsc['sc_point_gid'] = revsites.loc[out, 'sc_point_gid'].values
# print(len([i for i in cspsc.sc_point_gid if i not in pvsc.index]), len(cspsc))
# )[['cost_reinforcement_usd_per_mw','reinforcement_dist_km','cost_total_trans_usd_per_mw','dist_km']]
# #%% Map to csp
# for col in pvsc:
#     cspsc[col] = cspsc.sc_point_gid.map(pvsc[col])

#%% Take a look at pvsc and cspsc
cmap = plt.cm.gist_earth_r
for col in pvdatacols:
    vmin = 0
    vmax = min(cspsc[col].max(), 1e6)
    plt.close()
    f,ax = plt.subplots(figsize=(12,9))
    dfstates.plot(ax=ax, facecolor='none', edgecolor='k', lw=0.5, zorder=1e6)
    cspsc.plot(
        ax=ax, column=col, lw=0, marker='s', markersize=3,
        cmap=cmap, vmin=vmin, vmax=vmax)

    plots.addcolorbarhist(
        f=f, ax0=ax, data=cspsc[col].values,
        title=col, cmap=cmap,
        vmin=vmin, vmax=vmax,
        orientation='horizontal', labelpad=2.1, cbarbottom=-0.06,
        cbarheight=0.3, log=False,
        nbins=21, histratio=2,
        ticklabel_fontsize=20, title_fontsize=24,
        extend='neither',
    )

    ax.axis('off')
    plt.show()



#%% Aggregate to sc_point_gid
## Get MW by region/class
mw_sc_point_gid = cspsc.groupby(['sc_point_gid']).capacity_mw.sum()

## Run it
cspsc_agg = {}
for col in pvdatacols+['cf']:
    cspsc['val'] = cspsc[col] * cspsc['capacity_mw']
    cspsc_agg[col] = cspsc.groupby(['sc_point_gid']).val.sum() / mw_sc_point_gid

cspsc_agg = pd.concat(cspsc_agg, axis=1)
cspsc_agg['capacity'] = mw_sc_point_gid
## Assign class
cspsc_agg['class'] = 0
for k,v in classes.items():
    cspsc_agg.loc[cspsc_agg.cf<v, 'class'] = k
## Get total supply curve cost
cspsc_agg['supply_curve_cost_per_mw'] = cspsc_agg[
    ['cost_reinforcement_usd_per_mw', 'cost_total_trans_usd_per_mw']
].sum(axis=1)
## Get region
# scpointgid2rb = cspsc[['sc_point_gid','rb']].drop_duplicates().set_index('sc_point_gid').rb
# scpointgid2rb = pvsc[['sc_point_gid','rb']].drop_duplicates().set_index('sc_point_gid').rb
sitemap = reeds.io.get_sitemap()
county2zone = reeds.io.get_county2zone()
site2r = sitemap.FIPS.map(county2zone)
cspsc_agg['r'] = cspsc_agg.index.map(site2r)
cspsc_agg['class'] = cspsc_agg['class'].astype(int)

#%% Check for errors
if len(cspsc_agg.loc[cspsc_agg.r.isnull()]):
    print(cspsc_agg.loc[cspsc_agg.r.isnull()])
    raise ValueError('Missing regions in cspsc_agg')

# cspsc_agg
# cspsc_agg.groupby('class').capacity.sum()/1e3


# ## Run it
# cspsc_agg = {}
# for col in pvdatacols:
#     cspsc['val'] = cspsc[col] * cspsc['capacity_mw']
#     cspsc_agg[col] = cspsc.groupby(['rb','class']).val.sum() / mw_rclass

# cspsc_agg = pd.concat(cspsc_agg, axis=1)
# cspsc_agg['MW'] = mw_rclass

#%% Get MW by region/class
## Drop profiles with capacity below 100 MW
cutoff = 100
mw_rclass = (
    cspsc.rename(columns={'rb':'r','newclass':'class'})
    .groupby(['r','class']).capacity_mw.sum()
)

#%% Calculate the capacity-weighted-average CF profile by (region,class)
scpointgid2rclass = pd.Series(
    [tuple(x) for x in cspsc_agg[['r','class']].values],
    index=cspsc_agg.index,
)
cspgen = cspcf * mw_sc_point_gid
cspgen.columns = cspgen.columns.map(nearest).map(scpointgid2rclass).rename(['r','class'])
#%% Aggregate to (r,class) and divide by capacity to get weighted average
cspgen_agg = (cspgen.groupby(['r','class'], axis=1).sum() / mw_rclass).dropna(axis=1)

#%% Roll forward by one hour to match hourlize treatment
cspgen_rolled = pd.DataFrame(
    np.roll(cspgen_agg, shift=-1, axis=0),
    columns=cspgen_agg.columns, index=cspgen_agg.index,
)
### Rename columns to match ReEDS default
cspgen_rolled.columns = cspgen_rolled.columns.map(lambda x: f'{x[1]}_{x[0]}')
### Roll again to get to local time...
rb2tz = pd.read_csv(
    os.path.join(reedspath,'inputs','variability','reeds_ba_tz_map.csv'),
    index_col='r')
rb2tz['hourshift'] = rb2tz.tz.map({'PT':-3, 'MT':-2, 'CT':-1, 'ET':0})
r2shift = rb2tz.hourshift.copy()
cspgen_local = pd.DataFrame(
    {col: np.roll(cspgen_rolled[col], r2shift[col.split('_')[-1]])
          for col in cspgen_rolled},
    index=range(1,7*8760+1)
)
cspgen_local.index = cspgen_local.index.rename('hour')

#%% test
# dftest = LDC_prep.local_to_eastern(cspgen_local, reedspath)
# display(cspgen_rolled.head(12))
# display(dftest.head(12))
# display(cspgen_local.head(12))

#%% Write the outputs
## Supply curve
(
    cspsc_agg.reset_index()
    .rename(columns={'r':'region'})
    [['region','class','sc_point_gid','capacity',
      'supply_curve_cost_per_mw','dist_km','reinforcement_dist_km']]
    .round(3)
).to_csv(
    os.path.join(reedspath,'inputs','supplycurvedata','csp_supply_curve-reference.csv'),
    index=False,
)

cspgen_local.astype(np.float32).to_hdf(
    os.path.join(reedspath,'inputs','variability','multi_year','csp-reference.h5'),
    key='data', complevel=4, format='table')

#%% Check against the old ReEDS CSP CF
# dfold = pd.read_hdf(
#     os.path.join(reedspath,'inputs','variability','multi_year','csp.h5')
# )
# dfold.index = hourly_writetimeseries.get_timeindex()
dfnew = pd.read_hdf(
    os.path.join(reedspath,'inputs','variability','multi_year','csp-reference.h5')
)
dfnew.index = hourly_writetimeseries.get_timeindex()
dfpv = reeds.io.read_file(
    os.path.join(reedspath,'inputs','variability','multi_year','upv-reference')
)
dfpv.index = hourly_writetimeseries.get_timeindex()

### Take a look
months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
profiles = ['p1','p11','p48','p98','p101']
year = 2007
plt.close()
f,ax = plt.subplots(12,len(profiles),figsize=(12,12),sharex=True,sharey=True)
for row, month in enumerate(months):
    for col, r in enumerate(profiles):
        ax[0,col].set_title(r)
        ### PV
        # df = (
        #     dfpv[[c for c in dfpv if c.endswith(r)]].iloc[:,-1]
        #     .loc[f'{month} {year}']
        # ).copy()
        # ax[row,col].plot(df.groupby([df.index.hour]).mean(), label='PV', color='C0')
        ### Old CSP
        # df = (
        #     dfold[[c for c in dfold if rsmap[c.split('_')[1]].endswith(r)]].iloc[:,-1]
        #     .loc[f'{month} {year}']
        # ).copy()
        # ax[row,col].plot(
        #     df.groupby([df.index.hour]).mean(),
        #     label='Old CSP', color='C0', marker='o', markersize=2)
        ### New CSP
        df = (
            dfnew[[c for c in dfnew if c.endswith(r)]].iloc[:,-1]
            .loc[f'{month} {year}']
        ).copy()
        ax[row,col].plot(
            df.groupby([df.index.hour]).mean(),
            label='New CSP', color='C1', marker='o', markersize=2)
ax[0,0].legend()
ax[0,0].set_xlim(0,24)
plots.despine(ax)
plt.show()
