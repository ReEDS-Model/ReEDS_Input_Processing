"""
- Must be run on the NREL HPC
- Add rex to your environment first using `pip install nrel-rex==0.2.85`
"""

#%% Imports
import os
import site
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import h5py
from rex import NSRDBX
from tqdm import tqdm

reeds_path = os.path.expanduser('~/github/ReEDS-2.0')
reeds_path = '/projects/reedsweto/pbrown/github/ReEDS-2.0'

site.addsitedir(os.path.join(reeds_path))
import reeds
from reeds import plots

pd.options.display.max_columns = 200
plots.plotparams()

#%% User inputs
country = 'USA'
level = 'st'

#%% Setup
years = range(1998, 2025)
# years = range(2006,2015)
nsrdb_fpath_base = '/kfs2/datasets/NSRDB/current'

### Dummy year for meta file
year = 2010
nsrdb_fpath = os.path.join(nsrdb_fpath_base, f'nsrdb_{year}.h5')

### Get lat/lon of NSRDB sites
with h5py.File(nsrdb_fpath, 'r') as f:
    nsrdb_cols = list(f)
    meta = pd.DataFrame(f['meta'][...])

for col in ['country','state','county']:
    meta[col] = meta[col].map(lambda x: x.decode())
if country.lower() in ['usa','us','united states', 'united states of america']:
    meta = meta.loc[meta.country == 'United States'].copy()
dfmeta = plots.df2gdf(meta)

#%%### Unweighted average temperature for all regions and years
### Get list of regions
hierarchy = reeds.io.get_hierarchy().reset_index()
dfmap = reeds.io.get_dfmap()

if level not in hierarchy:
    raise ValueError(f"Provided level={level} but must be in {hierarchy.columns.tolist()}")

regions = sorted(hierarchy[level].unique())

# abbrevs = sorted(set(hierarchy.st.unique().tolist() + ['DC']))

# abbrev2state = pd.read_csv(
#     os.path.join(reeds_path, 'hourlize', 'inputs', 'resource', 'state_abbrev.csv'),
#     index_col='ST',
# ).squeeze()

#%% Get sites for desired region resolution
print('Getting site list for desired region resolution')
gids = {}
for r in tqdm(regions):
    geom = dfmap[level].loc[r,'geometry']
    gids[r] = dfmeta.loc[dfmeta.intersects(geom)].index.values

#%% Run it
dictout = {}
for year in years:
    dictyear = {}
    nsrdb_fpath = os.path.join(nsrdb_fpath_base, f'nsrdb_{year}.h5')
    for r in tqdm(regions, desc=str(year)):
    # for r in tqdm(['p132'], desc=str(year)):
        with NSRDBX(nsrdb_fpath, hsds=False) as f:
            dfregion = f.get_gid_df('air_temperature', gids[r])
            # dfstate = f.get_region_df(
            #     'air_temperature', region=abbrev2state[abbrev], region_col='state',
            # )
        ## Only keep the hourly values, not half-hourly
        dictyear[r] = dfregion.iloc[::2].mean(axis=1).astype(np.float32)

    dictout[year] = pd.concat(dictyear, axis=1, names=[level])

#%% Write it
outfile = os.path.join(
    reeds_path, 'inputs', 'variability', 'multi_year', f'temperature_celsius-{level}.h5')
os.makedirs(os.path.dirname(outfile), exist_ok=True)
if os.path.exists(outfile):
    os.remove(outfile)
with h5py.File(outfile, 'w') as f:
    f.create_dataset('columns', data=dictout[years[0]].columns, dtype='S29')
    for year in dictout:
        f.create_dataset(
            str(year), data=dictout[year],
            dtype=np.float32,
            compression='gzip', compression_opts=4,
        )
        f.create_dataset(f'index_{year}', data=dictout[year].index, dtype='S29')

#%% Test it
year = years[-1]
with h5py.File(outfile, 'r') as f:
    df = pd.DataFrame(
        data=f[str(year)],
        columns=pd.Series(f['columns']).map(lambda x: x.decode()),
        index=pd.to_datetime(pd.Series(f[f'index_{year}']).map(lambda x: x.decode())),
    )

print(df.shape)

cmap = plt.cm.turbo
dfplot = dfmap[level].copy()
dfplot['temp_mean'] = df.mean()
dfplot['temp_max'] = df.max()
dfplot['temp_min'] = df.min()
vmin = np.floor(dfplot.temp_min.min())
vmax = np.ceil(dfplot.temp_max.max())
for col in ['temp_mean', 'temp_max', 'temp_min']:
    plt.close()
    f,ax = plt.subplots()
    dfplot.plot(
        ax=ax, column=col, cmap=cmap, edgecolor='none',
        vmin=vmin, vmax=vmax,
    )
    plots.addcolorbarhist(
        f=f, ax0=ax, data=dfplot[col].values, cmap=cmap,
        vmin=vmin, vmax=vmax,
        title=f'{year}\n{col}\n[°C]', nbins=51,
    )
    ax.axis('off')
    plt.savefig(os.path.join(reeds_path, 'runs', f'{col}-{year}.png'))

plt.close()
f,ax = plots.plotyearbymonth(df, style='line')
plt.savefig(os.path.join(reeds_path, 'runs', f'temperature-{year}.png'))
