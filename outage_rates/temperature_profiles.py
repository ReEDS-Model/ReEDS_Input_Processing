"""
- Must be run on the NLR HPC
- Add rex to your environment first using `pip install nrel-rex==0.2.85`
"""

#%% Imports
import os
import sys
import numpy as np
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import h5py
from rex import NSRDBX
from tqdm import tqdm

reeds_path = Path('../../ReEDS').resolve()

sys.path.append(str(reeds_path))
import reeds

pd.options.display.max_columns = 200
reeds.plots.plotparams()

#%% User inputs
country = 'USA'
crs = 'EPSG:5070'
outpath = Path.cwd()

#%% Setup
# years = range(1998, 2026)
years = range(2006, 2026)
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
dfmeta = reeds.plots.df2gdf(meta, crs=crs)

#%%### Unweighted average temperature for all regions and years
### Get list of ReEDS sites
sitemap = reeds.io.get_sitemap(crs=crs)

#%% Get closest NSRDB site to each ReEDS site
nsrdb_gids = dfmeta.assign(nsrdb_gid=dfmeta.index)[['nsrdb_gid','geometry']].copy()
sitemap = sitemap.sjoin_nearest(nsrdb_gids, how='left')

#%% Run it
dictout = {}
for year in tqdm(years):
    dictyear = {}
    nsrdb_fpath = os.path.join(nsrdb_fpath_base, f'nsrdb_{year}.h5')
    with NSRDBX(nsrdb_fpath, hsds=False) as f:
        dfdata = f.get_gid_df('air_temperature', sitemap.nsrdb_gid.values)
        dfdata.columns = sitemap.index
    ## Only keep the hourly values, not half-hourly
    dictout[year] = dfdata.iloc[::2].round(0).astype(np.int8)

#%% Write it
outfile = Path(outpath, 'temperature_celsius.h5').resolve()
os.makedirs(outfile.parent, exist_ok=True)
if os.path.exists(outfile):
    os.remove(outfile)
with h5py.File(outfile, 'w') as f:
    f.create_dataset('columns', data=dictout[years[0]].columns, dtype=np.int32)
    for year in dictout:
        f.create_dataset(
            str(year), data=dictout[year], dtype=np.int8,
            compression='gzip', compression_opts=4,
        )
        f.create_dataset(f'index_{year}', data=dictout[year].index, dtype='S29')

#%% Test it
year = years[-1]
with h5py.File(outfile, 'r') as f:
    df = pd.DataFrame(
        data=f[str(year)],
        columns=pd.Series(f['columns']),
        index=pd.to_datetime(pd.Series(f[f'index_{year}']).map(lambda x: x.decode())),
    )
print(df.shape)

#%% Take a look
cmap = plt.cm.turbo
dfplot = sitemap.copy()
dfplot.geometry = dfplot.buffer(11530/2, cap_style='square')
dfplot['temp_mean'] = df.mean()
dfplot['temp_max'] = df.max()
dfplot['temp_min'] = df.min()
timestamps = df.sample(5).index
tcols = []
for timestamp in timestamps:
    tcol = timestamp.strftime('%Y%m%dT%H00%Z')
    dfplot[tcol] = df.loc[timestamp]
    tcols.append(tcol)
vmin = np.floor(dfplot.temp_min.min())
vmax = np.ceil(dfplot.temp_max.max())
for col in ['temp_mean', 'temp_max', 'temp_min'] + tcols:
    plt.close()
    f,ax = plt.subplots()
    dfplot.plot(
        ax=ax, column=col, cmap=cmap, edgecolor='none',
        vmin=vmin, vmax=vmax,
    )
    reeds.plots.addcolorbarhist(
        f=f, ax0=ax, data=dfplot[col].values, cmap=cmap,
        vmin=vmin, vmax=vmax,
        title=f'{year}\n{col}\n[°C]', nbins=51,
    )
    ax.axis('off')
    plt.savefig(os.path.join(outpath, f'{col}-{year}.png'))

dfsites = df.sample(10, axis=1)
plt.close()
f,ax = reeds.plots.plotyearbymonth(dfsites, style='line')
plt.savefig(os.path.join(outpath, f'temperature-{year}.png'))
