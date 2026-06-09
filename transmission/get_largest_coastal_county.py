#%% Imports
import cmocean
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import reeds

#%% Inputs
fpop = Path(reeds.io.reeds_path, 'inputs', 'disaggregation', 'county_population.csv')
GSw_ZoneSet = 'z3109'

#%%### Procedure
#%% Get data
dfcounty = reeds.io.get_countymap()

dfpop = pd.read_csv(fpop)
dfpop.FIPS = dfpop.FIPS.str.strip('p')
dfpop = dfpop.set_index('FIPS').squeeze(1)

#%% Get offshore links and hierarchy
fpath_134 = Path(reeds.io.reeds_path, 'inputs', 'zones', 'z134', 'newlinks_offshore_radial.csv')
df134 = pd.read_csv(fpath_134)

#%% Get new zones
county2zone = reeds.io.get_county2zone(GSw_ZoneSet='z134')

#%% Make a USA_shaped hole
usa = dfcounty.dissolve()

usabuff = usa.copy()
usabuff.geometry = usa.buffer(500000)

usahole = usabuff.difference(usa)

#%% Get counties adjacent to the border
border_counties = dfcounty.loc[
    dfcounty.buffer(1).intersection(usahole.iloc[0]).area > 0
].copy()
border_counties.plot()

#%%
border_counties['population'] = border_counties.index.map(dfpop)

#%% For each zone, identify the highest-population border county
zones = df134.rr.values
largest = county2zone.loc[county2zone.isin(zones)].reset_index()
largest['population'] = largest.FIPS.map(dfpop)
largest = largest.loc[largest.FIPS.isin(border_counties.index)]

largest = (
    largest
    .sort_values('population', ascending=False).drop_duplicates('r')
    .set_index('r').FIPS
)

#%%
state = 'CT'
state = 'NY'
state = 'OR'
state = 'NC'
dfplot = dfcounty.loc[dfcounty.STCODE==state].copy()
dfplot['population'] = dfplot.index.map(dfpop)
plt.close()
f,ax = plt.subplots(figsize=(10,10))
dfplot.plot(ax=ax, column='population', edgecolor='k', cmap=cmocean.cm.rain)
for r, row in dfplot.iterrows():
    ax.annotate(
        r, (row.geometry.centroid.x, row.geometry.centroid.y),
        ha='center', va='center', color='C3',
    )
plt.show()
print(dfplot.sort_values('population'))

#%% Write new county map
dfout = df134.set_index('r').copy()
dfout['rr'] = 'p' + dfout.rr.map(largest)
dfout.loc['oCT', 'rr'] = 'p09190'
fpath_out = Path(reeds.io.reeds_path, 'inputs', 'zones', 'z3109', 'newlinks_offshore_radial.csv')
dfout.to_csv(fpath_out)
