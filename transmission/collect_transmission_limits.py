#%% Imports
import pandas as pd
import numpy as np
import os
import site
from tqdm import tqdm
from glob import glob
import geopandas as gpd

reeds_path = os.path.expanduser('~/github/ReEDS-2.0')
remotepath = '/Volumes/ReEDS/Users/pbrown/Transmission/TSC/runs/'
tscpath = os.path.expanduser('~/github/TSC')

site.addsitedir(tscpath)
import functions as tsc

## Need to use the tsc environment from
## https://github.nrel.gov/pbrown/TSC/blob/main/environment.yml
assert os.environ['CONDA_DEFAULT_ENV'] == 'tsc'

#%% Inputs
runspath = remotepath
networksource = 'NARIS'
trans_init_year = 2024
# ### Group limits
# level = 'transgrp'
# cases = [
#     '20230727_NARIS_transgrp_EI_R0_D0_700km',
#     '20230727_NARIS_transgrp_EI_R1_D0_700km',

#     '20230727_NARIS_transgrp_WI_R0_D0',
#     '20230727_NARIS_transgrp_WI_R1_D0',
# ]

### Zone limits
level = 'r'
cases = [
    '20230727_NARIS_r_EI_R0_D0_800km',
    '20230727_NARIS_r_EI_R1_D0_800km',

    '20230727_NARIS_r_TI_R0_D0',
    '20230727_NARIS_r_TI_R1_D0',

    '20230727_NARIS_r_WI_R0_D0',
    '20230727_NARIS_r_WI_R1_D0',
]
levelname = {'r':'ba', 'subferc':'transgrp'}
savename = f'transmission_capacity_init_AC_{levelname.get(level,level)}_{networksource}{trans_init_year}.csv'

#################
### Procedure ###
#%% Get interface limits
levell = level + level[-1]
dictin_itl = {}
for case in cases:
    ### Example name:
    ## 20230501_NARIS_subferc_WI_R0_D0
    ## {prefix}_{networksource}_level_interconnect_R{reverse}_D{loadinject}
    level = case.split('_')[2].replace('subferc','transgrp')
    interconnect = case.split('_')[3]
    reverse = int(case.split('_')[4].strip('R'))
    loadinject = int(case.split('_')[5].strip('D'))

    ### Get output folders
    folders = glob(os.path.join(runspath,case,'*'))
    for f in tqdm(folders, desc=case):
        ### Parse it
        interface = os.path.basename(f).replace('\uf07c','|')
        ### Load the transfer capacities
        outputs = glob(os.path.join(f,'outputs','transfer*'))
        for i in outputs:
            ### Parse it
            contingency = int(os.path.basename(i).split('_')[-1].split('.')[0].strip('n'))
            ### Load it
            dictin_itl[level,reverse,interconnect,interface,contingency] = pd.read_csv(
                i, header=0, index_col='i', squeeze=True
            ## Use reindex in case the dataframe is empty
            ).reindex([interface]).fillna(0).values[0]

#%% Get individual line flows
dictin_lineflow = {}
for case in cases:
    if 'NARIS' in case:
        interconnect = case.split('_')[3]
        reverse = int(case.split('_')[4].strip('R'))
        loadinject = int(case.split('_')[5].strip('D'))
    else:
        interconnect = case.split('_')[1]
        reverse = int(case.split('_')[2].strip('R'))
        loadinject = int(case.split('_')[3].strip('D'))
    ### Get output folders
    folders = sorted(glob(os.path.join(runspath,case,'*')))
    for f in tqdm(folders, desc=case):
        ### Parse it
        interface = os.path.basename(f).replace('\uf07c','|')
        ### Load the node and line maps
        try:
            linemap = pd.read_csv(
                os.path.join(f,'io','linemap.csv'),
                header=None, index_col=1, squeeze=True)
        except pd.errors.EmptyDataError:
            print(interface)
            continue
        ### Load the flows
        for subnet in [0,1]:
            for n in [0,1]:
                try:
                    df = pd.read_csv(
                        os.path.join(f,'outputs',f'flow_level_{subnet}_n{n}.csv')
                    ).rename(columns={'Val':'MW'})
                    line_interface = pd.read_csv(
                        os.path.join(f,'inputs',f'line_interface_{subnet}_n{n}.csv'),
                        header=None, index_col=0, squeeze=True).values
                    df['il'] = df.l.isin(line_interface)
                    df.l = df.l.map(linemap)
                    dictin_lineflow[loadinject,reverse,interconnect,interface,n] = (
                        df.loc[df.il].set_index('l').MW)
                except FileNotFoundError as err:
                    if subnet > 0:
                        continue
                    print(err)
                    dictin_lineflow[loadinject,reverse,interconnect,interface,n] = pd.DataFrame(
                        columns=['l','MW']).set_index('l').MW


### Concat into one dataframe
dflineflow = (
    pd.concat(dictin_lineflow)
    .rename_axis(['D','R','interconnect','interface','contingency','line'])
)


#%% Combine
dfin_itl = pd.DataFrame(dictin_itl, index=['MW']).abs().T
dfin_itl.index = dfin_itl.index.rename(['level','R','interconnect','interface','contingency'])
dfin_itl = dfin_itl.reset_index()
dfin_itl[level] = dfin_itl.interface.map(lambda x: x.split('||')[0])
dfin_itl[levell] = dfin_itl.interface.map(lambda x: x.split('||')[1])

directioncontingency2label = {(0,0):'MW_f0', (0,1):'MW_f1', (1,0):'MW_r0', (1,1):'MW_r1'}
dfin_itl['datum'] = dfin_itl.apply(
    lambda row: directioncontingency2label[row.R, row.contingency],
    axis=1
)

#%% Get lines table (needed to filter line flows)
networksource = 'NARIS'
ceiipath = '/Volumes/PLEXOS CEII/'
crs = 'ESRI:102008'
nan_rating = 8888
dc_categories = ['DC_lines', 'DC Tie_WI', 'ACDC', 'B2B']
typemap = {1:'load',2:'gen',3:'genload'}
dfba = gpd.read_file(os.path.join(reeds_path,'inputs','shapefiles','US_PCA')).set_index('rb')
hierarchy = (
    pd.read_csv(os.path.join(reeds_path,'inputs','hierarchy.csv'))
    .rename(columns={'*r':'r','ba':'r'})
    .drop(columns=['*county','county','county_name'], errors='ignore').drop_duplicates()
    .set_index('r')
)
hierarchy = hierarchy.loc[hierarchy.country.str.lower()=='usa'].copy()

_, _, dfnodes, dflines = tsc.get_input_buses_lines(
    zonecol='PCA',
    subset_col_1=None, subset_list_1=None,
    subset_col_2=None, subset_list_2=None,
    networksource=networksource, projpath=tscpath,
    crs=crs, dc_categories=dc_categories,
    nan_rating=nan_rating, typemap=typemap, dfba=dfba,
    hierarchy=hierarchy,
)

dflines = tsc.get_interfaces(dfnodes, dflines)

# #%% Testing
# dflines_wecc = dflines.loc[
#     dflines['PCA From'].isin(hierarchy.loc[hierarchy.interconnect=='western'].index)
#     & dflines['PCA To'].isin(hierarchy.loc[hierarchy.interconnect=='western'].index)
#     & (dflines['From kV'] == dflines['To kV'])
#     & (dflines['From kV'] >= 69)
# ].copy()

# dflines_wecc['From kV'].value_counts().nlargest(20)

#%% Get interface miles for reference
dfmiles = pd.read_csv(
    os.path.join(
        reeds_path,'inputs','transmission',
        'transmission_distance_cost_500kVac_ba.csv'),
)
imiles = pd.Series(
    index=dfmiles.r+'||'+dfmiles.rr, data=dfmiles.length_miles.values)

#%% Combine line flows
lineflows = dflineflow.reset_index()
lineflows['kVfrom'] = lineflows.line.map(dflines['From kV'])
lineflows['kVto'] = lineflows.line.map(dflines['To kV'])
lineflows['MWabs'] = lineflows.MW.abs()

### Get the list of lines to exclude
exclude = dflines.loc[
    (dflines.x <= 1e-4)
    | (dflines.r <= 0)
    | (dflines['From kV'] != dflines['To kV'])
    | (dflines.transformer == 1.)
]

### Get the zone area (to use in cutoff)
area_km2 = dfba.area / 1e6

#%% Filter based on voltage
### For each interface:
##  * If >= threshold_kV of the interface capacity comes from lines < cutoff_kV kV,
##    or if the zone is smaller than area_km2,
##    keep the flow on all lines for the total.
##  * Otherwise, keep only the flow on ≥ cutoff_kV kV lines for the total
### OR, if level is regional (in ['transgrp','transreg']), ALWAYS apply the cutoff_kV
cutoff_kV = 230
threshold_kV = 0.5
cutoff_radius = 50 * 1.609 # km
cutoff_area = np.pi * cutoff_radius**2 # km^2
### Can use this parameter to limit particular interfaces to a higher voltage level.
### We used to set it to {'p2||p5':231}, for example.
special_interface_cutoff = {}

dictout = {}
for n in [0,1]:
    for interface in lineflows.interface.unique():
        r, rr = interface.split('||')
        _cutoff = special_interface_cutoff.get(interface,cutoff_kV)
        for R in [0,1]:
            ### Determine which lines to include on the basis of n-0
            df = lineflows.loc[
                (lineflows.interface == interface)
                & ~lineflows.line.isin(exclude.index)
                & (lineflows.D == 0)
                & (lineflows.R == R)
                & (lineflows.contingency == 0)
            ]

            MW_all = abs(df.MW.sum())
            MW_below_cutoff = abs(df.loc[df.kVfrom < _cutoff, 'MW'].sum())
            MW_at_or_above_cutoff = MW_all - MW_below_cutoff

            if level in ['transgrp','transreg']:
                apply_cutoff = True
            elif MW_below_cutoff >= threshold_kV * MW_all:
                apply_cutoff = False
            else:
                apply_cutoff = True

            ### Don't apply cutoff if either of the zones is too small
            if (area_km2[r] <= cutoff_area) or (area_km2[rr] <= cutoff_area):
                apply_cutoff = False

            ### Now include the appropriate lines
            if (n == 0) and apply_cutoff:
                dictout[R, n, interface] = MW_at_or_above_cutoff
            elif (n == 0) and not apply_cutoff:
                dictout[R, n, interface] = MW_all
            elif n > 0:
                df = lineflows.loc[
                    (lineflows.interface == interface)
                    & ~lineflows.line.isin(exclude.index)
                    & (lineflows.D == 0)
                    & (lineflows.R == R)
                    & (lineflows.contingency == n)
                ]

                MW_all = abs(df.MW.sum())
                MW_below_cutoff = abs(df.loc[df.kVfrom < _cutoff, 'MW'].sum())
                MW_at_or_above_cutoff = MW_all - MW_below_cutoff

                if apply_cutoff:
                    dictout[R, n, interface] = MW_at_or_above_cutoff
                else:
                    dictout[R, n, interface] = MW_all

dfout = pd.Series(dictout)
dfout.index = dfout.index.rename(['R','n','interface'])
for n in [0,1]:
    twmiles = dfout.unstack('R').loc[n].multiply(imiles, axis=0).abs()
    print('n={}, {:>3} kV, {:.0f}% threshold_kV: {:.1f} TW-miles'.format(
        n, cutoff_kV, threshold_kV*100,
        twmiles.mean(axis=1).sum() / 1e6
    ))

#%% Sort interfaces
def sort_lines(df, level='r', levell='rr'):
    dfout = df.copy()
    try:
        dfout[level+'num'] = dfout[level].str.strip('p').astype(int)
        dfout[levell+'num'] = dfout[levell].str.strip('p').astype(int)
    except ValueError:
        dfout[level+'num'] = dfout[level].copy()
        dfout[levell+'num'] = dfout[levell].copy()
    dfout = (
        dfout.sort_values([level+'num',levell+'num'])
        .drop([level+'num',levell+'num'], axis=1))
    return dfout


#%% Reformat for ReEDS
dfwrite = dfout.unstack(['n','R'])
dfwrite.columns = ['MW_f0','MW_r0','MW_f1','MW_r1']
dfwrite[level] = dfwrite.index.map(lambda x: x.split('||')[0])
dfwrite[levell] = dfwrite.index.map(lambda x: x.split('||')[1])
dfwrite = (
    sort_lines(dfwrite, level, levell)
    [[level,levell,'MW_f0','MW_r0','MW_f1','MW_r1']]
    .round(3)
)

#%% Write it
dfwrite.to_csv(os.path.join(reeds_path,'inputs','transmission', savename))


#%%### Plots (optional)
### Imports
import matplotlib.pyplot as plt
import matplotlib as mpl
import geopandas as gpd
import shapely

site.addsitedir(os.path.join(reeds_path,'postprocessing'))
import plots
import reedsplots
plots.plotparams()

#%% Get background
dfba = gpd.read_file(
    os.path.join(reeds_path,'inputs','shapefiles','US_PCA')).set_index('rb')
dfstates = dfba.dissolve('st')
dfba['labelx'] = dfba.centroid.x
dfba['labely'] = dfba.centroid.y

hierarchy = pd.read_csv(
    os.path.join(reeds_path,'inputs','hierarchy.csv'),
    index_col='*r',
).drop(columns=['*county','county','county_name'], errors='ignore').drop_duplicates()
hierarchy = hierarchy.loc[hierarchy.country=='USA'].copy()
dfba['transgrp'] = dfba.index.map(hierarchy.transgrp)

dftransgrp = dfba.dissolve('transgrp')
dftransgrp['labelx'] = dftransgrp.centroid.x
dftransgrp['labely'] = dftransgrp.centroid.y

cmap = {
    'CAISO':plt.cm.tab20c(6),
    'NorthernGrid_West':plt.cm.tab20c(1),
    'NorthernGrid_East':plt.cm.tab20c(2),
    'NorthernGrid_South':plt.cm.tab20c(3),
    'WestConnect_North':plt.cm.tab20c(9),
    'WestConnect_South':plt.cm.tab20c(10),
    'SPP_North':plt.cm.tab20c(5),
    'SPP_South':plt.cm.tab20c(6),
    'MISO_North':plt.cm.tab20c(9),
    'MISO_Central':plt.cm.tab20c(10),
    'MISO_South':plt.cm.tab20c(11),
    'ERCOT':plt.cm.tab20c(2),
    'PJM_West':plt.cm.tab20c(5),
    'PJM_East':plt.cm.tab20c(6),
    'SERTP':plt.cm.tab20c(2),
    'FRCC':plt.cm.tab20c(10),
    'NYISO':plt.cm.tab20c(2),
    'ISONE':plt.cm.tab20c(10),
}
### Tweak the location of some transgrp centers
offset = dict(zip(cmap.keys(), [(0,0)]*len(cmap)))
offset['MISO_Central'] = (-1e5,-2e5)
offset['SPP_North'] = (1e5,0)
offset['CAISO'] = (-0.1e5,-0.5e5)
offset['NorthernGrid_East'] = (0,-1e5)
offset['WestConnect_North'] = (0,-1e5)

for i, row in dftransgrp.iterrows():
    dftransgrp.loc[i,'labelx'] += offset[i][0]
    dftransgrp.loc[i,'labely'] += offset[i][1]


#%%### Plot it
endpoints = 'label' # or ''
scaledown = 0.8
wscale = 1e-3 * scaledown
arrowscale = 8.5 * scaledown
alpha = 0.8
cm = plt.cm.turbo
norm = mpl.colors.Normalize(vmin=0, vmax=1)
colors = {'r':'0.5', 'transgrp':'k'}
level2n = {'r':0, 'transgrp':1}
level2shape = {'r':dfba, 'transgrp':dftransgrp}
scale = 10

plt.close()
f,ax = plt.subplots(figsize=(10,8))
###### Background
dfba.plot(ax=ax, facecolor='none', edgecolor='C7', lw=0.1)
dfstates.plot(ax=ax, facecolor='none', edgecolor='C7', lw=0.2)
dftransgrp.plot(ax=ax, facecolor='none', edgecolor='k', lw=0.7)
for i, row in dftransgrp.iterrows():
    dftransgrp.loc[[i]].plot(
        ax=ax, facecolor=cmap[i], edgecolor='none', zorder=-1e6, alpha=0.5,
    )
for r, row in dfba.iterrows():
    ax.annotate(
        r.lstrip('p'), (row.labelx, row.labely),
        ha='center', va='center', fontsize=6, c='C7',
        bbox={'facecolor':'w', 'edgecolor':'none', 'alpha':1, 'pad':0.5},
    )

###### Starting transmission capacities
for level in ['r','transgrp']:
    levell = level + level[-1]
    dfplot = pd.read_csv(
        os.path.join(
            reeds_path,'inputs','transmission',
            f'transmission_capacity_init_AC_NARIS2024-{level}.csv'),
        index_col='interface',
    )
    dfzones = level2shape[level]
    ### Loop over interfaces
    for i, row in dfplot.iterrows():
        r = row[level]
        rr = row[levell]
        forward = row[f'MW_f{level2n[level]}']
        reverse = row[f'MW_r{level2n[level]}']
        ### Get endpoints (halfway)
        startx = (dfzones.loc[r,endpoints+'x'] + dfzones.loc[rr,endpoints+'x']) / 2
        starty = (dfzones.loc[r,endpoints+'y'] + dfzones.loc[rr,endpoints+'y']) / 2
        delxtor = -(dfzones.loc[rr,endpoints+'x'] - dfzones.loc[r,endpoints+'x']) / 2
        delytor = -(dfzones.loc[rr,endpoints+'y'] - dfzones.loc[r,endpoints+'y']) / 2
        delxtorr = (dfzones.loc[rr,endpoints+'x'] - dfzones.loc[r,endpoints+'x']) / 2
        delytorr = (dfzones.loc[rr,endpoints+'y'] - dfzones.loc[r,endpoints+'y']) / 2
        ### Forward
        arrow = mpl.patches.FancyArrow(
            startx, starty, delxtorr, delytorr,
            width=abs(forward)*arrowscale,
            length_includes_head=True,
            head_width=abs(forward)*arrowscale*1.,
            head_length=abs(forward)*arrowscale*0.5,
            lw=0, color=colors[level], alpha=0.8, zorder=1e6,
        )
        ax.add_patch(arrow)
        ### Reverse
        arrow = mpl.patches.FancyArrow(
            startx, starty, delxtor, delytor,
            width=abs(reverse)*arrowscale,
            length_includes_head=True,
            head_width=abs(reverse)*arrowscale*1.,
            head_length=abs(reverse)*arrowscale*0.5,
            lw=0, color=colors[level], alpha=0.8, zorder=1e6,
        )
        ax.add_patch(arrow)

### Scale
ax.axis('off')

ax.plot(
    [-2.1e6,-1.7e6], [-1.1e6, -1.1e6],
    color=colors['r'], lw=wscale*scale*1e3, solid_capstyle='butt',
)
ax.annotate(
    f"r (n – {level2n['r']})", (-1.6e6, -1.1e6),
    ha='left', va='center', weight='normal', fontsize='large')

ax.plot(
    [-2.1e6,-1.7e6], [-1.2e6, -1.2e6],
    color=colors['transgrp'], lw=wscale*scale*1e3, solid_capstyle='butt',
)
ax.annotate(
    f"transgrp (n – {level2n['transgrp']})", (-1.6e6, -1.2e6),
    ha='left', va='center', weight='normal', fontsize='large')

ax.annotate(
    f'{scale} GW', (-1.9e6, -1.3e6),
    ha='center', va='top', weight='bold', fontsize='large')

plt.savefig(os.path.expanduser('~/Desktop/transmission_capacity_initial-r,transgrp.png'))
plt.show()
