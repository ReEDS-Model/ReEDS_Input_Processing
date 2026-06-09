#%% Imports
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import os
import sys
import cmocean


#%% Functions
def smear(dfzones, dfgroups, decay_km=50, decay_func=np.exp):
    weights = {}
    distances_km_all = {}

    for r, row in dfzones.iterrows():
        ## Get distance from centroid to edge of all other zones
        ## To get edge-of-polygon-to-edge-of-polygon distance, remove .centroid below
        ## To get centroid-to-centroid distance, add .centroid after dfgroups
        distances_km = dfgroups.distance(row.geometry.centroid) / 1000
        rb_key = row['rb']
        distances_km_all[rb_key] = distances_km
        ## Weight decays with distance from centroid
        weight = decay_func(-distances_km / decay_km)  # this blurs with all the regions
        weights[rb_key] = weight

    weight_df = pd.DataFrame(weights)
    weight_norm = weight_df / weight_df.sum()
    weight_norm = weight_norm.T

    return weight_norm, distances_km_all


#%% Procedure
if __name__ == '__main__':
    #%% Imports
    reeds_path = os.path.expanduser('~/github/ReEDS')
    sys.path.append(reeds_path)
    import reeds

    #%% Inputs
    #decay_kms = [50, 100, 150, 200]
    decay_kms = [150]
    nrows = len(decay_kms)
    scale=7
    decay_func = np.exp
    decimals = 3

    # Set spatial resolution
    resolution = 'county'
    figpath = f'outputs/figures/{resolution}'


    #%% Load files
    # read county shapefile directly from census
    dfcounty = reeds.spatial.get_map('county', source='tiger')
    ## Format for ReEDS
    dfcounty['FIPS'] = dfcounty.index.values
    dfcounty['rb'] = 'p' + dfcounty['FIPS']
    state_fips = pd.read_csv(
        os.path.join(reeds_path, 'inputs', 'shapefiles', 'state_fips_codes.csv'),
        dtype={'state_fips': str},
        index_col='state_fips',
    ).rename(columns={'state':'STATE', 'state_code':'STCODE'})[['STATE', 'STCODE']]
    dfcounty = dfcounty.merge(state_fips, left_on='STATEFP', right_index=True, how='left')
    ## Add cendiv
    fpath = Path(reeds.io.reeds_path, 'inputs', 'zones', 'state_groups.csv')
    st2cendiv = pd.read_csv(fpath, index_col='st').cendiv
    ## Map Washington, D.C. to appropriate census division
    st2cendiv['DC'] = 'South_Atlantic'
    dfcounty['cendiv'] = dfcounty.STCODE.map(st2cendiv)
    if dfcounty.cendiv.isnull().sum() > 0:
        raise ValueError('Unmapped counties')

    dfcendiv = dfcounty.dissolve('cendiv').buffer(0.)


    #%% Save the results
    dfweights = {}
    for decay_km in decay_kms:
        dfweights[decay_km], _ = smear(dfcounty, dfcendiv, decay_km=decay_km, decay_func=decay_func)
        if not (dfweights[decay_km].sum(axis=1).map(lambda x: np.isclose(x, 1))).all():
            raise ValueError("Census division weights don't sum to 1")
        fpath = f'{resolution}_weights_{decay_km}kmExpDecay.csv'
        dfweights[decay_km].rename_axis('r').sort_index().round(decimals).to_csv(fpath)


    #%% Take a look
    cmap = cmocean.cm.rain
    cendivs = dfcendiv.bounds.minx.sort_values().index
    nrows, ncols, coords = reeds.plots.get_coordinates(cendivs, aspect=1)
    for decay_km in decay_kms:
        plt.close()
        f,ax = plt.subplots(
            nrows, ncols, sharex=True, sharey=True, figsize=(3*ncols, 2.5*nrows),
            gridspec_kw={'hspace':0, 'wspace':0},
        )
        for cendiv in cendivs:
            _ax = ax[coords[cendiv]]
            _ax.axis('off')
            dfcendiv.plot(ax=_ax, facecolor='none', edgecolor='k', lw=0.5, zorder=1e6)
            dfplot = dfcounty.copy()
            dfplot['value'] = dfplot.rb.map(dfweights[decay_km][cendiv])
            dfplot = dfplot[['value','geometry']].round(decimals).replace(0,np.nan).dropna()
            dfplot.plot(ax=_ax, column='value', vmin=0, vmax=1, cmap=cmap)
            _ax.set_title(cendiv, fontsize=12, weight='bold', y=0.92)
    reeds.plots.addcolorbarhist(
        f, ax[-1, 1], dfplot.value,
        cmap=cmap, vmin=0, vmax=1,
        orientation='horizontal', cbarbottom=-0.1, cbarheight=2, cbarwidth=0.1,
        histratio=0.1, histcolor='w', title='Weight [fraction]',
        labelpad=1.3, title_fontsize=12, ticklabel_fontsize=12,
    )
    plt.show()
