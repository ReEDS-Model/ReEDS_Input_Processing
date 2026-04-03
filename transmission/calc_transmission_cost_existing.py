#%% Imports
import os
import sys
import numpy as np
import pandas as pd
import geopandas as gpd

sys.path.append(os.path.expanduser('~/github/ReEDS-2.0'))
import reeds

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

#%% Functions
def assign_line_to_region(dflines, dfregions, label='region'):
    """
    Copied from
    https://github.nrel.gov/cobika/DLR/blob/341f451193c99124d7a9fac4344e0ceddd900fc5/dlr/helpers.py#L458
    """
    ## If a line overlaps with at least one region, assign it to the most overlapping region
    regions = dfregions.index
    _overlaps = {}
    for region in regions:
        _overlaps[region] = dflines.intersection(dfregions.loc[region,'geometry']).length
    overlaps = pd.concat(_overlaps, axis=1, names=label)
    main_region = (
        overlaps.stack().rename('overlap')
        .sort_values().reset_index().drop_duplicates('ID', keep='last')
        .set_index('ID')
    )
    main_region.loc[main_region.overlap == 0, label] = '_none'
    _dflines = dflines.merge(main_region[[label]], left_index=True, right_index=True)
    ## Also record lines that cross between regions
    _dflines[f'multi_{label}'] = overlaps.replace(0, np.nan).apply(
        lambda row: ','.join(row.dropna().index.tolist()),
        axis=1,
    )
    ## For unmapped lines, map them to the closest region
    ids_unmapped = _dflines.loc[_dflines[label] == '_none'].index
    for ID in ids_unmapped:
        _dflines.loc[ID,label] = (
            dfregions.distance(_dflines.loc[ID, 'geometry']).nsmallest(1).index[0])

    return _dflines


#%% Get the rebuild cost
trans_rebuildcost = gpd.read_file(
    os.path.join(repo_path, 'transmission', 'trans_rebuildcost-USD2024.gpkg')
).set_index('ID')

#%% Assign each line to the state it overlaps the most
dfmap = reeds.io.get_dfmap()

trans_rebuildcost = assign_line_to_region(trans_rebuildcost, dfmap['st'], label='st')

#%% Convert $/mile back to $, then sum by state
trans_rebuildcost['USD'] = (
    trans_rebuildcost['MUSDpermile'] * 1e6
    * trans_rebuildcost.length / 1609.34
)
transcost_state = trans_rebuildcost.groupby('st').USD.sum()

#%% Write it
transcost_state.rename('USD2024').rename_axis('state').round(0).astype(int).to_csv(
    os.path.join(
        reeds.io.reeds_path, 'postprocessing',
        'retail_rate_module', 'calc_historical_capex',
        'existing_transmission_cost_bystate_USD2024.csv',
    )
)
