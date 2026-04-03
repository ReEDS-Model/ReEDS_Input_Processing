#%% Imports
import pandas as pd
from pathlib import Path
rip_path = Path(__file__).parent.parent

#%% Inputs
countypath = Path(rip_path, 'zones', 'z3109_20260223')
z54path = Path(rip_path, 'zones', 'z54_20220624')
z132path = Path(rip_path, 'zones', 'z132_20250313')
z134path = Path(rip_path, 'zones', 'z134_20030521')

zonedirs = [
    Path(rip_path, 'zones', 'z48_state'),
    Path(rip_path, 'zones', 'z153_20260223'),
    Path(rip_path, 'zones', 'z1259_20260223'),
    Path(rip_path, 'zones', 'z2972_20260303'),
    Path(rip_path, 'zones', 'z2975_20260223'),
]

###### Procedure
#%% Get helper hierarchy files
hierarchy_54 = pd.read_csv(Path(z54path, 'hierarchy.csv'), index_col='r')
hierarchy_132 = pd.read_csv(Path(z132path, 'hierarchy.csv'), index_col='r')
hierarchy_134 = pd.read_csv(Path(z134path, 'hierarchy.csv'), index_col='ba')
hierarchy_county = pd.read_csv(Path(countypath, 'hierarchy.csv'), index_col='r')
## Normalize formatting
hierarchy_county.index = hierarchy_county.index.map(lambda x: 'FIPS_' + x.strip('p'))

#%% Run for each zondir
for zonedir in zonedirs:
    hpath = Path(zonedir, 'hierarchy.csv')
    hierarchy_in = pd.read_csv(hpath)

    ### Assign missing hierarchy levels to ones we know
    levels = ['transgrp', 'transreg', 'nercr', 'hurdlereg']
    hierarchy = hierarchy_in.copy()
    for level in levels:
        ### County
        ## Simplification: Always just assign to the first county in a multi-county list
        hierarchy[level] = hierarchy.r.str[:10].map(hierarchy_county[level])
        ## z132
        hierarchy.loc[hierarchy[level].isnull(), level] = (
            hierarchy.loc[hierarchy[level].isnull(), 'r']
            .map(hierarchy_132[level])
        )
        ## z134
        hierarchy.loc[hierarchy[level].isnull(), level] = (
            hierarchy.loc[hierarchy[level].isnull(), 'r']
            .map(hierarchy_134[level])
        )
        ## z54
        hierarchy.loc[hierarchy[level].isnull(), level] = (
            hierarchy.loc[hierarchy[level].isnull(), 'r']
            .map(hierarchy_54[level])
        )
    ### Make sure it worked
    if hierarchy.loc[hierarchy.isnull().sum(axis=1) > 0].shape[0] != 0:
        print(hierarchy.loc[hierarchy.isnull().sum(axis=1) > 0])
        raise KeyError('Some hierarchy levels not mapped')

    ### Write it
    hpath.rename(Path(hpath.parent, f'{hpath.stem}-old.csv'))
    hierarchy.to_csv(hpath, index=False)
