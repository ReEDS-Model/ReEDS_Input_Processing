# -*- coding: utf-8 -*-
"""
Created on Wed Feb 15 11:03:37 2023

@author: wcole

This file writes out capital cost differences for each county.  It can easily be adjusted to
write out differences for BA instead if that is preferred.
"""

import pandas as pd

### Read in relevant files

# The working directory should be set to the folder where this file lives

# Capital cost multipliers for each supply curve point and technology category
dfin = pd.read_csv('reg_cap_cost_mult.csv').round(3)
# Downselect to technology groups
rename = {
    'battery': 'BATTERY|PVB',
    'coal-new': 'COAL|BIO',
    'gas-cc': 'COMBINED_CYCLE',
    'gas-ct': 'COMBUSTION_TURBINE|CONSUME|LFILL|OGS',
    'nuclear': 'NUCLEAR',
    'pv': 'CSP',
}
dfdiff = dfin.rename(columns=rename)[['sc_gid']+list(rename.values())].copy()

# Mapping of supply curve points to counties
scgid_mapping = pd.read_csv('scgid_mapping.csv')

# Merge mapping to counties onto dataset
df = pd.merge(dfdiff, scgid_mapping.drop(columns='reeds_ba'), on='sc_gid')

# Calculate the county-level multiplier as the mean of all supply curve points within the county
df = df.groupby(by='fips').mean().drop('sc_gid', axis=1).rename_axis('r')

# Convert to differences and write
dfdiff = (df - 1).round(4)
dfdiff.to_csv('reg_cap_cost_diff_default.csv')
