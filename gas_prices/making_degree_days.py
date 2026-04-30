import pandas as pd
import numpy as np

#read in the monthly degree days from https://ftp.cpc.ncep.noaa.gov/htdocs/products/analysis_monitoring/cdus/degree_days/archives/
degdays = pd.read_csv("data/heating_cooling.csv")

#read in population forecast from https://www.coopercenter.org/research/how-accurate-are-our-50-state-population-projections
pop = pd.read_csv("data/NationalProjections_ProjectedTotalPopulation_2030-2050.csv")

#read in the mapping 
mappy = pd.read_csv("data/state_groups.csv")
missing_regs = mappy[mappy['usda_region'].isin(['mountain', 'pacific'])]

#map of state to state abbrev
state_abbrev = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'District of Columbia': 'DC',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC', 
    'North Dakota': 'ND',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY'
}

pop['st'] = pop['Geography Name'].map(state_abbrev)
pop = pop[pop['st'].isin(missing_regs['st'])]

#linear interpolation in pop to get numbers for 2010-2050
id_cols = ['st', 'FIPS', 'Geography Name']

# isolate the known year columns
year_cols = ['2020', '2030', '2040', '2050']
pop[year_cols] = pop[year_cols].replace(',', '', regex=True).astype(float)

# make a numeric copy
pop_years = pop[year_cols].copy()
pop_years.columns = pop_years.columns.astype(int)

# expand to every year you want
full_years = list(range(2010, 2051))
pop_years = pop_years.reindex(columns=full_years)

# interpolate horizontally across years
pop_years = pop_years.interpolate(axis=1, method='linear', limit_direction='both')

# put identifiers back
pop_full = pd.concat([pop[id_cols], pop_years], axis=1)

degdaysr = degdays[degdays['state_abbrev'].isin(missing_regs['st'])]
degdaysr = degdaysr[degdaysr['year'] >= 2010]

months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
degdaysr['annual'] = degdaysr[months].sum(axis=1)

ddr = degdaysr[['state_abbrev', 'year', 'annual', 'element_code']]
ddr["reg"] = ddr['state_abbrev'].map(missing_regs.set_index('st')['gasreg'])

pop_long = (
    pop_full
    .melt(id_vars=id_cols, var_name='year', value_name='pop')
    .rename(columns={'st': 'state_abbrev'})
)

pop_long['year'] = pop_long['year'].astype(int)
pop_long['pop'] = pd.to_numeric(pop_long['pop'], errors='coerce')
ddr2 = ddr.merge(
    pop_long[['state_abbrev', 'year', 'pop']],
    on=['state_abbrev', 'year'],
    how='left'
)

# population share within each region-year-element
ddr2['pop_share'] = (
    ddr2['pop'] /
    ddr2.groupby(['reg', 'year', 'element_code'])['pop'].transform('sum')
)

# weighted contribution
ddr2['weighted_component'] = ddr2['annual'] * ddr2['pop_share']

# final NEMS-style population-weighted degree days
ddr_reg = (
    ddr2.groupby(['reg', 'year', 'element_code'], as_index=False)
        .agg(annual_pw=('weighted_component', 'sum'))
)

kdegc = pd.read_csv("data/kdegday_census_divs.csv")
reg_to_usda = {
    'California': 'Pacific',
    'Northwest': 'Pacific',
    'Mountain': 'Mountain',
    'Southwest': 'Mountain'
}
kdegc_long = kdegc.melt(
    id_vars=['year', 'type'],
    value_vars=['Mountain', 'Pacific'],
    var_name='usda_region',
    value_name='usda_dd'
)

# map element_code
kdegc_long['element_code'] = kdegc_long['type'].map({
    'heating': 25,
    'cooling': 26
})
base_2025 = ddr_reg[ddr_reg['year'] == 2025].copy()
base_2025['usda_region'] = base_2025['reg'].map(reg_to_usda)

# get 2025 USDA values
usda_2025 = kdegc_long[kdegc_long['year'] == 2025][
    ['usda_region', 'element_code', 'usda_dd']
].rename(columns={'usda_dd': 'usda_2025'})

# merge and compute ratios
kdegc_future = kdegc_long.merge(
    usda_2025,
    on=['usda_region', 'element_code'],
    how='left'
)

kdegc_future['ratio'] = kdegc_future['usda_dd'] / kdegc_future['usda_2025']
future = []

for _, row in base_2025.iterrows():
    reg = row['reg']
    ec = row['element_code']
    base_val = row['annual_pw']
    usda_reg = row['usda_region']

    temp = kdegc_future[
        (kdegc_future['usda_region'] == usda_reg) &
        (kdegc_future['element_code'] == ec) &
        (kdegc_future['year'] > 2025)
    ].copy()

    temp['reg'] = reg
    temp['annual_pw'] = base_val * temp['ratio']

    future.append(temp[['reg', 'year', 'element_code', 'annual_pw']])


future_df = pd.concat(future, ignore_index=True)
ddr_reg_full = pd.concat([ddr_reg, future_df], ignore_index=True)


kdegc_targets = kdegc_long[['year', 'element_code', 'usda_region', 'usda_dd']].rename(
    columns={'usda_dd': 'target_total'}
)

out = ddr_reg_full.copy()
out['usda_region'] = out['reg'].map(reg_to_usda)

# keep only the rows we want to enforce
mask = out['usda_region'].notna()

# current sum within each USDA bucket
out['current_bucket_total'] = np.nan
out.loc[mask, 'current_bucket_total'] = (
    out.loc[mask]
       .groupby(['year', 'element_code', 'usda_region'])['annual_pw']
       .transform('sum')
)

# merge target totals
out = out.merge(
    kdegc_targets,
    on=['year', 'element_code', 'usda_region'],
    how='left'
)

# scale factor
out['scale'] = 1.0
m = mask & out['current_bucket_total'].notna() & out['target_total'].notna() & (out['current_bucket_total'] != 0)
out.loc[m, 'scale'] = out.loc[m, 'target_total'] / out.loc[m, 'current_bucket_total']

# enforced values
out['annual_pw_enforced'] = out['annual_pw']
out.loc[m, 'annual_pw_enforced'] = out.loc[m, 'annual_pw'] * out.loc[m, 'scale']

# final cleaned df
ddr_reg_full_enforced = out[['reg', 'year', 'element_code', 'annual_pw_enforced']].rename(
    columns={'annual_pw_enforced': 'annual_pw'}
)

ddr_reg_full_enforced.sort_values(['reg', 'year'], inplace=True)
ddrc = ddr_reg_full_enforced[ddr_reg_full_enforced['element_code'] == 26].copy()
ddrh = ddr_reg_full_enforced[ddr_reg_full_enforced['element_code'] == 25].copy()
#make reg of ddr_reg_full_enforced be columns
ddrc = ddrc.pivot(index='year', columns='reg', values='annual_pw')
ddrh = ddrh.pivot(index='year', columns='reg', values='annual_pw')
#fix index
ddrc.reset_index(inplace=True)
ddrh.reset_index(inplace=True)
#make them all ints
ddrc['year'] = ddrc['year'].astype(int)
ddrh['year'] = ddrh['year'].astype(int)
for col in ddrc.columns:
    if col != 'year':
        ddrc[col] = ddrc[col].astype(int)
for col in ddrh.columns:
    if col != 'year':
        ddrh[col] = ddrh[col].astype(int)

heating = pd.read_csv("data/ngreg_hdd.csv")
heating.drop(columns=['Mountain','California','Northwest','Southwest'], inplace=True)
cooling = pd.read_csv("data/ngreg_cdd.csv")
cooling.drop(columns=['Mountain','California','Northwest','Southwest'], inplace=True)

heating['California'] = ddrh['California']
heating['Mountain'] = ddrh['Mountain']
heating['Northwest'] = ddrh['Northwest']
heating['Southwest'] = ddrh['Southwest']
cooling['California'] = ddrc['California']
cooling['Mountain'] = ddrc['Mountain']
cooling['Northwest'] = ddrc['Northwest']
cooling['Southwest'] = ddrc['Southwest']

cooling.to_csv("data/gasreg_cdd.csv", index=False)
heating.to_csv("data/gasreg_hdd.csv", index=False)