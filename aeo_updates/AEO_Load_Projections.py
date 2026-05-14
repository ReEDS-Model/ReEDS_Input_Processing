# This script creates state-level demand multiplier files for AEO scenarios.
#
# For historical years (2010-lastyear), state-level demand is derived from
# EIA retail electricity sales and behind-the-meter PV generation (via EIA API).
#
# For projected years (lastyear+1 through 2050), demand growth is computed from:
#   - AEO electricity consumption CSVs (outputs/ folder, by census division, in quads)
#   - Regional rooftop PV generation from an EIA-provided Excel file (residential +
#     commercial, by census division, in trillion Btu)
# These two are added to reconstruct gross electricity demand, then normalized to the
# first AEO projected year so the historical and projected series join cleanly.

import os
import pandas as pd
from _eia_api_functions import api_key, create_EIA_url, create_SEDS_url, retrieve_EIA_data
import matplotlib.pyplot as plt

### Set up information

# lastyear is the last year that historical data are available
lastyear = 2024
AEO_year = 2026

### Helper functions

# Census division number to name mapping (from "census division mapping" tab
# in AEO2026_bldgs_pv_gen_cb_high_low_economic_growth_2026-04-27.xlsx)
_DIV_TO_CENDIV = {
    1: 'NewEngland', 2: 'MiddleAtlantic', 3: 'EastNorthCentral',
    4: 'WestNorthCentral', 5: 'SouthAtlantic', 6: 'EastSouthCentral',
    7: 'WestSouthCentral', 8: 'Mountain', 9: 'Pacific',
}

# Mapping from AEO CSV column-name format to no-spaces cendiv format
_CENDIV_NAME_MAP = {
    'East North Central': 'EastNorthCentral',
    'East South Central': 'EastSouthCentral',
    'Middle Atlantic': 'MiddleAtlantic',
    'Mountain': 'Mountain',
    'New England': 'NewEngland',
    'Pacific': 'Pacific',
    'South Atlantic': 'SouthAtlantic',
    'West North Central': 'WestNorthCentral',
    'West South Central': 'WestSouthCentral',
}


def read_dgpv_from_excel(xl_file, sheet_name):
    """Read census-division DGPV (all sectors, residential + commercial) from
    an EIA AEO2026 Excel sheet. Returns long-format DataFrame with
    columns [cendiv, year, dgpv] where dgpv is in quads."""
    df = pd.read_excel(xl_file, sheet_name=sheet_name, header=None)

    # Section headers are rows where column 1 == 'Division'
    header_rows = df[df.iloc[:, 1].astype(str).str.strip() == 'Division'].index.tolist()

    records = []
    for header_row in header_rows:
        # Extract (year, column_index) pairs from this header row
        year_cols = []
        for col in range(2, df.shape[1]):
            raw = df.iloc[header_row, col]
            if pd.isna(raw):
                continue
            try:
                year_cols.append((int(float(raw)), col))
            except (ValueError, TypeError):
                pass

        # Read the 9 division rows immediately following the header
        for row_idx in range(header_row + 1, header_row + 11):
            if row_idx >= len(df):
                break
            div_raw = df.iloc[row_idx, 1]
            if pd.isna(div_raw) or str(div_raw).strip() == 'Grand Total':
                break
            try:
                div = int(float(div_raw))
            except (ValueError, TypeError):
                break
            cendiv = _DIV_TO_CENDIV[div]
            for yr, col in year_cols:
                val = df.iloc[row_idx, col]
                if val == '-' or pd.isna(val):
                    val = 0.0
                records.append({'cendiv': cendiv, 'year': yr, 'dgpv': float(val)})

    df_dgpv = pd.DataFrame(records)
    df_dgpv = df_dgpv.groupby(['cendiv', 'year'])['dgpv'].sum().reset_index()
    # Convert trillion Btu to quads
    df_dgpv['dgpv'] = df_dgpv['dgpv'] / 1000
    return df_dgpv


def read_aeo_electricity(csv_path):
    """Read an AEO electricity consumption CSV and return long-format
    DataFrame with columns [year, cendiv, aeo_electricity] in quads."""
    df = pd.read_csv(csv_path)
    rename_map = {}
    for col in df.columns:
        if col in ('year', 'units'):
            continue
        for long_name, short_name in _CENDIV_NAME_MAP.items():
            if long_name in col:
                rename_map[col] = short_name
                break
    df = df.rename(columns=rename_map)
    cendiv_cols = [c for c in df.columns if c in _CENDIV_NAME_MAP.values()]
    df_long = df.melt(id_vars=['year'], value_vars=cendiv_cols,
                      var_name='cendiv', value_name='aeo_electricity')
    df_long['year'] = df_long['year'].astype(int)
    df_long['aeo_electricity'] = pd.to_numeric(df_long['aeo_electricity'],
                                               errors='coerce').fillna(0)
    return df_long


### Load and process data

# Import state to census division mapping file
st_cendiv = pd.read_csv(os.path.join('inputs', 'st_cendiv.csv'))
# Rename columns to match EIA data
st_cendiv.rename(columns={'st':'stateid'}, inplace=True)

# Scenario configuration: maps scenario names to input files
dgpv_file = 'AEO2026_bldgs_pv_gen_cb_high_low_economic_growth_2026-04-27.xlsx'

scenario_config = [
    {
        'name': 'Counterfactual Baseline',
        'elec_csv': os.path.join('outputs', f'AEO_CB2026_{AEO_year}_electricity_consumption.csv'),
        'dgpv_sheet': 'AEO2026 Counterfactual Baseline',
    },
    {
        'name': 'High Economic Growth',
        'elec_csv': os.path.join('outputs', f'AEO_HM2026_{AEO_year}_electricity_consumption.csv'),
        'dgpv_sheet': 'AEO2026 High Economic Growth',
    },
    {
        'name': 'Low Economic Growth',
        'elec_csv': os.path.join('outputs', f'AEO_LM2026_{AEO_year}_electricity_consumption.csv'),
        'dgpv_sheet': 'AEO2026 Low Economic Growth',
    },
]

# Collect state-level retail sales data from EIA API
url_retail = create_EIA_url(api_key, 'retail-sales', ['sales'],
                            {'sectorid':['ALL']}, freq='annual', start=2010)
df_retail = retrieve_EIA_data(url_retail)
df_retail = df_retail[['year', 'stateid', 'sales']].copy()
df_retail['sales'] = pd.to_numeric(df_retail['sales'], errors='coerce').fillna(0)
df_retail.replace({'stateid': 'DC'}, 'MD', inplace=True)
df_retail = df_retail.groupby(by=['year', 'stateid']
                              ).agg('sum').reset_index(drop=False)
# There is more recent retail sales data than PV generation data, so drop rows with years greater than lastyear
df_retail = df_retail[df_retail['year'] <= lastyear]

# Collect state-level behind-the-meter PV generation data from SEDS via EIA API
# SOR7P = Residential PV
# SOCCP = Commercial PV
# SOICP = Industrial PV
url_seds = create_SEDS_url(api_key,
                           series_IDs=['SOR7P','SOCCP','SOICP'],
                           freq='annual', start=2009)
df_pv = retrieve_EIA_data(url_seds)
# Rename stateId to stateid to match retail sales df
df_pv.rename(columns={'stateId':'stateid'}, inplace=True)
df_pv2 = df_pv[['year', 'stateid', 'value']].copy()
df_pv2.replace({'stateid': 'DC'}, 'MD', inplace=True)
# Convert value column to numbers
df_pv2['value'] = pd.to_numeric(df_pv2['value'], errors='coerce').fillna(0)
df_pv3 = df_pv2.groupby(by=['year', 'stateid']
                      ).agg('sum').reset_index(drop=False)
# Rename value column to generation
df_pv3.rename(columns={'value':'pvgen'}, inplace=True)

# Combine retail sales and behind-the-meter PV generation
df_combined = df_retail.merge(df_pv3, on=['year', 'stateid'], how='left').fillna(0)

# Add load column (sales + pv generation)
df_combined['load'] = df_combined['sales'] + df_combined['pvgen']
# Drop sales and pvgen columns
df_combined.drop(columns=['sales', 'pvgen'], inplace=True)

# Build demand ratios from AEO electricity consumption + DGPV for each scenario.
# Ratios are normalized so that the first AEO projected year (2025) = 1.0 for each cendiv.
# This avoids unit-mismatch between historical EIA data (million kWh) and AEO data (quads).
# Historical years (2010-lastyear) use ratio = 1.0 so the state-level loadmult drives
# the multiplier for those years; AEO growth trajectory applies from lastyear onward.
aeo_first_year = 2025
all_cendivs = list(_DIV_TO_CENDIV.values())
hist_years = list(range(2010, lastyear + 1))  # 2010 through lastyear inclusive
all_ratios = []

# Add historical rows with ratio = 1.0 for all scenarios
for cfg in scenario_config:
    hist_rows = pd.DataFrame([
        {'scenario': cfg['name'], 'cendiv': cd, 'year': yr, 'ratio': 1.0}
        for cd in all_cendivs
        for yr in hist_years
    ])
    all_ratios.append(hist_rows)

# Add projected rows (2025-2050) normalized to ratio = 1.0 in aeo_first_year
for cfg in scenario_config:
    df_elec = read_aeo_electricity(cfg['elec_csv'])
    df_dgpv = read_dgpv_from_excel(dgpv_file, cfg['dgpv_sheet'])

    df_future = df_elec.merge(df_dgpv, on=['year', 'cendiv'], how='left').fillna(0)
    df_future['total_demand'] = df_future['aeo_electricity'] + df_future['dgpv']

    # Normalize so ratio = 1.0 in aeo_first_year for each cendiv
    base = (df_future[df_future['year'] == aeo_first_year][['cendiv', 'total_demand']]
            .rename(columns={'total_demand': 'demand_base'}))
    df_future = df_future.merge(base, on='cendiv')
    df_future['ratio'] = df_future['total_demand'] / df_future['demand_base']
    df_future['scenario'] = cfg['name']
    all_ratios.append(df_future[['scenario', 'cendiv', 'year', 'ratio']])

demand_ratios = pd.concat(all_ratios, ignore_index=True)
# Drop duplicate lastyear rows (historical block already added lastyear = 1.0)
demand_ratios = demand_ratios.drop_duplicates(subset=['scenario', 'cendiv', 'year'], keep='last')

# Normalize load by 2010 load
df_load_2010 = df_combined[df_combined['year'] == 2010][['stateid', 'load']].rename(columns={'load':'load_2010'})
df_load = df_combined.merge(df_load_2010, on='stateid', how='left')
df_load['loadmult'] = df_load['load'] / df_load['load_2010']
df_load.drop(columns=['load_2010','load'], inplace=True)

# Fill in future years using a value of 1.0 for loadmult
future_years = pd.DataFrame({
    'year': range(2025, 2051)
}).assign(key=1)

unique_states = df_load[['stateid']].drop_duplicates().assign(key=1)

df_future_yrs = pd.merge(future_years, unique_states, on='key').drop(columns=['key'])

# Fill loadmult with the value from lastyear
lastyear_loadmult = df_load[df_load['year'] == lastyear][['stateid', 'loadmult']]
df_future_yrs = df_future_yrs.merge(lastyear_loadmult, on='stateid', how='left')

df_load2 = pd.concat([df_load, df_future_yrs], ignore_index=True)

# Map states to census divisions
df_load3 = df_load2.merge(st_cendiv, on='stateid', how='left').dropna()

# Remove spaces from cendiv names and make lower case for merging
df_load3['cendiv'] = df_load3['cendiv'].str.strip().str.replace(' ', '').str.lower()
demand_ratios['cendiv'] = demand_ratios['cendiv'].str.lower()

# Merge with demand ratios
df_loadtot = df_load3.merge(demand_ratios, on=['year', 'cendiv'], how='left')

# Check stateid = "ND" for verification
df_check = df_loadtot[df_loadtot['stateid'] == 'ND'].copy()

# Combine loadmult and ratio to get final ratio
df_loadtot['multiplier'] = df_loadtot['loadmult'] * df_loadtot['ratio']
df_loadtot.drop(columns=['loadmult','ratio','cendiv'], inplace=True)
# Rename stateid to r
df_loadtot.rename(columns={'stateid':'r'}, inplace=True)
# Reorder columns
df_loadtot = df_loadtot[['r', 'year', 'scenario', 'multiplier']].copy()


# Split out into different scenarios
df_low = df_loadtot[df_loadtot['scenario']=='Low Economic Growth'].copy()
df_baseline = df_loadtot[df_loadtot['scenario']=='Counterfactual Baseline'].copy()
df_high = df_loadtot[df_loadtot['scenario']=='High Economic Growth'].copy()

# Drop scenario column
df_low.drop(columns=['scenario'], inplace=True)
df_baseline.drop(columns=['scenario'], inplace=True)
df_high.drop(columns=['scenario'], inplace=True)

# Plot the multipliers by r for each scenario (optional)
for scenario, df in zip(['Low', 'Baseline', 'High'], [df_low, df_baseline, df_high]):
    plt.figure(figsize=(10,6))
    for r in df['r'].unique():
        df_r = df[df['r'] == r]
        plt.plot(df_r['year'], df_r['multiplier'], label=r)
    plt.title(f'Demand Multipliers by Region - {scenario} Scenario')
    plt.xlabel('Year')
    plt.ylabel('Demand Multiplier')
    plt.legend(title='Region', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid()
    plt.tight_layout()
    plt.show()

# Save to csv files

# Create Outputs directory if it doesn't exist
Output_folder = 'Outputs'
if not os.path.exists(Output_folder):
    os.makedirs(Output_folder)

df_low.to_csv(os.path.join(Output_folder, 'demand_AEO_{}_low.csv'.format(AEO_year)), index=False)
df_baseline.to_csv(os.path.join(Output_folder, 'demand_AEO_{}_baseline.csv'.format(AEO_year)), index=False)
df_high.to_csv(os.path.join(Output_folder, 'demand_AEO_{}_high.csv'.format(AEO_year)), index=False)
