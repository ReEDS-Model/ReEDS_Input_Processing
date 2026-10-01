# This script creates state-level demand multiplier files that blend MISO
# LTLF forward growth with AEO forward growth using ReEDS county-population
# weights.
#
# Historical years (2010-lastyear) reuse the AEO_Load_Projections.py approach:
# state-level demand is derived from EIA retail electricity sales and
# behind-the-meter PV generation (via EIA API), producing a state loadmult
# normalized to 2010.
#
# Projected years (lastyear+1 through 2050) use a BA-level ratio for every
# ReEDS BA in the z90 hierarchy:
#   - MISO BAs pick up the growth ratio of their MISO subregion (North /
#     Central / South) from the MISO LTLF Annual_Energy_TWh totals, normalized
#     against a first-principles ReEDS-implied subregion energy at
#     ltlf_first_year:
#         ltlf_ratio(sub, year) = LTLF_energy(sub, year)
#                               / reeds_implied_2026(sub)
#     where
#         reeds_implied_2026(sub) = sum over MISO BAs in sub of
#             (state_2010_sales * loadmult(st, lastyear)
#              * BA_pop_share_in_state)
#     This absolute LTLF anchor rebases the ReEDS 2026 level toward LTLF
#     2026 (so the MISO subregion comparison matches at the anchor year
#     without requiring a prior ReEDS run), while LTLF year-over-year
#     growth drives the trajectory from 2026 onward.
#   - Non-MISO BAs pick up the growth ratio of their state from
#     aeo_updates/Outputs/demand_AEO_{AEO_year}_{scenario}.csv, backed out by
#     dividing the AEO state multiplier by the state loadmult.
# BA ratios are aggregated to a state ratio using a population-weighted
# average of BAs within the state (weights static, sum to 1 per state).
# The final state multiplier is loadmult(st, year) * state_ratio(st, year).
#
# Output files (matching demand_AEO_{year}_{scenario}.csv schema, r=state):
#   Outputs/demand_MISOLTLF_{AEO_year}_baseline.csv  (Current Trajectory)
#   Outputs/demand_MISOLTLF_{AEO_year}_high.csv      (High Trajectory)
#   Outputs/demand_MISOLTLF_{AEO_year}_low.csv       (Low Trajectory)
#
# Prerequisite: aeo_updates/AEO_Load_Projections.py must have been run so that
# the AEO scenario CSVs exist under aeo_updates/Outputs/.

import os
import sys

import matplotlib.pyplot as plt
import pandas as pd

### Configuration

# Anchor all paths to this script's location so the workflow is portable
# regardless of the caller's working directory.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

# Make the AEO EIA helpers importable and reuse them verbatim.
AEO_DIR = os.path.join(REPO_ROOT, 'aeo_updates')
if AEO_DIR not in sys.path:
    sys.path.insert(0, AEO_DIR)
from _eia_api_functions import (
    api_key,
    create_EIA_url,
    create_SEDS_url,
    retrieve_EIA_data,
)

# lastyear is the last year that historical data are available
lastyear = 2024
# LTLF publication year is aligned with AEO2026, so use the same token
AEO_year = 2026
# First LTLF projected year (used as the LTLF ratio normalization anchor)
ltlf_first_year = 2026
# Full output year range
projection_years = range(2010, 2051)

# LTLF input workbook and sheet
LTLF_WORKBOOK = os.path.join(SCRIPT_DIR, 'MISO LTLF Data.xlsx')
LTLF_SHEET = 'Data'

# AEO scenario CSVs produced by aeo_updates/AEO_Load_Projections.py
AEO_OUTPUTS_DIR = os.path.join(REPO_ROOT, 'aeo_updates', 'Outputs')

# ReEDS reference data (external ReEDS repo checkout)
REEDS_TREES = r'C:\Users\challora\reeds-trees\main'
HIERARCHY_PATH = os.path.join(REEDS_TREES, 'inputs', 'zones', 'z90', 'hierarchy.csv')
COUNTY2ZONE_PATH = os.path.join(REEDS_TREES, 'inputs', 'zones', 'z90', 'county2zone.csv')
COUNTY_POP_PATH = os.path.join(REEDS_TREES, 'inputs', 'disaggregation', 'county_population.csv')

# Fallback hierarchy inside this repo if the external file is missing
HIERARCHY_FALLBACK = os.path.join(REPO_ROOT, 'zones', 'z90_20260216', 'hierarchy.csv')

# Output folder
OUTPUT_DIR = os.path.join(SCRIPT_DIR, 'Outputs')

# LTLF workbook TRAJECTORY values mapped to output scenario suffixes
SCENARIO_MAP = {
    'Current Trajectory': 'baseline',
    'High Trajectory':    'high',
    'Low Trajectory':     'low',
}

# LTLF MISO subregion values as they appear in the workbook ZONE/REGION col
MISO_SUBREGIONS = ['North', 'Central', 'South']

# Hardcoded state -> MISO subregion mapping. This overrides any subregion
# label carried in the hierarchy's `transgrp` column so LTLF ratios are
# applied consistently regardless of hierarchy vintage. Each MISO BA is
# still identified via hierarchy (`transreg == 'MISO'`); this map controls
# only which LTLF subregion ratio (North/Central/South) is used for MISO
# BAs sitting in each state.
MISO_STATE_TO_SUBREGION = {
    # North
    'ND': 'North', 'MN': 'North', 'IA': 'North',
    # Central
    'WI': 'Central', 'MI': 'Central', 'IL': 'Central',
    'IN': 'Central', 'KY': 'Central', 'MO': 'Central',
    # South
    'AR': 'South', 'LA': 'South', 'TX': 'South', 'MS': 'South',
}


### Helper functions

def read_hierarchy():
    """Read the ReEDS z90 hierarchy, preferring the external ReEDS-trees path
    and falling back to the workspace copy if the external file is missing."""
    if os.path.exists(HIERARCHY_PATH):
        path = HIERARCHY_PATH
    elif os.path.exists(HIERARCHY_FALLBACK):
        path = HIERARCHY_FALLBACK
        print(f"[info] Using fallback hierarchy at {HIERARCHY_FALLBACK}")
    else:
        raise FileNotFoundError(
            f"Hierarchy not found at either\n  {HIERARCHY_PATH}\n  or\n  "
            f"{HIERARCHY_FALLBACK}"
        )
    df = pd.read_csv(path)
    required = {'r', 'st', 'transreg', 'transgrp'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Hierarchy at {path} is missing required columns: {missing}"
        )
    return df[['r', 'st', 'transreg', 'transgrp']].copy()


def build_ba_population_weights(hierarchy):
    """Return a DataFrame [r, st, weight] where weight is each BA's share of
    its state's population. Weights sum to 1.0 per state.

    Uses the ReEDS-canonical county-population allocation
    (GSw_LoadAllocationMethod = 'population'): county populations from
    inputs/disaggregation/county_population.csv, mapped to BAs via
    inputs/zones/z90/county2zone.csv. State is derived from the hierarchy
    since some vintages of county2zone.csv do not carry a state column.
    """
    c2z = pd.read_csv(COUNTY2ZONE_PATH, dtype=str)
    c2z = c2z.rename(columns={'ba': 'r', 'state': 'st'})
    required = {'FIPS', 'r'}
    missing = required - set(c2z.columns)
    if missing:
        raise ValueError(
            f"county2zone at {COUNTY2ZONE_PATH} is missing columns: {missing}"
        )
    # Attach state from the hierarchy (BA -> state); drop any local st column
    c2z = c2z.drop(columns=['st'], errors='ignore').merge(
        hierarchy[['r', 'st']], on='r', how='left',
    )
    unmapped = c2z['st'].isna().sum()
    if unmapped:
        bad = c2z.loc[c2z['st'].isna(), 'r'].drop_duplicates().tolist()
        raise ValueError(
            f"{unmapped} county2zone rows reference BAs missing from hierarchy: "
            f"{bad[:10]}"
        )
    c2z['FIPS'] = 'p' + c2z['FIPS'].astype(str).str.zfill(5)

    pop = pd.read_csv(COUNTY_POP_PATH)
    # Normalize column names (file may use FIPS/fips and value/population)
    pop.columns = [c.lower() for c in pop.columns]
    if 'fips' not in pop.columns:
        raise ValueError(
            f"county_population at {COUNTY_POP_PATH} must have a 'FIPS' column; "
            f"got {list(pop.columns)}"
        )
    pop_col = 'value' if 'value' in pop.columns else 'population'
    if pop_col not in pop.columns:
        raise ValueError(
            f"county_population at {COUNTY_POP_PATH} must have a 'value' or "
            f"'population' column; got {list(pop.columns)}"
        )
    pop = pop.rename(columns={'fips': 'FIPS', pop_col: 'population'})
    pop['FIPS'] = pop['FIPS'].astype(str)
    # Handle the case where the raw file lacks the 'p' prefix
    if not pop['FIPS'].str.startswith('p').all():
        pop['FIPS'] = 'p' + pop['FIPS'].str.lstrip('p').str.zfill(5)
    pop['population'] = pd.to_numeric(pop['population'], errors='coerce').fillna(0)

    # Aggregate county populations to BA/state
    merged = c2z[['FIPS', 'r', 'st']].merge(
        pop[['FIPS', 'population']], on='FIPS', how='inner',
    )
    ba_pop = merged.groupby(['r', 'st'], as_index=False)['population'].sum()

    # Every hierarchy BA should appear in the aggregation
    missing_bas = set(hierarchy['r']) - set(ba_pop['r'])
    if missing_bas:
        raise ValueError(
            f"BAs in hierarchy have no counties in county2zone: "
            f"{sorted(missing_bas)}"
        )
    ba_pop = ba_pop[ba_pop['r'].isin(hierarchy['r'])].copy()

    state_pop = ba_pop.groupby('st')['population'].sum().rename('state_pop')
    ba_pop = ba_pop.merge(state_pop, on='st')
    zero_states = ba_pop.loc[ba_pop['state_pop'] == 0, 'st'].unique().tolist()
    if zero_states:
        raise ValueError(
            f"States with zero total population from county-population file: "
            f"{zero_states}"
        )
    ba_pop['weight'] = ba_pop['population'] / ba_pop['state_pop']

    wsum = ba_pop.groupby('st')['weight'].sum()
    max_dev = (wsum - 1.0).abs().max()
    assert max_dev < 1e-9, (
        f"BA population weights do not sum to 1.0 per state; "
        f"worst deviation = {max_dev}"
    )
    return ba_pop[['r', 'st', 'weight']].copy()


def read_ltlf_ratios(reeds_implied_2026):
    """Read LTLF Annual_Energy_TWh subregion totals per scenario and return
    ratios normalized against the ReEDS-implied 2026 subregion energy.

    For each (subregion, scenario, year >= ltlf_first_year):
        ltlf_ratio = LTLF_energy_TWh / reeds_implied_2026[subregion]

    `reeds_implied_2026` is a mapping / Series {subregion -> TWh} produced by
    `compute_reeds_implied_2026_by_subregion()`. It represents what the
    uncorrected ReEDS inputs (loadmult * historical state energy * BA pop
    share) would project for each MISO subregion at `ltlf_first_year`, so
    dividing LTLF by it rebases the LTLF curve onto an absolute ReEDS-level
    anchor. The result: when the state ratio hits 2026, the LTLF ratio is
    k = LTLF_2026 / ReEDS_implied_2026 (not 1.0), pulling the ReEDS 2026
    absolute level toward LTLF.

    Returns [scenario, subregion, year, ltlf_ratio] covering every year in
    projection_years. Pre-anchor years (2010 - ltlf_first_year - 1) are
    filled with 1.0 so historical loadmult carries the full projection
    through 2025; the step at 2025 -> 2026 (1.0 -> k) is the correction
    itself. Post-horizon years inherit the final published ratio."""
    df = pd.read_excel(LTLF_WORKBOOK, sheet_name=LTLF_SHEET)
    id_cols = ['LOAD_TYPE', 'TRAJECTORY', 'ZONE/REGION', 'DATA_TYPE', 'DRIVER']
    missing = [c for c in id_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"LTLF sheet '{LTLF_SHEET}' is missing expected columns: {missing}"
        )
    year_cols = [c for c in df.columns if c not in id_cols]

    # Filter to Net Load, Annual_Energy_TWh totals (DRIVER blank), and the
    # three MISO subregions across all three scenarios.
    driver_blank = df['DRIVER'].isna() | (df['DRIVER'].astype(str).str.strip() == '')
    mask = (
        (df['LOAD_TYPE'] == 'Net Load')
        & (df['DATA_TYPE'] == 'Annual_Energy_TWh')
        & driver_blank
        & (df['ZONE/REGION'].isin(MISO_SUBREGIONS))
        & (df['TRAJECTORY'].isin(SCENARIO_MAP.keys()))
    )
    sub = df.loc[mask, ['TRAJECTORY', 'ZONE/REGION'] + year_cols].copy()

    long = sub.melt(
        id_vars=['TRAJECTORY', 'ZONE/REGION'],
        value_vars=year_cols,
        var_name='year', value_name='energy_TWh',
    )
    long['year'] = long['year'].astype(int)
    long['energy_TWh'] = pd.to_numeric(long['energy_TWh'], errors='coerce')
    long = long.rename(columns={'ZONE/REGION': 'subregion'})
    long['scenario'] = long['TRAJECTORY'].map(SCENARIO_MAP)
    long = long.drop(columns='TRAJECTORY')

    dup = long.duplicated(subset=['scenario', 'subregion', 'year']).sum()
    if dup:
        raise ValueError(
            f"LTLF long-form has {dup} duplicate (scenario, subregion, year) rows"
        )

    # Absolute LTLF anchor: divide LTLF energy by the per-subregion
    # ReEDS-implied 2026 energy (scenario-independent). This replaces the
    # self-normalization where every scenario had ltlf_ratio = 1.0 at 2026.
    implied = pd.Series(reeds_implied_2026, name='implied_2026_TWh')
    implied.index.name = 'subregion'
    missing_sub = set(MISO_SUBREGIONS) - set(implied.index)
    if missing_sub:
        raise ValueError(
            f"reeds_implied_2026 is missing subregion(s): {sorted(missing_sub)}"
        )
    nonpos = implied[implied <= 0]
    if len(nonpos):
        raise ValueError(
            f"reeds_implied_2026 has non-positive values: {nonpos.to_dict()}"
        )
    long = long.merge(
        implied.reset_index(), on='subregion', how='left',
    )
    long['ltlf_ratio'] = long['energy_TWh'] / long['implied_2026_TWh']

    # Expand to cover 2010-2050. Missing years (pre-ltlf_first_year) get 1.0.
    combos = long[['scenario', 'subregion']].drop_duplicates().assign(_k=1)
    grid = (
        pd.DataFrame({'year': list(projection_years)}).assign(_k=1)
        .merge(combos, on='_k').drop(columns='_k')
    )
    ratios = grid.merge(
        long[['scenario', 'subregion', 'year', 'ltlf_ratio']],
        on=['scenario', 'subregion', 'year'], how='left',
    )
    # Pre-anchor (year < ltlf_first_year) missing values -> 1.0 so the
    # historical loadmult chain carries the projection unchanged through
    # 2025; the 2025 -> 2026 step is the LTLF-anchor correction.
    # Post-horizon (year > last LTLF year) missing values -> hold last LTLF
    # ratio flat (extrapolate the final published year forward through 2050).
    ltlf_last_year = int(long['year'].max())
    ratios = ratios.sort_values(['scenario', 'subregion', 'year'])
    pre_mask = ratios['year'] < ltlf_first_year
    post_mask = ratios['year'] > ltlf_last_year
    ratios.loc[pre_mask, 'ltlf_ratio'] = ratios.loc[pre_mask, 'ltlf_ratio'].fillna(1.0)
    ratios['ltlf_ratio'] = ratios.groupby(['scenario', 'subregion'])['ltlf_ratio'].ffill()
    # Defensive: any remaining NaN (should be none) -> 1.0.
    ratios['ltlf_ratio'] = ratios['ltlf_ratio'].fillna(1.0)
    print(f"    LTLF horizon: {int(long['year'].min())}..{ltlf_last_year}; "
          f"holding final ratio flat for {ltlf_last_year + 1}..{projection_years[-1]}"
          if post_mask.any() else
          f"    LTLF horizon: {int(long['year'].min())}..{ltlf_last_year}")

    # Print the absolute anchor factors at ltlf_first_year for visibility.
    anchor_rows = ratios[ratios['year'] == ltlf_first_year][
        ['scenario', 'subregion', 'ltlf_ratio']
    ].sort_values(['subregion', 'scenario'])
    print(f"    LTLF-absolute anchor factors at {ltlf_first_year} "
          f"(k = LTLF / ReEDS_implied):")
    for _, r in anchor_rows.iterrows():
        k = r['ltlf_ratio']
        print(f"      {r['subregion']:<8} {r['scenario']:<9} k = {k:.4f}  "
              f"({(k - 1) * 100:+.2f}%)")

    return ratios[['scenario', 'subregion', 'year', 'ltlf_ratio']].copy()


def compute_reeds_implied_2026_by_subregion(hierarchy, ba_weights, loadmult):
    """Estimate each MISO subregion's uncorrected ReEDS 2026 annual energy
    from first principles (no prior ReEDS run required).

    For each state ``st``:
        state_2026_TWh = state_2010_sales_GWh * loadmult(st, lastyear) / 1000

    Then each MISO BA contributes its share of the state's projected 2026
    energy, where the share is the BA's population weight within the state
    (the ReEDS-canonical GSw_LoadAllocationMethod='population' assumption).
    Finally, BA contributions are summed by MISO subregion:

        reeds_implied_2026[sub] = sum over MISO BAs in sub of
            (state_2026_TWh * ba_pop_weight_in_state)

    This correctly captures that for mixed-MISO states (e.g., TX where
    TX_MISO is a small share of ERCOT-dominated TX), only the MISO portion
    of the state's energy is counted toward the MISO subregion total. Units
    are TWh so the result is directly comparable to LTLF Annual_Energy_TWh.

    Returns a pandas Series indexed by subregion name ('North', 'Central',
    'South'), values in TWh.
    """
    # 2010 retail sales (million kWh = GWh) per state, matching the DC->MD
    # convention used in fetch_eia_state_loadmult.
    url_retail = create_EIA_url(
        api_key, 'retail-sales', ['sales'], {'sectorid': ['ALL']},
        freq='annual', start=2010, end=2010,
    )
    df_retail = retrieve_EIA_data(url_retail)
    df_retail = df_retail[['stateid', 'sales']].copy()
    df_retail['sales'] = pd.to_numeric(
        df_retail['sales'], errors='coerce',
    ).fillna(0)
    df_retail.loc[df_retail['stateid'] == 'DC', 'stateid'] = 'MD'
    df_retail = df_retail.groupby('stateid', as_index=False)['sales'].sum()
    df_retail = df_retail.rename(
        columns={'stateid': 'st', 'sales': 'sales_GWh_2010'},
    )

    # loadmult is held flat at lastyear through 2050; pick the lastyear value
    # as the projection to ltlf_first_year (2026).
    lm_last = loadmult[loadmult['year'] == lastyear][['st', 'loadmult']].rename(
        columns={'loadmult': 'loadmult_last'},
    )
    proj = df_retail.merge(lm_last, on='st', how='inner')
    proj['state_2026_TWh'] = (
        proj['sales_GWh_2010'] * proj['loadmult_last'] / 1000.0
    )

    # Each MISO BA contributes (state_2026_TWh * BA pop share in state).
    miso = hierarchy[hierarchy['transreg'] == 'MISO'].copy()
    miso['subregion'] = miso['st'].map(MISO_STATE_TO_SUBREGION)
    missing_sub = miso[miso['subregion'].isna()][['r', 'st']]
    if len(missing_sub):
        raise ValueError(
            f"MISO BAs missing from MISO_STATE_TO_SUBREGION:\n"
            f"{missing_sub.drop_duplicates()}"
        )
    miso = miso.merge(ba_weights[['r', 'weight']], on='r', how='left')
    if miso['weight'].isna().any():
        bad = miso.loc[miso['weight'].isna(), 'r'].tolist()
        raise ValueError(f"MISO BAs with no population weight: {bad}")
    miso = miso.merge(
        proj[['st', 'state_2026_TWh']], on='st', how='left',
    )
    missing_state = miso.loc[miso['state_2026_TWh'].isna(), 'st'].unique()
    if len(missing_state):
        raise ValueError(
            f"MISO states missing from EIA 2010 retail-sales projection: "
            f"{sorted(missing_state)}"
        )
    miso['ba_2026_TWh'] = miso['state_2026_TWh'] * miso['weight']
    implied = miso.groupby('subregion')['ba_2026_TWh'].sum()

    print(f"  ReEDS-implied {ltlf_first_year} subregion energy (TWh), "
          f"first-principles projection of EIA 2010 retail "
          f"x loadmult({lastyear}) x BA pop share:")
    for sub in MISO_SUBREGIONS:
        print(f"    {sub:<8} = {implied.get(sub, float('nan')):.2f} TWh")
    return implied


def fetch_eia_state_loadmult():
    """Reproduce the historical block from AEO_Load_Projections.py: combine
    EIA retail sales and BTM PV to compute state loadmult = load_year /
    load_2010 for 2010-lastyear, then carry the lastyear value forward flat
    through 2050. Returns [st, year, loadmult]."""
    # Retail sales
    url_retail = create_EIA_url(
        api_key, 'retail-sales', ['sales'], {'sectorid': ['ALL']},
        freq='annual', start=2010,
    )
    df_retail = retrieve_EIA_data(url_retail)
    df_retail = df_retail[['year', 'stateid', 'sales']].copy()
    df_retail['sales'] = pd.to_numeric(df_retail['sales'], errors='coerce').fillna(0)
    df_retail.loc[df_retail['stateid'] == 'DC', 'stateid'] = 'MD'
    df_retail = df_retail.groupby(['year', 'stateid']).agg('sum').reset_index()
    df_retail = df_retail[df_retail['year'] <= lastyear]

    # BTM PV
    url_seds = create_SEDS_url(
        api_key, series_IDs=['SOR7P', 'SOCCP', 'SOICP'],
        freq='annual', start=2009,
    )
    df_pv = retrieve_EIA_data(url_seds)
    df_pv = df_pv.rename(columns={'stateId': 'stateid'})
    df_pv = df_pv[['year', 'stateid', 'value']].copy()
    df_pv['value'] = pd.to_numeric(df_pv['value'], errors='coerce').fillna(0)
    df_pv.loc[df_pv['stateid'] == 'DC', 'stateid'] = 'MD'
    df_pv = df_pv.groupby(['year', 'stateid']).agg('sum').reset_index()
    df_pv = df_pv.rename(columns={'value': 'pvgen'})

    df = df_retail.merge(df_pv, on=['year', 'stateid'], how='left').fillna(0)
    df['load'] = df['sales'] + df['pvgen']

    df_2010 = df.loc[df['year'] == 2010, ['stateid', 'load']].rename(
        columns={'load': 'load_2010'},
    )
    df = df.merge(df_2010, on='stateid', how='left')
    df['loadmult'] = df['load'] / df['load_2010']
    df = df[['year', 'stateid', 'loadmult']]

    # Carry lastyear value forward to cover lastyear+1 through 2050
    future = pd.DataFrame({'year': range(lastyear + 1, 2051)}).assign(_k=1)
    states = df[['stateid']].drop_duplicates().assign(_k=1)
    grid = future.merge(states, on='_k').drop(columns='_k')
    last = df.loc[df['year'] == lastyear, ['stateid', 'loadmult']]
    grid = grid.merge(last, on='stateid', how='left')
    df = pd.concat([df, grid], ignore_index=True)
    df = df.rename(columns={'stateid': 'st'})
    return df[['st', 'year', 'loadmult']].copy()


def read_aeo_state_ratios(loadmult):
    """Read demand_AEO_{AEO_year}_{scenario}.csv and back out each state's
    AEO growth ratio by dividing the AEO multiplier by our state loadmult.
    Since the AEO script uses the same EIA-based loadmult, this exactly
    recovers the AEO cendiv ratio.

    Returns [scenario, st, year, aeo_ratio]."""
    frames = []
    for suffix in SCENARIO_MAP.values():
        path = os.path.join(AEO_OUTPUTS_DIR, f'demand_AEO_{AEO_year}_{suffix}.csv')
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"Required AEO scenario file not found: {path}\n"
                f"Run aeo_updates/AEO_Load_Projections.py first to produce it."
            )
        df = pd.read_csv(path)
        df = df.rename(columns={'r': 'st'})
        df['scenario'] = suffix
        frames.append(df[['scenario', 'st', 'year', 'multiplier']])
    aeo = pd.concat(frames, ignore_index=True)
    aeo = aeo.merge(loadmult, on=['st', 'year'], how='left')
    if aeo['loadmult'].isna().any():
        missing = aeo.loc[aeo['loadmult'].isna(), ['st', 'year']].drop_duplicates()
        raise ValueError(
            f"AEO rows without a matching state loadmult (first few):\n"
            f"{missing.head()}"
        )
    if (aeo['loadmult'] == 0).any():
        raise ValueError("Zero state loadmult while extracting AEO ratio.")
    aeo['aeo_ratio'] = aeo['multiplier'] / aeo['loadmult']
    return aeo[['scenario', 'st', 'year', 'aeo_ratio']].copy()


### Main workflow

def main():
    print('Reading hierarchy and building BA population weights...')
    hierarchy = read_hierarchy()
    ba_weights = build_ba_population_weights(hierarchy)

    # BA -> MISO subregion (values 'North' / 'Central' / 'South') for MISO BAs.
    # Subregion is assigned from the hardcoded MISO_STATE_TO_SUBREGION map
    # (keyed on the BA's state), NOT from hierarchy.transgrp.
    miso = hierarchy[hierarchy['transreg'] == 'MISO'].copy()
    miso['subregion'] = miso['st'].map(MISO_STATE_TO_SUBREGION)
    unmapped = miso[miso['subregion'].isna()]
    if len(unmapped):
        bad = unmapped[['r', 'st']].drop_duplicates()
        raise ValueError(
            f"{len(bad)} MISO BAs are in states missing from "
            f"MISO_STATE_TO_SUBREGION; add these states to the map:\n{bad}"
        )
    expected_miso_bas = 16
    if len(miso) != expected_miso_bas:
        print(
            f"[warn] Expected {expected_miso_bas} MISO BAs in z90 hierarchy, "
            f"found {len(miso)}. Proceeding with what was read."
        )
    ba_subregion = miso[['r', 'subregion']].copy()

    print('Fetching EIA historical state loadmult...')
    loadmult = fetch_eia_state_loadmult()

    print('Computing ReEDS-implied 2026 subregion energy '
          '(first-principles, no prior ReEDS run needed)...')
    reeds_implied_2026 = compute_reeds_implied_2026_by_subregion(
        hierarchy, ba_weights, loadmult,
    )

    print('Reading MISO LTLF workbook...')
    ltlf = read_ltlf_ratios(reeds_implied_2026)

    print('Reading AEO scenario multipliers...')
    aeo_ratios = read_aeo_state_ratios(loadmult)

    # ---- Build BA-level ratios --------------------------------------------
    scenarios = list(SCENARIO_MAP.values())
    ba_grid = (
        hierarchy[['r', 'st']].assign(_k=1)
        .merge(pd.DataFrame({'year': list(projection_years)}).assign(_k=1), on='_k')
        .merge(pd.DataFrame({'scenario': scenarios}).assign(_k=1), on='_k')
        .drop(columns='_k')
    )

    # Attach subregion (NaN for non-MISO BAs), then the corresponding ratios
    ba_grid = ba_grid.merge(ba_subregion, on='r', how='left')
    ba_grid = ba_grid.merge(
        ltlf.rename(columns={'ltlf_ratio': 'miso_ratio'}),
        on=['scenario', 'subregion', 'year'], how='left',
    )
    ba_grid = ba_grid.merge(aeo_ratios, on=['scenario', 'st', 'year'], how='left')
    # MISO BAs use LTLF ratio; non-MISO BAs use their state's AEO ratio
    ba_grid['ba_ratio'] = ba_grid['miso_ratio'].where(
        ba_grid['subregion'].notna(), ba_grid['aeo_ratio'],
    )
    if ba_grid['ba_ratio'].isna().any():
        bad = ba_grid.loc[ba_grid['ba_ratio'].isna(), ['r', 'st', 'year', 'scenario']]
        raise ValueError(
            f"{len(bad)} BA-ratio rows are NaN after merges; first few:\n"
            f"{bad.head()}"
        )

    # ---- Population-weighted state ratio ----------------------------------
    ba_grid = ba_grid.merge(ba_weights[['r', 'weight']], on='r', how='left')
    if ba_grid['weight'].isna().any():
        bad = ba_grid.loc[ba_grid['weight'].isna(), 'r'].drop_duplicates().tolist()
        raise ValueError(f"BAs with no population weight: {bad}")
    ba_grid['weighted'] = ba_grid['ba_ratio'] * ba_grid['weight']
    state_ratio = (
        ba_grid.groupby(['scenario', 'st', 'year'], as_index=False)['weighted']
        .sum().rename(columns={'weighted': 'state_ratio'})
    )

    # ---- Compose final state multiplier -----------------------------------
    out = state_ratio.merge(loadmult, on=['st', 'year'], how='left')
    if out['loadmult'].isna().any():
        raise ValueError('Missing loadmult after merge into state ratios')
    out['multiplier'] = out['loadmult'] * out['state_ratio']
    out = out.rename(columns={'st': 'r'})[['r', 'year', 'scenario', 'multiplier']]

    # ---- Write scenario CSVs ----------------------------------------------
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for suffix in scenarios:
        df = out[out['scenario'] == suffix].drop(columns='scenario').copy()
        df = df.sort_values(['r', 'year']).reset_index(drop=True)
        path = os.path.join(OUTPUT_DIR, f'demand_MISOLTLF_{AEO_year}_{suffix}.csv')
        df.to_csv(path, index=False)
        print(f'Wrote {path}  ({len(df):,} rows, {df["r"].nunique()} states)')

    # ---- Optional plots (set MISO_LTLF_PLOT=1 to enable) -------------------
    if os.environ.get('MISO_LTLF_PLOT', '0') == '1':
        for suffix in scenarios:
            df = out[out['scenario'] == suffix]
            plt.figure(figsize=(10, 6))
            for r in df['r'].unique():
                df_r = df[df['r'] == r]
                plt.plot(df_r['year'], df_r['multiplier'], label=r)
            plt.title(f'MISO LTLF Demand Multipliers by State - {suffix}')
            plt.xlabel('Year')
            plt.ylabel('Demand Multiplier')
            plt.legend(title='State', bbox_to_anchor=(1.05, 1), loc='upper left',
                       fontsize=7, ncol=2)
            plt.grid()
            plt.tight_layout()
            plt.show()


if __name__ == '__main__':
    main()
