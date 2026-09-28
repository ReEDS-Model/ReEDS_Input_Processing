# Compare ReEDS-projected MISO load (from a ReEDS run's inputs_case/load.h5)
# with the input MISO LTLF workbook.
#
# Pipeline:
#   1. Read hourly BA load from load.h5 for each ReEDS model year.
#   2. Subtract hourly BA distributed-PV generation:
#         dpv_gen(BA, ts) = distpvcap(BA, year) * recf_dpv(BA, ts)
#      Both inputs come from the same ReEDS run's inputs_case (recf.h5,
#      distpvcap.csv). DPV capacity is linearly interpolated between the
#      even-year columns of distpvcap.csv.
#   3. Strip industrial direct-use (on-site cogen consumed behind the
#      meter, excluded from grid-served demand) proportionally per BA:
#         adjusted(BA, ts) = (load - dpv_gen) * (1 - direct_frac[state])
#      where direct_frac is the EIA anchor-year (default 2010) state ratio
#      of direct-use / (retail + direct-use). Because loadmult(y) scales
#      the historical base which contains direct-use, the constant state
#      fraction correctly strips direct-use at every projected year.
#   4. Aggregate MISO BAs to N / Central / South / MISO_Total using the
#      hardcoded MISO_STATE_TO_SUBREGION from MISO_LTLF_Load_Projections.py.
#   5. Split subregion hourly series into 15 weather years, compute annual
#      energy (TWh) and annual coincident peak (MW) per weather year, then
#      report the mean across weather years alongside min/max diagnostics.
#   6. Compare with MISO LTLF workbook (Annual_Energy_TWh and
#      Annual_Coincident_Peak_MW).
#
# Note: Both ReEDS load.h5 and MISO LTLF Net Load represent the bulk-power
# demand the transmission system serves (i.e. include distribution losses),
# so no loss-factor adjustment is applied.
#
# Outputs (per scenario found under REEDS_RUN_ROOT):
#   Outputs/ltlf_comparison_{scenario}.csv
#   Outputs/ltlf_comparison_energy_{scenario}.png
#   Outputs/ltlf_comparison_peak_{scenario}.png

import os
import re
import sys
import h5py
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

sys.path.insert(0, SCRIPT_DIR)
from MISO_LTLF_Load_Projections import (  # noqa: E402
    LTLF_SHEET,
    LTLF_WORKBOOK,
    MISO_STATE_TO_SUBREGION,
    MISO_SUBREGIONS,
    SCENARIO_MAP,
)

### Configuration

# Root of the ReEDS-trees runs directory containing per-scenario run folders.
REEDS_RUN_ROOT = r'C:\Users\challora\reeds-trees\miso-ltlf-2026\runs'
RUN_TEMPLATE = 'v20260928_LTLF_{scenario}'
INPUTS_CASE_DIR = 'inputs_case'

# Model years to compare (ReEDS solves at 5-yr steps in this range).
COMPARE_YEARS = [2030, 2035, 2040, 2045]

# Ordered subregion labels for output tables and plots.
SUBREGION_ORDER = ['North', 'Central', 'South', 'MISO_Total']

# Subregions to break down by BA for diagnostic purposes (energy + peak per
# BA per year, mean across weather years). Written to
# 'ltlf_comparison_{scenario}_by_ba.csv' and printed to stdout.
DIAGNOSTIC_SUBREGIONS = ['South']

# Base-year diagnostic configuration. Two independent comparisons:
#   Table A (state-level):  ReEDS load.h5 model-year `y` per state (all BAs,
#     summed) vs `EIA_loadbystate.csv` for that state, for y in
#     BASE_YEARS_STATE. Comparison is gross (no DPV or direct-use
#     subtraction) because EIA_loadbystate values include both BTM PV and
#     direct-use per its downstream use.
#   Table B (subregion-level): ReEDS at LTLF_BASE_YEAR, DPV-subtracted and
#     direct-use-stripped (per-state fraction), vs MISO LTLF workbook
#     absolute values at the same year. Matches the main-loop pipeline.
EIA_LOADBYSTATE_PATH = (
    r'C:\Users\challora\reeds-trees\miso-ltlf-2026\inputs\load'
    r'\EIA_loadbystate.csv'
)
BASE_YEARS_STATE = [2010, 2024]     # 2010 = hourlize anchor; 2024 = latest EIA
LTLF_BASE_YEAR = 2026               # LTLF workbook anchor year

# Direct-use (on-site industrial cogen consumed behind the meter) is not
# grid-served, so MISO LTLF Net Load excludes it. ReEDS load.h5 inherits
# a direct-use share from the historical hourlize base timeseries, which
# then scales with loadmult(y). To compare against LTLF we subtract a
# constant per-state direct-use fraction (measured at the hourlize anchor
# year) from load.h5 in the main year loop and Table B. Table A stays
# gross because EIA_loadbystate.csv also includes direct-use.
DIRECT_USE_ANCHOR_YEAR = 2010

OUTPUT_DIR = os.path.join(SCRIPT_DIR, 'Outputs')


### Helpers

def run_path(scenario, *rest):
    return os.path.join(
        REEDS_RUN_ROOT, RUN_TEMPLATE.format(scenario=scenario),
        INPUTS_CASE_DIR, *rest,
    )


def _extract_ltlf_metric(df, data_type, zones, value_col, year_cols):
    """Filter LTLF workbook rows to (LOAD_TYPE='Net Load', DATA_TYPE=data_type,
    DRIVER blank, ZONE/REGION in `zones`, TRAJECTORY in scenario map) and
    return long-form DataFrame [scenario, subregion, year, value_col]."""
    driver_blank = (
        df['DRIVER'].isna() | (df['DRIVER'].astype(str).str.strip() == '')
    )
    mask = (
        (df['LOAD_TYPE'] == 'Net Load')
        & (df['DATA_TYPE'] == data_type)
        & driver_blank
        & (df['ZONE/REGION'].isin(zones))
        & (df['TRAJECTORY'].isin(SCENARIO_MAP.keys()))
    )
    sub = df.loc[mask, ['TRAJECTORY', 'ZONE/REGION'] + year_cols].copy()
    if sub.empty:
        return pd.DataFrame(columns=['scenario', 'subregion', 'year', value_col])
    long = sub.melt(
        id_vars=['TRAJECTORY', 'ZONE/REGION'], value_vars=year_cols,
        var_name='year', value_name=value_col,
    )
    long['year'] = long['year'].astype(int)
    long[value_col] = pd.to_numeric(long[value_col], errors='coerce')
    long = long.rename(columns={'ZONE/REGION': 'subregion'})
    long['scenario'] = long['TRAJECTORY'].map(SCENARIO_MAP)
    return long[['scenario', 'subregion', 'year', value_col]]


def read_ltlf_absolute():
    """Read absolute annual energy and coincident peak from LTLF workbook.

    Returns (energy_df, peak_df) with columns:
      energy_df: [scenario, subregion, year, ltlf_energy_TWh]
      peak_df:   [scenario, subregion, year, ltlf_peak_MW]
    Subregion values include 'North', 'Central', 'South', and 'MISO_Total'.
    MISO_Total uses the workbook's own 'MISO' row if present; otherwise
    energy is summed and peak is left NaN (peaks are non-additive)."""
    df = pd.read_excel(LTLF_WORKBOOK, sheet_name=LTLF_SHEET)
    id_cols = ['LOAD_TYPE', 'TRAJECTORY', 'ZONE/REGION', 'DATA_TYPE', 'DRIVER']
    missing = [c for c in id_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"LTLF sheet '{LTLF_SHEET}' missing expected columns: {missing}"
        )
    year_cols = [c for c in df.columns if c not in id_cols]

    energy_sub = _extract_ltlf_metric(
        df, 'Annual_Energy_TWh', MISO_SUBREGIONS, 'ltlf_energy_TWh', year_cols,
    )
    peak_sub = _extract_ltlf_metric(
        df, 'Annual_Coincident_Peak_MW', MISO_SUBREGIONS, 'ltlf_peak_MW',
        year_cols,
    )

    if energy_sub.empty:
        raise ValueError(
            "Could not find 'Annual_Energy_TWh' rows for MISO subregions in "
            "the LTLF workbook."
        )
    if peak_sub.empty:
        peak_types = sorted(
            df.loc[
                df['DATA_TYPE'].astype(str).str.contains(
                    'peak', case=False, na=False,
                ),
                'DATA_TYPE',
            ].unique().tolist()
        )
        raise ValueError(
            "Could not find 'Annual_Coincident_Peak_MW' rows in the LTLF "
            f"workbook. Peak-like DATA_TYPE values present: {peak_types}"
        )

    # MISO totals: prefer a direct 'MISO' row if the workbook has one.
    energy_total = _extract_ltlf_metric(
        df, 'Annual_Energy_TWh', ['MISO'], 'ltlf_energy_TWh', year_cols,
    )
    peak_total = _extract_ltlf_metric(
        df, 'Annual_Coincident_Peak_MW', ['MISO'], 'ltlf_peak_MW', year_cols,
    )

    if not energy_total.empty:
        energy_total['subregion'] = 'MISO_Total'
    else:
        print("[info] LTLF workbook has no 'MISO' total row for energy; "
              "summing subregions.")
        agg = energy_sub.groupby(
            ['scenario', 'year'], as_index=False,
        )['ltlf_energy_TWh'].sum()
        agg['subregion'] = 'MISO_Total'
        energy_total = agg[['scenario', 'subregion', 'year', 'ltlf_energy_TWh']]

    if not peak_total.empty:
        peak_total['subregion'] = 'MISO_Total'
    else:
        print("[info] LTLF workbook has no 'MISO' total row for coincident "
              "peak; peaks are non-additive so MISO_Total LTLF peak left "
              "blank.")
        # Emit an empty MISO_Total peak so the merge works.
        peak_total = pd.DataFrame({
            'scenario': peak_sub['scenario'].unique().repeat(
                peak_sub['year'].nunique(),
            ),
            'subregion': 'MISO_Total',
            'year': list(peak_sub['year'].unique()) * peak_sub['scenario'].nunique(),
            'ltlf_peak_MW': float('nan'),
        })

    energy = pd.concat([energy_sub, energy_total], ignore_index=True)
    peak = pd.concat([peak_sub, peak_total], ignore_index=True)
    return energy, peak


def read_run_hierarchy(scenario):
    p = run_path(scenario, 'hierarchy.csv')
    if not os.path.exists(p):
        raise FileNotFoundError(f"Run hierarchy not found: {p}")
    df = pd.read_csv(p)
    # ReEDS run inputs_case hierarchy uses GAMS convention '*r' for the first
    # column; normalize to 'r' so downstream code doesn't need to know.
    if '*r' in df.columns and 'r' not in df.columns:
        df = df.rename(columns={'*r': 'r'})
    return df


def build_ba_subregion_map(hierarchy):
    """Return DataFrame [r, st, subregion] for MISO BAs whose state is in
    MISO_STATE_TO_SUBREGION. BAs in other states are excluded with a warning
    printed so they are visible in the log."""
    miso = hierarchy[hierarchy['transreg'] == 'MISO'].copy()
    miso['subregion'] = miso['st'].map(MISO_STATE_TO_SUBREGION)
    dropped = miso[miso['subregion'].isna()][['r', 'st']]
    if len(dropped):
        print(f"[warn] {len(dropped)} MISO BAs are in states outside the "
              f"MISO_STATE_TO_SUBREGION map; excluded from comparison:")
        print(dropped.to_string(index=False))
    miso = miso.dropna(subset=['subregion'])
    return miso[['r', 'st', 'subregion']].reset_index(drop=True)


def read_distpvcap(scenario, target_years, miso_bas):
    """Read inputs_case/distpvcap.csv and linearly interpolate to target_years.
    Returns wide DataFrame index=r, columns=target_years."""
    p = run_path(scenario, 'distpvcap.csv')
    if not os.path.exists(p):
        raise FileNotFoundError(f"distpvcap.csv not found: {p}")
    df = pd.read_csv(p)
    df = df.set_index('r')
    df.columns = df.columns.astype(int)
    # Ensure years are ordered and add any missing target years as NaN.
    all_years = sorted(set(df.columns) | set(target_years))
    df = df.reindex(columns=all_years).interpolate(axis=1, method='linear')
    df = df[target_years]

    missing = [b for b in miso_bas if b not in df.index]
    if missing:
        print(f"[warn] {len(missing)} MISO BAs missing from distpvcap.csv "
              f"(assumed 0 DPV): {missing}")
    return df.reindex(miso_bas).fillna(0.0)


def _read_reeds_h5(path):
    """Load a ReEDS-formatted .h5 written by reeds.io.write_profile_to_h5.

    Expected root-level datasets:
        - 'data'    : the 2D array
        - 'columns' : column labels (bytes)
        - 'index_0', 'index_1', ... : one dataset per index level
        - optional 'index_names', 'column_names'

    Returns a DataFrame with the appropriate (Multi)Index. Datetime-like
    index levels (name == 'datetime') are converted to pandas timestamps.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    with h5py.File(path, 'r') as f:
        keys = list(f.keys())
        if 'data' not in keys:
            raise ValueError(
                f"'data' dataset not found in {path}. Root keys: {keys}"
            )
        data = f['data'][:]
        columns = pd.Series(f['columns'][:]).map(
            lambda x: x.decode('utf-8') if isinstance(x, bytes) else x
        ).values

        idx_keys = sorted([k for k in keys if re.match(r'index_[0-9]+$', k)])
        index_arrays = []
        for k in idx_keys:
            vals = f[k][:]
            if vals.dtype.kind == 'S' or (
                len(vals) and isinstance(vals[0], bytes)
            ):
                vals = pd.Series(vals).str.decode('utf-8').values
            index_arrays.append(vals)

        if 'index_names' in keys:
            index_names = pd.Series(f['index_names'][:]).map(
                lambda x: x.decode('utf-8') if isinstance(x, bytes) else x
            ).tolist()
        else:
            index_names = [None] * len(index_arrays)

    df = pd.DataFrame(data, columns=columns)
    if len(index_arrays) == 1:
        idx = pd.Index(index_arrays[0], name=index_names[0])
    else:
        idx = pd.MultiIndex.from_arrays(index_arrays, names=index_names)
    df.index = idx

    # Parse any 'datetime' index level to pandas Timestamps.
    df = _parse_h5_datetime_index(df)

    # Cast float16 up to float32 to avoid precision issues in sums/diffs.
    float16_cols = [c for c in df.columns if df[c].dtype == np.float16]
    if float16_cols:
        df = df.astype({c: np.float32 for c in float16_cols})
    return df


def _parse_h5_datetime_index(df):
    """If the DataFrame's index (or a MultiIndex level) is named 'datetime',
    coerce it to pandas Timestamps."""
    if isinstance(df.index, pd.MultiIndex):
        if 'datetime' not in (df.index.names or []):
            return df
        lvl = df.index.names.index('datetime')
        raw = df.index.get_level_values(lvl)
        parsed = pd.to_datetime(raw, format='ISO8601', errors='coerce')
        arrays = [
            parsed if i == lvl else df.index.get_level_values(i)
            for i in range(df.index.nlevels)
        ]
        df.index = pd.MultiIndex.from_arrays(arrays, names=df.index.names)
    else:
        if df.index.name == 'datetime':
            df.index = pd.to_datetime(df.index, format='ISO8601',
                                      errors='coerce')
    return df


def read_recf_dpv(scenario, miso_bas):
    """Read hourly per-BA distributed-PV capacity factor from recf.h5.

    ReEDS stores per-resource CF profiles with column names formatted as
    '{tech}|{r}' (e.g. 'distpv|AR_MISO'). We keep only 'distpv|*' columns
    and strip the prefix so columns match ReEDS BAs.

    Note: ReEDS pre-scales distpv CF by 1/(1-distloss) so it represents
    gross generation (matching how load.h5 accounts for DPV).

    Returns a DataFrame indexed by weather datetime, columns = miso_bas.
    """
    p = run_path(scenario, 'recf.h5')
    df = _read_reeds_h5(p)
    print(f"    recf.h5 loaded: {df.shape[0]} timestamps x {df.shape[1]} "
          f"resources; index name={df.index.name}")

    dpv_cols = [c for c in df.columns if str(c).startswith('distpv|')]
    if not dpv_cols:
        sample = list(df.columns[:5])
        raise ValueError(
            f"No 'distpv|*' columns in recf.h5 at {p}. Column sample: "
            f"{sample}"
        )
    df = df[dpv_cols]
    df.columns = [c.split('|', 1)[1] for c in dpv_cols]
    print(f"    recf.h5 distpv columns: {len(df.columns)} BAs "
          f"(sample: {list(df.columns[:5])})")

    if not isinstance(df.index, pd.DatetimeIndex):
        try:
            df.index = pd.to_datetime(df.index)
        except (ValueError, TypeError) as err:
            raise ValueError(
                f"recf.h5 index is not datetime-like: "
                f"{df.index[:3].tolist()} ({err})"
            ) from err

    missing = [b for b in miso_bas if b not in df.columns]
    if missing:
        print(f"[warn] {len(missing)} MISO BAs missing from recf.h5 DPV CF "
              f"(assumed 0 CF): {missing}")
        for b in missing:
            df[b] = 0.0
    return df[list(miso_bas)]


def read_load_all(scenario, miso_bas, return_full=False):
    """Read hourly BA load from load.h5. Returns a DataFrame with MultiIndex
    (model_year, weather_datetime) and columns = miso_bas.

    If `return_full=True`, returns `(load_miso, load_full)` where `load_full`
    contains all BAs (not just MISO). Both frames share the same normalized
    year-int MultiIndex; datetime coercion for the second level is left to
    the caller (they normalize once and the change propagates)."""
    p = run_path(scenario, 'load.h5')
    print("    Reading load.h5 (may take a moment)...")
    df = _read_reeds_h5(p)
    print(f"    load.h5 loaded: {df.shape[0]} rows x {df.shape[1]} BAs; "
          f"index names={df.index.names}")

    if not isinstance(df.index, pd.MultiIndex) or df.index.nlevels != 2:
        raise ValueError(
            f"Expected 2-level MultiIndex in load.h5 (model_year, datetime); "
            f"got index={df.index.__class__.__name__} nlevels="
            f"{getattr(df.index, 'nlevels', 1)}"
        )
    # Ensure model-year level is int (h5py may return as string or float).
    year_lvl_i = 0
    if 'year' in df.index.names:
        year_lvl_i = df.index.names.index('year')
    year_vals = df.index.get_level_values(year_lvl_i)
    if not pd.api.types.is_integer_dtype(year_vals.dtype):
        arrays = [
            year_vals.astype(int) if i == year_lvl_i
            else df.index.get_level_values(i)
            for i in range(df.index.nlevels)
        ]
        df.index = pd.MultiIndex.from_arrays(arrays, names=df.index.names)

    missing = [b for b in miso_bas if b not in df.columns]
    if missing:
        raise ValueError(
            f"MISO BAs missing from load.h5 columns "
            f"(first 10): {missing[:10]}"
        )
    load_miso = df[list(miso_bas)]
    if return_full:
        return load_miso, df
    return load_miso


def read_eia_state_load(year):
    """Return a Series (index=state abbrev, values=TWh) of annual load for
    `year` from `EIA_LOADBYSTATE_PATH`. Raises if the file or year is missing.

    Note: EIA_loadbystate.csv values are in MWh and represent state total
    end-use load (retail sales + BTM PV self-consumption), matching how the
    ReEDS load scaling pipeline consumes them downstream.
    """
    if not os.path.exists(EIA_LOADBYSTATE_PATH):
        raise FileNotFoundError(
            f"EIA_loadbystate.csv not found: {EIA_LOADBYSTATE_PATH}"
        )
    df = pd.read_csv(EIA_LOADBYSTATE_PATH)
    sub = df.loc[df['year'] == year]
    if sub.empty:
        available = sorted(df['year'].unique())
        raise ValueError(
            f"Year {year} not present in {EIA_LOADBYSTATE_PATH}. "
            f"Available: {available[0]}..{available[-1]}"
        )
    return (sub.set_index('st')['MWh'] / 1e6).astype(float)


def fetch_state_direct_use_fraction(anchor_year=DIRECT_USE_ANCHOR_YEAR):
    """Return ``{state_abbrev: direct_use / (retail_sales + direct_use)}``
    for `anchor_year`, hitting the EIA API v2. Values are dimensionless and
    computed from same-year annual state totals.

    Sources:
      * retail-sales (data=sales, sectorid=ALL): million kWh = GWh
      * state-electricity-profiles/source-disposition (data=direct-use):
        megawatt-hours -> divide by 1000 to reach GWh.

    Behavior:
      * Cache result to ``Outputs/state_direct_use_fraction_{year}.csv``;
        subsequent calls read the cache.
      * If ``EIA_API_KEY`` is unset AND cache is missing, emit ``[warn]`` and
        return an empty dict (caller uses 0.0 for missing states -> behaves
        like HEAD, i.e. no direct-use subtraction).
    """
    cache_path = os.path.join(
        OUTPUT_DIR, f'state_direct_use_fraction_{anchor_year}.csv',
    )
    if os.path.exists(cache_path):
        cached = pd.read_csv(cache_path)
        return dict(zip(cached['st'], cached['direct_use_frac']))

    # Lazy import so users without EIA_API_KEY can still run the comparison
    # if a cache file already exists (checked above).
    aeo_updates_dir = os.path.join(REPO_ROOT, 'aeo_updates')
    if aeo_updates_dir not in sys.path:
        sys.path.insert(0, aeo_updates_dir)
    try:
        from _eia_api_functions import (
            api_key, create_EIA_url, retrieve_EIA_data,
        )
    except (ImportError, ValueError) as err:
        print(f"  [warn] direct-use fraction unavailable ({err}); "
              f"proceeding without direct-use subtraction.")
        return {}

    print(f"  Fetching state direct-use fraction for anchor year "
          f"{anchor_year}...")
    try:
        url_retail = create_EIA_url(
            api_key, 'retail-sales', ['sales'], {'sectorid': ['ALL']},
            freq='annual', start=anchor_year, end=anchor_year,
        )
        df_retail = retrieve_EIA_data(url_retail)
        df_retail = df_retail.rename(columns={'stateid': 'stateid'})
        df_retail = df_retail[['stateid', 'sales']].copy()
        df_retail['sales'] = pd.to_numeric(
            df_retail['sales'], errors='coerce',
        ).fillna(0.0)
        df_retail.loc[df_retail['stateid'] == 'DC', 'stateid'] = 'MD'
        df_retail = df_retail.groupby('stateid', as_index=False)['sales'].sum()

        url_direct = create_EIA_url(
            api_key, 'state-electricity-profiles/source-disposition',
            ['direct-use'], {}, freq='annual',
            start=anchor_year, end=anchor_year,
        )
        df_direct = retrieve_EIA_data(url_direct)
        df_direct = df_direct.rename(columns={'state': 'stateid'})
        df_direct = df_direct[['stateid', 'direct-use']].copy()
        # source-disposition returns MWh; retail-sales.sales is GWh.
        df_direct['direct-use'] = pd.to_numeric(
            df_direct['direct-use'], errors='coerce',
        ).fillna(0.0) / 1000.0
        df_direct.loc[df_direct['stateid'] == 'DC', 'stateid'] = 'MD'
        df_direct = df_direct.groupby(
            'stateid', as_index=False,
        )['direct-use'].sum()
    except Exception as err:  # noqa: BLE001
        print(f"  [warn] EIA API fetch failed ({err}); proceeding without "
              f"direct-use subtraction.")
        return {}

    merged = df_retail.merge(df_direct, on='stateid', how='outer').fillna(0.0)
    denom = merged['sales'] + merged['direct-use']
    merged['direct_use_frac'] = np.where(
        denom > 0, merged['direct-use'] / denom, 0.0,
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out = merged.rename(columns={'stateid': 'st'})[
        ['st', 'sales', 'direct-use', 'direct_use_frac']
    ]
    out.to_csv(cache_path, index=False)
    print(f"    -> cached to {os.path.relpath(cache_path, REPO_ROOT)}")

    return dict(zip(out['st'], out['direct_use_frac']))


### Per-scenario processing

def compute_scenario_metrics(scenario, ba_map, hierarchy, ltlf_energy):
    """Return `(subregion_df, per_ba_df, state_base_df, subregion_base_df)`
    for this scenario, or all-`None` if the run folder is absent/incomplete.

    - `subregion_df`: long-form energy/peak metrics per (year, subregion).
    - `per_ba_df`: energy/peak metrics per (year, subregion, ba), restricted
      to subregions listed in `DIAGNOSTIC_SUBREGIONS`.
    - `state_base_df`: state-level ReEDS-vs-EIA base-year comparison (Table A).
    - `subregion_base_df`: subregion-level ReEDS-vs-LTLF LTLF_BASE_YEAR
      comparison (Table B).
    """
    run_dir = os.path.join(
        REEDS_RUN_ROOT, RUN_TEMPLATE.format(scenario=scenario),
    )
    if not os.path.isdir(run_dir):
        print(f"[skip] Run folder not found for scenario '{scenario}': "
              f"{run_dir}")
        return None, None, None, None

    # Skip gracefully if the run hasn't finished producing all required
    # inputs_case files yet (e.g. run still in progress).
    required = ['recf.h5', 'load.h5', 'distpvcap.csv', 'hierarchy.csv']
    missing = [
        f for f in required if not os.path.exists(run_path(scenario, f))
    ]
    if missing:
        print(f"[skip] Scenario '{scenario}' run is incomplete "
              f"(missing in inputs_case: {missing}).")
        return None, None, None, None

    print(f"\n=== Scenario: {scenario} ===")

    miso_bas = ba_map['r'].tolist()
    print(f"  {len(miso_bas)} MISO BAs to analyze.")

    # DPV target years include LTLF_BASE_YEAR so Table B can DPV-adjust its
    # base-year ReEDS load consistently with the main comparison pipeline.
    dpv_target_years = sorted(set(COMPARE_YEARS + [LTLF_BASE_YEAR]))
    print("  Reading distpvcap.csv (linear interpolation to target years)...")
    dpv_cap = read_distpvcap(scenario, dpv_target_years, miso_bas)

    print("  Reading recf.h5 DPV hourly CF...")
    dpv_cf = read_recf_dpv(scenario, miso_bas)
    if not isinstance(dpv_cf.index, pd.DatetimeIndex):
        try:
            dpv_cf.index = pd.to_datetime(dpv_cf.index)
        except (ValueError, TypeError) as err:
            raise ValueError(
                f"recf.h5 index is not datetime-like: "
                f"{dpv_cf.index[:3].tolist()} ({err})"
            ) from err

    # Read full BA load frame once; downstream we use `load_all` (MISO only)
    # for the main pipeline and `load_full` (all BAs) for Table A state
    # aggregation.
    _, load_full = read_load_all(scenario, miso_bas, return_full=True)
    load_datetime_level = load_full.index.get_level_values(1)
    if not isinstance(load_datetime_level, pd.DatetimeIndex):
        try:
            new_idx = pd.MultiIndex.from_arrays([
                load_full.index.get_level_values(0),
                pd.to_datetime(load_datetime_level),
            ], names=load_full.index.names)
            load_full.index = new_idx
        except (ValueError, TypeError) as err:
            raise ValueError(
                f"load.h5 datetime level is not datetime-like: "
                f"{load_datetime_level[:3].tolist()} ({err})"
            ) from err
    load_all = load_full[list(miso_bas)]

    # State map (BA -> USPS state); used both for Table A aggregation and
    # for the per-BA direct-use multiplier applied to load.h5 below.
    state_map = dict(zip(hierarchy['r'], hierarchy['st']))

    # Direct-use is not grid-served (excluded from MISO LTLF Net Load) but is
    # baked into load.h5 via the historical hourlize base and scales with
    # loadmult(y). Strip it proportionally per state using an EIA anchor-year
    # fraction. Missing states -> 0.0 (no subtraction).
    direct_frac = fetch_state_direct_use_fraction(DIRECT_USE_ANCHOR_YEAR)
    ba_direct_multiplier = pd.Series(
        {ba: 1.0 - direct_frac.get(state_map.get(ba), 0.0)
         for ba in miso_bas},
        dtype=float,
    )
    miso_states_seen = sorted({
        state_map.get(ba) for ba in miso_bas if state_map.get(ba)
    })
    reported = {
        st: direct_frac.get(st, 0.0) for st in miso_states_seen
    }
    print(f"  Direct-use fraction (anchor {DIRECT_USE_ANCHOR_YEAR}): "
          + ", ".join(f"{st}={f:.3f}" for st, f in reported.items()))

    available_years = sorted(load_all.index.get_level_values(0).unique())
    for y in COMPARE_YEARS:
        if y not in available_years:
            raise ValueError(
                f"Model year {y} not present in load.h5. Available: "
                f"{available_years}"
            )
    print(f"  load.h5 model years available: "
          f"{available_years[0]}..{available_years[-1]} "
          f"(comparing {COMPARE_YEARS})")
    print(f"    Full list: {available_years}")

    rows = []
    per_ba_rows = []
    for year in COMPARE_YEARS:
        load = load_all.xs(year, level=0)   # index=datetime, cols=BA
        common_ts = load.index.intersection(dpv_cf.index)
        if len(common_ts) == 0:
            raise ValueError(
                f"No overlapping timestamps between load.h5 model year "
                f"{year} and recf.h5 DPV CF index."
            )
        if len(common_ts) < len(load.index):
            print(f"  [warn] year {year}: {len(load.index) - len(common_ts)} "
                  f"load timestamps not present in DPV CF; dropped.")
        load = load.loc[common_ts]
        cf = dpv_cf.loc[common_ts]

        cap_row = dpv_cap[year].reindex(miso_bas).fillna(0.0)   # MW per BA
        dpv_gen = cf.multiply(cap_row, axis=1)                  # MW per BA per hr
        pre_direct = load - dpv_gen                              # MW
        adjusted = pre_direct.mul(ba_direct_multiplier, axis=1)  # strip direct-use
        direct_gen = pre_direct - adjusted                       # MW (diagnostic)

        wy = adjusted.index.year

        for sub_name in SUBREGION_ORDER:
            if sub_name == 'MISO_Total':
                bas = miso_bas
            else:
                bas = ba_map.loc[ba_map['subregion'] == sub_name, 'r'].tolist()
            if not bas:
                rows.append({
                    'scenario': scenario, 'year': year,
                    'subregion': sub_name,
                    'reeds_energy_TWh_mean': float('nan'),
                    'reeds_energy_TWh_min': float('nan'),
                    'reeds_energy_TWh_max': float('nan'),
                    'reeds_peak_MW_mean': float('nan'),
                    'reeds_peak_MW_min': float('nan'),
                    'reeds_peak_MW_max': float('nan'),
                    'dpv_energy_TWh_mean': float('nan'),
                    'direct_use_energy_TWh_mean': float('nan'),
                    'n_bas': 0,
                })
                continue

            sub_hourly = adjusted[bas].sum(axis=1)   # MW per hour
            energy_by_wy = sub_hourly.groupby(wy).sum() / 1e6   # MWh -> TWh
            peak_by_wy = sub_hourly.groupby(wy).max()           # MW

            dpv_hourly = dpv_gen[bas].sum(axis=1)
            dpv_energy_by_wy = dpv_hourly.groupby(wy).sum() / 1e6

            direct_hourly = direct_gen[bas].sum(axis=1)
            direct_energy_by_wy = direct_hourly.groupby(wy).sum() / 1e6

            rows.append({
                'scenario': scenario,
                'year': year,
                'subregion': sub_name,
                'reeds_energy_TWh_mean': energy_by_wy.mean(),
                'reeds_energy_TWh_min': energy_by_wy.min(),
                'reeds_energy_TWh_max': energy_by_wy.max(),
                'reeds_peak_MW_mean': peak_by_wy.mean(),
                'reeds_peak_MW_min': peak_by_wy.min(),
                'reeds_peak_MW_max': peak_by_wy.max(),
                'dpv_energy_TWh_mean': dpv_energy_by_wy.mean(),
                'direct_use_energy_TWh_mean': direct_energy_by_wy.mean(),
                'n_bas': len(bas),
            })

            # Per-BA diagnostic breakdown for selected subregions.
            if sub_name in DIAGNOSTIC_SUBREGIONS:
                for ba in bas:
                    ba_hourly = adjusted[ba]
                    ba_energy_wy = ba_hourly.groupby(wy).sum() / 1e6
                    ba_peak_wy = ba_hourly.groupby(wy).max()
                    ba_dpv_wy = dpv_gen[ba].groupby(wy).sum() / 1e6
                    ba_direct_wy = direct_gen[ba].groupby(wy).sum() / 1e6
                    per_ba_rows.append({
                        'scenario': scenario,
                        'year': year,
                        'subregion': sub_name,
                        'ba': ba,
                        'reeds_energy_TWh_mean': ba_energy_wy.mean(),
                        'reeds_energy_TWh_min': ba_energy_wy.min(),
                        'reeds_energy_TWh_max': ba_energy_wy.max(),
                        'reeds_peak_MW_mean': ba_peak_wy.mean(),
                        'reeds_peak_MW_min': ba_peak_wy.min(),
                        'reeds_peak_MW_max': ba_peak_wy.max(),
                        'dpv_energy_TWh_mean': ba_dpv_wy.mean(),
                        'direct_use_energy_TWh_mean': ba_direct_wy.mean(),
                    })

    # --- Table A: state-level base-year comparison (ReEDS vs EIA) ---
    # Compute state totals from `load_full` (all BAs, not just MISO) for each
    # year in BASE_YEARS_STATE. Aggregate BA->state via `hierarchy`. Compare to
    # EIA_loadbystate values. Comparison is gross-of-DPV on both sides.
    #
    # If a target base year is not present in load.h5 (only solve years are
    # stored), snap to the nearest available year. This is semantically valid
    # because ReEDS's state multiplier is flat between adjacent solve years
    # for the historical anchor period.
    miso_states = sorted(ba_map['st'].unique())
    # Per-state BA counts, computed once.
    n_bas_state = hierarchy.groupby('st')['r'].count().to_dict()
    n_bas_miso = ba_map.groupby('st')['r'].count().to_dict()

    def _nearest_year(target, avail):
        return int(min(avail, key=lambda y: abs(y - target)))

    state_rows = {st: {'st': st,
                       'n_bas_state': int(n_bas_state.get(st, 0)),
                       'n_bas_miso': int(n_bas_miso.get(st, 0))}
                  for st in miso_states}
    for by in BASE_YEARS_STATE:
        if by in available_years:
            by_reeds = by
        else:
            by_reeds = _nearest_year(by, available_years)
            print(f"  [note] Table A: {by} not in load.h5; using "
                  f"load.h5({by_reeds}) as ReEDS proxy for target {by}.")
        # Sum load across weather years, take mean across weather years.
        load_by = load_full.xs(by_reeds, level=0)
        wy_by = load_by.index.year
        ba_energy_by_wy = load_by.groupby(wy_by).sum() / 1e6   # TWh per BA/wy
        ba_energy_mean = ba_energy_by_wy.mean()                # Series over BAs
        ba_states = ba_energy_mean.index.to_series().map(state_map)
        unmapped_mask = ba_states.isna()
        if unmapped_mask.any():
            unmapped_bas = ba_energy_mean.index[unmapped_mask].tolist()
            print(f"  [warn] Year {by_reeds}: {int(unmapped_mask.sum())} BAs "
                  f"in load.h5 have no hierarchy state (excluded): "
                  f"{unmapped_bas[:5]}"
                  + ("..." if len(unmapped_bas) > 5 else ""))
            ba_energy_mean = ba_energy_mean[~unmapped_mask]
            ba_states = ba_states[~unmapped_mask]
        reeds_state_totals = ba_energy_mean.groupby(ba_states).sum()
        # EIA lookup uses the ORIGINAL target year (semantics: load.h5 at
        # by_reeds encodes loadmult(target) via ReEDS's flat-carry-forward
        # for post-history years).
        try:
            eia_state_totals = read_eia_state_load(by)
        except (FileNotFoundError, ValueError) as err:
            print(f"  [warn] Table A EIA lookup for {by}: {err}")
            eia_state_totals = pd.Series(dtype=float)
        for st in miso_states:
            reeds_v = float(reeds_state_totals.get(st, float('nan')))
            eia_v = float(eia_state_totals.get(st, float('nan')))
            diff_pct = (100.0 * (reeds_v - eia_v) / eia_v
                        if (pd.notna(eia_v) and eia_v) else float('nan'))
            state_rows[st][f'reeds_{by}_TWh'] = reeds_v
            state_rows[st][f'eia_{by}_TWh'] = eia_v
            state_rows[st][f'diff_{by}_pct'] = diff_pct

    # Order columns: identifiers, then per-year triplets in BASE_YEARS_STATE order.
    col_order = ['st', 'n_bas_state', 'n_bas_miso']
    for by in BASE_YEARS_STATE:
        col_order.extend([f'reeds_{by}_TWh', f'eia_{by}_TWh', f'diff_{by}_pct'])
    state_base_df = pd.DataFrame(
        [state_rows[st] for st in miso_states]
    )[col_order]

    # --- Table B: subregion base-year comparison (ReEDS vs LTLF) ---
    # Same DPV-subtraction pipeline as the main year loop, applied at
    # LTLF_BASE_YEAR (snapped to nearest available load.h5 year if needed).
    # LTLF workbook lookup uses the ORIGINAL LTLF_BASE_YEAR since the ReEDS
    # multiplier at LTLF_BASE_YEAR anchors ratio=1.0 to that same year.
    subregion_base_rows = []
    if LTLF_BASE_YEAR in available_years:
        ltlf_year_reeds = LTLF_BASE_YEAR
    else:
        ltlf_year_reeds = _nearest_year(LTLF_BASE_YEAR, available_years)
        print(f"  [note] Table B: LTLF_BASE_YEAR {LTLF_BASE_YEAR} not in "
              f"load.h5; using load.h5({ltlf_year_reeds}) as ReEDS proxy.")

    load_by = load_all.xs(ltlf_year_reeds, level=0)
    common_ts = load_by.index.intersection(dpv_cf.index)
    if len(common_ts) == 0:
        print(f"  [warn] No overlapping timestamps between load.h5 model "
              f"year {ltlf_year_reeds} and DPV CF; skipping Table B.")
    else:
        load_by = load_by.loc[common_ts]
        cf_by = dpv_cf.loc[common_ts]
        # DPV capacity is only computed for dpv_target_years; use the closest
        # available column to ltlf_year_reeds.
        if ltlf_year_reeds in dpv_cap.columns:
            dpv_year_col = ltlf_year_reeds
        else:
            dpv_year_col = _nearest_year(
                ltlf_year_reeds, list(dpv_cap.columns),
            )
        cap_row_by = dpv_cap[dpv_year_col].reindex(miso_bas).fillna(0.0)
        dpv_gen_by = cf_by.multiply(cap_row_by, axis=1)
        pre_direct_by = load_by - dpv_gen_by
        # Strip direct-use with the same per-BA state fraction used in the
        # main year loop; keeps Table B semantics aligned with LTLF Net Load.
        adjusted_by = pre_direct_by.mul(ba_direct_multiplier, axis=1)
        wy_by = adjusted_by.index.year
        ltlf_at_base = ltlf_energy[
            (ltlf_energy['scenario'] == scenario)
            & (ltlf_energy['year'] == LTLF_BASE_YEAR)
        ]
        for sub_name in SUBREGION_ORDER:
            if sub_name == 'MISO_Total':
                bas = miso_bas
            else:
                bas = ba_map.loc[ba_map['subregion'] == sub_name,
                                 'r'].tolist()
            if not bas:
                continue
            sub_hourly = adjusted_by[bas].sum(axis=1)
            energy_by_wy = sub_hourly.groupby(wy_by).sum() / 1e6
            reeds_val = float(energy_by_wy.mean())
            ltlf_v = ltlf_at_base[
                ltlf_at_base['subregion'] == sub_name
            ]['ltlf_energy_TWh']
            ltlf_val = float(ltlf_v.iloc[0]) if len(ltlf_v) else float('nan')
            diff_pct = (100.0 * (reeds_val - ltlf_val) / ltlf_val
                        if (pd.notna(ltlf_val) and ltlf_val)
                        else float('nan'))
            subregion_base_rows.append({
                'scenario': scenario,
                'reeds_year': ltlf_year_reeds,
                'ltlf_year': LTLF_BASE_YEAR,
                'subregion': sub_name,
                'ltlf_TWh': ltlf_val,
                'reeds_TWh_mean': reeds_val,
                'diff_TWh': reeds_val - ltlf_val,
                'diff_pct': diff_pct,
            })
    subregion_base_df = pd.DataFrame(subregion_base_rows)

    return (pd.DataFrame(rows), pd.DataFrame(per_ba_rows),
            state_base_df, subregion_base_df)


def merge_with_ltlf(reeds, ltlf_energy, ltlf_peak):
    e = ltlf_energy.rename(columns={})
    p = ltlf_peak.rename(columns={})
    out = reeds.merge(e, on=['scenario', 'subregion', 'year'], how='left')
    out = out.merge(p, on=['scenario', 'subregion', 'year'], how='left')
    out['energy_diff_pct'] = (
        100.0
        * (out['reeds_energy_TWh_mean'] - out['ltlf_energy_TWh'])
        / out['ltlf_energy_TWh']
    )
    out['peak_diff_pct'] = (
        100.0
        * (out['reeds_peak_MW_mean'] - out['ltlf_peak_MW'])
        / out['ltlf_peak_MW']
    )
    col_order = [
        'scenario', 'year', 'subregion', 'n_bas',
        'ltlf_energy_TWh', 'reeds_energy_TWh_mean',
        'reeds_energy_TWh_min', 'reeds_energy_TWh_max',
        'energy_diff_pct',
        'ltlf_peak_MW', 'reeds_peak_MW_mean',
        'reeds_peak_MW_min', 'reeds_peak_MW_max',
        'peak_diff_pct',
        'dpv_energy_TWh_mean',
        'direct_use_energy_TWh_mean',
    ]
    return out[col_order]


def print_comparison(df):
    if df is None or df.empty:
        return
    show = df[[
        'year', 'subregion',
        'ltlf_energy_TWh', 'reeds_energy_TWh_mean', 'energy_diff_pct',
        'ltlf_peak_MW', 'reeds_peak_MW_mean', 'peak_diff_pct',
        'dpv_energy_TWh_mean',
        'direct_use_energy_TWh_mean',
    ]].copy()
    show = show.sort_values(
        ['year', 'subregion'],
        key=lambda s: s.map(
            {v: i for i, v in enumerate(SUBREGION_ORDER)},
        ) if s.name == 'subregion' else s,
    )
    with pd.option_context('display.max_rows', None,
                           'display.max_columns', None,
                           'display.width', 200):
        print(show.to_string(
            index=False, float_format=lambda x: f"{x:9.2f}",
        ))


def make_plots(df, scenario):
    if df is None or df.empty:
        return

    for kind, ylabel, ltlf_col, reeds_col in [
        ('energy', 'Annual Energy (TWh)',
         'ltlf_energy_TWh', 'reeds_energy_TWh_mean'),
        ('peak', 'Annual Coincident Peak (MW)',
         'ltlf_peak_MW', 'reeds_peak_MW_mean'),
    ]:
        fig, axes = plt.subplots(2, 2, figsize=(12, 8), sharex=True)
        axes = axes.flatten()
        for ax, sub in zip(axes, SUBREGION_ORDER):
            d = df[df['subregion'] == sub].sort_values('year')
            if d.empty:
                ax.set_title(f"{sub} (no data)")
                continue
            ax.plot(d['year'], d[ltlf_col], marker='o', label='LTLF')
            ax.plot(d['year'], d[reeds_col], marker='s', label='ReEDS (adj)')
            ax.set_title(sub)
            ax.set_ylabel(ylabel)
            ax.grid(True, alpha=0.3)
            ax.legend()
        fig.suptitle(f"MISO {kind.capitalize()} — scenario '{scenario}'",
                     fontsize=14)
        fig.tight_layout()
        out = os.path.join(
            OUTPUT_DIR, f'ltlf_comparison_{kind}_{scenario}.png',
        )
        fig.savefig(out, dpi=150)
        plt.close(fig)
        print(f"  Wrote {out}")


### Main

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print('Reading MISO LTLF workbook (absolute values)...')
    ltlf_energy, ltlf_peak = read_ltlf_absolute()
    print(f"  LTLF energy rows: {len(ltlf_energy)}, "
          f"peak rows: {len(ltlf_peak)}")

    scenarios = list(SCENARIO_MAP.values())
    available = [
        s for s in scenarios
        if os.path.isdir(os.path.join(
            REEDS_RUN_ROOT, RUN_TEMPLATE.format(scenario=s),
        ))
    ]
    if not available:
        raise FileNotFoundError(
            f"No ReEDS runs found under {REEDS_RUN_ROOT} matching "
            f"'{RUN_TEMPLATE.format(scenario='<scenario>')}'"
        )
    print(f"Available scenarios in runs/: {available}")

    # Use the first available scenario's hierarchy as canonical for MISO BA
    # membership. Hierarchies across scenarios should match, since they were
    # copied by the same ReEDS launch step.
    print(f"\nReading hierarchy from '{available[0]}' run...")
    hierarchy = read_run_hierarchy(available[0])
    ba_map = build_ba_subregion_map(hierarchy)
    print("MISO BAs by subregion:")
    for sub in MISO_SUBREGIONS:
        bas = ba_map.loc[ba_map['subregion'] == sub, 'r'].tolist()
        print(f"  {sub}: {len(bas)} BAs -> {bas}")
    print(f"  Total MISO BAs analyzed: {len(ba_map)}")

    for scenario in scenarios:
        reeds, per_ba, state_base, sub_base = compute_scenario_metrics(
            scenario, ba_map, hierarchy, ltlf_energy,
        )
        if reeds is None:
            continue
        merged = merge_with_ltlf(reeds, ltlf_energy, ltlf_peak)

        print(f"\n--- Comparison table: {scenario} ---")
        print_comparison(merged)

        # Diagnostic: DPV as % of MISO annual energy per year
        miso_rows = merged[merged['subregion'] == 'MISO_Total']
        for _, row in miso_rows.iterrows():
            dpv_share = (
                100.0 * row['dpv_energy_TWh_mean']
                / (row['reeds_energy_TWh_mean'] + row['dpv_energy_TWh_mean'])
                if pd.notna(row['dpv_energy_TWh_mean']) else float('nan')
            )
            print(f"  DPV share of MISO gross energy in {int(row['year'])}: "
                  f"{dpv_share:.1f}%")

        # Per-BA breakdown for diagnostic subregions.
        if per_ba is not None and len(per_ba):
            for sub_name in DIAGNOSTIC_SUBREGIONS:
                sub_ba = per_ba[per_ba['subregion'] == sub_name]
                if not len(sub_ba):
                    continue
                print(f"\n--- Per-BA breakdown: {scenario} / {sub_name} ---")
                show = sub_ba[[
                    'year', 'ba',
                    'reeds_energy_TWh_mean',
                    'reeds_peak_MW_mean',
                    'dpv_energy_TWh_mean',
                    'direct_use_energy_TWh_mean',
                ]].copy()
                show = show.sort_values(['year', 'ba']).reset_index(drop=True)
                print(show.round(2).to_string(index=False))

            ba_csv = os.path.join(
                OUTPUT_DIR, f'ltlf_comparison_{scenario}_by_ba.csv',
            )
            per_ba.to_csv(ba_csv, index=False)
            print(f"\n  Wrote {ba_csv}")

        # Table A: state-level base-year comparison (ReEDS vs EIA_loadbystate)
        if state_base is not None and len(state_base):
            years_str = ", ".join(str(y) for y in BASE_YEARS_STATE)
            print(f"\n--- Base year(s) {years_str}: ReEDS vs EIA state "
                  f"loads (MISO-touching states), {scenario} ---")
            with pd.option_context('display.max_rows', None,
                                   'display.max_columns', None,
                                   'display.width', 220):
                print(state_base.round(2).to_string(index=False))
            state_csv = os.path.join(
                OUTPUT_DIR, f'ltlf_baseyear_state_{scenario}.csv',
            )
            state_base.to_csv(state_csv, index=False)
            print(f"  Wrote {state_csv}")

        # Table B: subregion base-year comparison (ReEDS vs LTLF workbook)
        if sub_base is not None and len(sub_base):
            print(f"\n--- Base year {LTLF_BASE_YEAR}: ReEDS "
                  f"(DPV + direct-use adj) vs LTLF MISO subregion, "
                  f"{scenario} ---")
            with pd.option_context('display.max_rows', None,
                                   'display.max_columns', None,
                                   'display.width', 200):
                print(sub_base.round(2).to_string(index=False))
            sub_csv = os.path.join(
                OUTPUT_DIR, f'ltlf_baseyear_subregion_{scenario}.csv',
            )
            sub_base.to_csv(sub_csv, index=False)
            print(f"  Wrote {sub_csv}")

        # Large-diff warning
        big = merged[
            (merged['energy_diff_pct'].abs() > 15)
            | (merged['peak_diff_pct'].abs() > 15)
        ]
        if len(big):
            print(f"\n[warn] {len(big)} (subregion, year) pairs with "
                  f"|diff| > 15%:")
            print(big[['year', 'subregion', 'energy_diff_pct',
                       'peak_diff_pct']].to_string(index=False))

        csv_path = os.path.join(
            OUTPUT_DIR, f'ltlf_comparison_{scenario}.csv',
        )
        merged.to_csv(csv_path, index=False)
        print(f"\n  Wrote {csv_path}")
        make_plots(merged, scenario)

    print("\nDone.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
