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
#      Coincident peak follows the MISO LTLF definition: for each weather
#      year, find the hour at which the full MISO footprint hits its max,
#      then record each subregion's demand at that hour.
#   6. Compare with MISO LTLF workbook (Annual_Energy_TWh and
#      Annual_Coincident_Peak_MW).
#
# Note: Both ReEDS load.h5 and MISO LTLF Net Load represent the bulk-power
# demand the transmission system serves (i.e. include distribution losses),
# so no loss-factor adjustment is applied.
#
# Outputs (per scenario found under REEDS_RUN_ROOT):
#   Outputs/ltlf_comparison_energy_{scenario}.png
#   Outputs/ltlf_comparison_peak_{scenario}.png

import os
import re
import sys

import h5py
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib import ticker

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

sys.path.insert(0, SCRIPT_DIR)
from MISO_LTLF_Load_Projections import (
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

# Direct-use (on-site industrial cogen consumed behind the meter) is not
# grid-served, so MISO LTLF Net Load excludes it. ReEDS load.h5 inherits
# a direct-use share from the historical hourlize base timeseries, which
# then scales with loadmult(y). To compare against LTLF we subtract a
# constant per-state direct-use fraction (measured at the hourlize anchor
# year) from load.h5 in the main year loop.
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


def read_load_all(scenario, miso_bas):
    """Read hourly BA load from load.h5. Returns a DataFrame with MultiIndex
    (model_year, weather_datetime) and columns = miso_bas."""
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
    return df[list(miso_bas)]


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
            api_key,
            create_EIA_url,
            retrieve_EIA_data,
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

def compute_scenario_metrics(scenario, ba_map, hierarchy):
    """Return `subregion_df` (long-form energy/peak metrics per (year,
    subregion)) for this scenario, or `None` if the run folder is absent
    or incomplete.
    """
    run_dir = os.path.join(
        REEDS_RUN_ROOT, RUN_TEMPLATE.format(scenario=scenario),
    )
    if not os.path.isdir(run_dir):
        print(f"[skip] Run folder not found for scenario '{scenario}': "
              f"{run_dir}")
        return None

    # Skip gracefully if the run hasn't finished producing all required
    # inputs_case files yet (e.g. run still in progress).
    required = ['recf.h5', 'load.h5', 'distpvcap.csv', 'hierarchy.csv']
    missing = [
        f for f in required if not os.path.exists(run_path(scenario, f))
    ]
    if missing:
        print(f"[skip] Scenario '{scenario}' run is incomplete "
              f"(missing in inputs_case: {missing}).")
        return None

    print(f"\n=== Scenario: {scenario} ===")

    miso_bas = ba_map['r'].tolist()
    print(f"  {len(miso_bas)} MISO BAs to analyze.")

    print("  Reading distpvcap.csv (linear interpolation to target years)...")
    dpv_cap = read_distpvcap(scenario, COMPARE_YEARS, miso_bas)

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

    load_all = read_load_all(scenario, miso_bas)
    load_datetime_level = load_all.index.get_level_values(1)
    if not isinstance(load_datetime_level, pd.DatetimeIndex):
        try:
            new_idx = pd.MultiIndex.from_arrays([
                load_all.index.get_level_values(0),
                pd.to_datetime(load_datetime_level),
            ], names=load_all.index.names)
            load_all.index = new_idx
        except (ValueError, TypeError) as err:
            raise ValueError(
                f"load.h5 datetime level is not datetime-like: "
                f"{load_datetime_level[:3].tolist()} ({err})"
            ) from err

    # State map (BA -> USPS state); used for the per-BA direct-use
    # multiplier applied to load.h5 below.
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

        # Coincident peak: for each weather year, find the hour at which
        # the full MISO footprint (sum across all MISO BAs) hits its max,
        # then read each subregion's load at that hour. This matches the
        # MISO LTLF definition of Annual_Coincident_Peak_MW:
        #   "demand in a MISO region at the hour of the MISO-wide peak".
        # Use positional (iloc) indexing to avoid any datetime-dtype
        # round-trip issues on the shared index.
        miso_hourly = adjusted[miso_bas].sum(axis=1)   # MW per hour
        miso_vals = miso_hourly.values
        wy_arr = np.asarray(wy)
        unique_wys = np.unique(wy_arr)
        peak_iloc_by_wy = np.empty(len(unique_wys), dtype=np.int64)
        for i, yr in enumerate(unique_wys):
            positions = np.where(wy_arr == yr)[0]
            peak_iloc_by_wy[i] = positions[np.argmax(miso_vals[positions])]

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
            # Subregion's demand at the MISO-wide coincident peak hour of
            # each weather year (not the subregion's own non-coincident max).
            peak_by_wy = pd.Series(
                sub_hourly.values[peak_iloc_by_wy], index=unique_wys,
            )

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

    return pd.DataFrame(rows)


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

    for kind, ylabel, ltlf_col, reeds_col, reeds_min_col, reeds_max_col, scale in [
        ('energy', 'Annual Energy (TWh)',
         'ltlf_energy_TWh', 'reeds_energy_TWh_mean',
         'reeds_energy_TWh_min', 'reeds_energy_TWh_max', 1.0),
        ('peak', 'Coincident Peak (GW)',
         'ltlf_peak_MW', 'reeds_peak_MW_mean',
         'reeds_peak_MW_min', 'reeds_peak_MW_max', 1e-3),
    ]:
        fig, axes = plt.subplots(2, 2, figsize=(9, 4), sharex=True)
        axes = axes.flatten()
        legend_handles, legend_labels = None, None
        for ax, sub in zip(axes, SUBREGION_ORDER):
            d = df[df['subregion'] == sub].sort_values('year')
            if d.empty:
                ax.set_title(f"{sub} (no data)")
                continue
            ax.plot(d['year'], d[ltlf_col] * scale, marker='o', 
                    label=f'MISO LTLF {scenario}', c='#db9728')
            ax.plot(d['year'], d[reeds_col] * scale, marker='s',
                    label=f'ReEDS (adj) {scenario} mean', c='#0079C2')
            ax.fill_between(
                d['year'],
                d[reeds_min_col] * scale,
                d[reeds_max_col] * scale,
                color='#0079C2', alpha=0.2, linewidth=0,
                label='ReEDS weather-year range',
            )
            ax.set_title(sub)
            ax.set_ylabel(ylabel)
            ax.grid(True, alpha=0.3)
            ax.xaxis.set_major_locator(ticker.MultipleLocator(5))
            if legend_handles is None:
                legend_handles, legend_labels = ax.get_legend_handles_labels()
        fig.tight_layout()
        if legend_handles:
            fig.legend(
                legend_handles, legend_labels,
                loc='lower center', bbox_to_anchor=(0.5, 0),
                ncol=len(legend_handles), frameon=False,
            )
            fig.subplots_adjust(bottom=0.18)
        out = os.path.join(
            OUTPUT_DIR, f'ltlf_comparison_{kind}_{scenario}.png',
        )
        fig.savefig(out, dpi=300, bbox_inches='tight')
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
        reeds = compute_scenario_metrics(scenario, ba_map, hierarchy)
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
