"""
This script processes and produces ATB costs input files for ReEDS.
Workflow choices are defined in ../config.yaml; technology-specific formatting
rules are defined in settings.yaml. This formatter reads local raw inputs only.
The script runs in the standard ReEDS environment (reeds2).
"""

#%% ===========================================================================
### --- IMPORTS ---
### ===========================================================================
import argparse
import os
import numpy as np
import pandas as pd
import shutil
from pandas.errors import EmptyDataError
import sys

from atb_config import ATBE_COLUMN_MAPPING, load_processing_settings

# directory containing this script (atb/scripts); settings.yaml lives here.
THISDIR = os.path.dirname(os.path.abspath(__file__))
# parent atb/ directory; used to build paths to input/, manual_input/, output/.
ATBDIR = os.path.dirname(THISDIR)

# define zero marginal cost technologies (to assign zero 'vom' if input data is missing)
zero_vom_techs = {'upv', 'wind-ons', 'wind-ofs', 'battery'}

# write LF line endings on every platform so repeated runs on Windows do not
# rewrite every versioned CSV with different line endings than the stored ones
CSV_LINE_TERMINATOR = "\n"

#%% ===========================================================================
### --- FUNCTIONS ---
### ===========================================================================

def subset_atb_rows(subset_rows, atb_data_in):
    """
    Subset the ATB flat file data to desired Case | CRPYears rows

    Parameters
    ----------
    subset_rows: dict
        Default {Case: "Exp + TC", CRPYears: 30} for most techs; some techs can have other values for Case, e.g., Market, R&D etc.)
    atb_data_in: pd.DataFrame
        ATB flat file dataframe to subset
    """
    atb_data = atb_data_in.copy()
    atb_col_vals = {}
    # subset to relevant cases (typically Market case and 30-year CRP, but specified in settings.yaml) 
    for sr in subset_rows:
        print(f"Subsetting to {sr}={subset_rows[sr]}")
        if sr not in atb_data.columns:
            raise ValueError(f"Column {sr} not in ATB file.")
        else:
            atb_col_vals[sr] = atb_data_in[sr].unique().tolist()
            atb_data = atb_data.loc[atb_data[sr] == subset_rows[sr]]
    return atb_data, atb_col_vals

def load_atb_flat_file(settings, args, techs_to_run):
    """
    load ATB flat file with all inputs for specified techs

    Parameters
    ----------
    settings: dict
        Parameter settings from settings.yaml
    args: argparse.Namespace
        Used for debug mode
    techs_to_run: list[str]
        Ordered list of tech keys (matching keys in settings['techs'])
    """
    filepath = settings['flat_file_path']
    if not os.path.isfile(filepath):
        raise FileNotFoundError(
            f"Raw ATB flat file not found: {filepath}\n"
            "Run 'python scrape_atb_inputs.py' before formatting ReEDS inputs."
        )
    print(f"Loading raw ATB flat file: {filepath}")
    atb_data_in = pd.read_csv(filepath, low_memory=False)
    # Apply the same mapping for downloaded ATBe files and canonical flat files.
    atb_data_in = atb_data_in.rename(columns=ATBE_COLUMN_MAPPING)

    if args.debug:
        breakpoint()
    
    # store validated/subsetted data for all requested techs here
    validated_data_list = []

    # iterate over requested techs to validate and subset immediately
    for tech in techs_to_run:
        tech_settings = settings['techs'][tech]
        
        # 1. Get tech-specific subset settings
        if 'subset_rows' not in tech_settings:
            raise ValueError(f"Missing 'subset_rows' for {tech} in settings.yaml")
        
        # 2. Filter by Technology
        tech_rows_full = atb_data_in[atb_data_in['Technology'] == tech_settings['Technology']]
        
        if tech_rows_full.empty:
            print(f"Warning: No data found for Technology '{tech_settings['Technology']}' ({tech}).")
            continue

        # 3. Apply specific subset_rows (Case, CRPYears, etc.)
        # subset_atb_rows returns (subset_df, col_vals)
        tech_subset, _ = subset_atb_rows(tech_settings['subset_rows'], tech_rows_full)

        # 4. Filter for specific DisplayName(s) if applicable
        if 'DisplayName' in tech_settings:
            atb_techs = tech_settings['DisplayName']
            atb_techs_list = list(atb_techs.keys()) if isinstance(atb_techs, dict) else [atb_techs]

            # append all subtechs present
            found_data = tech_subset[tech_subset['DisplayName'].isin(atb_techs_list)]
            missing_subtechs = [t for t in atb_techs_list if t not in found_data['DisplayName'].values]

            if found_data is not None and not found_data.empty:
                validated_data_list.append(found_data)
            
            # fallback: handle any subtechs that were missing from the subset
            for atb_tech_name in missing_subtechs:
                # Fallback logic: check if it exists in the wider tech data (e.g., for a different Case)
                temp_df = tech_rows_full[tech_rows_full['DisplayName'] == atb_tech_name]
                
                if temp_df.empty:
                    print(f"Warning: DisplayName '{atb_tech_name}' not found in source data for {tech}.")
                    continue
                print(f"Could not subset {atb_tech_name} data to {tech_settings['subset_rows']}.")
                print(f"Available Case options: {temp_df.Case.unique()}")
                
                new_case = str(input("Select an alternative Case to proceed, or (q) to quit: "))
                
                if new_case.lower() == "q":
                    sys.exit("Exiting now.")
                elif new_case not in temp_df.Case.unique():
                    print(f"'{new_case}' not found in available Case options. Exiting.")
                    sys.exit(1)
                else:
                    # Grab the data for the alternative case
                    new_subset_rows = tech_settings['subset_rows'].copy()
                    new_subset_rows['Case'] = new_case
                    
                    # We only need this specific subtech with the new case
                    subtech_data, _ = subset_atb_rows(new_subset_rows, temp_df)
                    validated_data_list.append(subtech_data)
        else:
            # If no DisplayName filtering is needed, just keep the successful subset
            if not tech_subset.empty:
                validated_data_list.append(tech_subset)

    # Combine all valid chunks
    if not validated_data_list:
        raise EmptyDataError(f"\n\nNo valid data found for any of the requested technologies: {techs_to_run}")

    atb_data_final = pd.concat(validated_data_list, ignore_index=True)
    return atb_data_final

def get_atb_file_path(tech, atbyear, scenario, settings):
    """
    function to construct full filepath and per-tech ATB filename

    Parameters
    ----------
    tech: str
        Tech identifier used as the file prefix (e.g., 'gas', 'upv' etc.)
    atbyear: int
        ATB year (e.g., 2025) to process
    scenario: str
        ATB scenario (Moderate/Advanced) from settings.yaml
    settings: dict
        Loaded settings from settings.yaml
    """
    # allow per-tech override of the filename in existing ReEDS input files
    # e.g. settings['techs']['wind-ons']['reeds_name'] = "ons-wind"
    filename_root = settings['techs'].get(tech, {}).get('reeds_name', tech)
    filename = f"{filename_root}_ATB_{atbyear}_{scenario.lower()}"
    filepath = os.path.join(
        settings['reedspath'], 'inputs', 'plant_characteristics', filename + ".csv"
    )
    return filename, filepath

def check_columns(df, col_list, setting, tech):
    """
    function to check that all columns specified in settings.yml exist in the data

    Parameters
    ----------
    col_list: list[str]
        Sequence of required column names based on tech settings
    setting: str
        Settings key being validated (used in the error message, e.g. 'indexcols' or 'cols')
    tech: str
        Technology identifier
    """
    missing_cols = [c for c in col_list if c not in df.columns]
    if len(missing_cols) > 0:
        raise ValueError(
            f"The following columns are specified as '{setting}' but are not in the data: {missing_cols}. "
            f"Please update your 'settings.yaml' for {tech}."
            )

def _history_file_path(tech, scenario, settings):
    """Return the stable, year-independent history path for one scenario."""
    return os.path.join(
        settings['history_dir'],
        f"{tech}_ATB_historical_{str(scenario).lower()}.csv",
    )


def _cost_columns(df):
    """Return every monetary output column, including battery energy costs."""
    return [
        col for col in df.columns
        if col == 'vom' or col.startswith('capcost') or col.startswith('fom')
    ]


def _select_pre_projection_history(history, current, idcols):
    """Keep history before each current series starts and all retired series."""
    if not idcols:
        return history.loc[history['t'] < current['t'].min()].copy()

    starts = (
        current.groupby(idcols, dropna=False, as_index=False)['t']
        .min()
        .rename(columns={'t': '_projection_start'})
    )
    selected = history.merge(starts, on=idcols, how='left')
    selected = selected.loc[
        selected['_projection_start'].isna()
        | (selected['t'] < selected['_projection_start'])
    ]
    return selected.drop(columns='_projection_start')


def _projection_boundary_rows(current, idcols):
    """Select the first current-ATB year for every active technology series."""
    if not idcols:
        return current.loc[current['t'] == current['t'].min()].copy()
    starts = current.groupby(idcols, dropna=False)['t'].transform('min')
    return current.loc[current['t'] == starts].copy()


def _validate_year_continuity(frame, tech, settings):
    """Fail if any output series has a missing year or starts after ReEDS."""
    groupcols = [
        col for col in settings['techs'][tech]['indexcols'] if col != 't'
    ]
    failures = []
    grouper = groupcols[0] if len(groupcols) == 1 else groupcols
    for group_values, group in frame.groupby(grouper, dropna=False, sort=False):
        years = sorted(pd.to_numeric(group['t'], errors='raise').astype(int).unique())
        expected = set(range(settings['reeds_start_year'], max(years) + 1))
        missing = sorted(expected - set(years))
        if years[0] != settings['reeds_start_year'] or missing:
            values = group_values if isinstance(group_values, tuple) else (group_values,)
            label = dict(zip(groupcols, values))
            failures.append(
                f"{label}: starts {years[0]}, missing {missing[:10]}"
            )
    if failures:
        raise ValueError(
            f"Historical continuity check failed for {tech}: "
            + "; ".join(failures[:10])
        )


def _normalize_reeds_history(frame, tech, settings):
    """Normalize a ReEDS file to the current technology's output schema."""
    tech_settings = settings['techs'][tech]
    frame = frame.rename(columns=tech_settings.get('renamecols', {})).copy()
    required = tech_settings['cols']
    missing = [col for col in required if col not in frame.columns]
    if missing:
        raise KeyError(
            f"Historical ReEDS file for {tech} is missing columns {missing}. "
            f"Available columns: {list(frame.columns)}"
        )
    frame = frame[required]
    frame['t'] = pd.to_numeric(frame['t'], errors='raise').astype(int)
    return frame


def _seed_history_from_reeds(
    tech, scenario, current, history_path, settings, dollaryear, deflator
):
    """Create a local historical baseline from the current ReEDS ATB file."""
    reeds_key, reeds_path = get_atb_file_path(
        tech, settings['atbyear'], scenario, settings
    )
    if not os.path.isfile(reeds_path):
        moderate_key, moderate_path = get_atb_file_path(
            tech, settings['atbyear'], 'moderate', settings
        )
        if os.path.isfile(moderate_path):
            print(
                f"...no {str(scenario).lower()} ReEDS history for {tech}; "
                "using moderate history"
            )
            reeds_key, reeds_path = moderate_key, moderate_path
        else:
            raise FileNotFoundError(
                f"Historical baseline is missing: {history_path}\n"
                f"Could not seed it because neither the matching nor moderate "
                f"ReEDS file exists: {reeds_path}"
            )

    history = _normalize_reeds_history(pd.read_csv(reeds_path), tech, settings)
    idcols = [
        col for col in settings['techs'][tech]['indexcols']
        if col not in ['Scenario', 't']
    ]
    history = _select_pre_projection_history(history, current, idcols)

    source_dollar_year = int(dollaryear[reeds_key])
    history_dollar_year = settings['history_dollar_year']
    adjustment = (
        deflator[source_dollar_year] / deflator[history_dollar_year]
    )
    costcols = _cost_columns(history)
    history[costcols] = history[costcols] * adjustment
    history = history.sort_values(settings['techs'][tech]['cols']).reset_index(drop=True)
    os.makedirs(settings['history_dir'], exist_ok=True)
    history.to_csv(history_path, index=False, lineterminator=CSV_LINE_TERMINATOR)
    print(
        f"...seeded {os.path.basename(history_path)} from "
        f"{os.path.basename(reeds_path)}"
    )
    return history


def _observed_history_source_path(settings):
    """Return the configured normalized observed-history CSV path."""
    source_settings = settings['config'].get('historical_cost_sources', {})
    return os.path.join(
        ATBDIR,
        source_settings['directory'],
        source_settings['normalized_filename'],
    )


def _observed_values_by_year(tech, mapping, settings, deflator):
    """Read, filter, and deflate one observed series into {year: value}."""
    source_path = _observed_history_source_path(settings)
    if not os.path.isfile(source_path):
        raise FileNotFoundError(
            f"Observed historical-cost file is missing: {source_path}. "
            "Run scripts/scrape_historical_costs.py first."
        )
    observed = pd.read_csv(source_path)
    for column, value in mapping.get('filters', {}).items():
        if column not in observed.columns:
            raise KeyError(
                f"Observed historical-cost file is missing filter column {column}."
            )
        observed = observed.loc[observed[column] == value]
    if observed.empty:
        raise KeyError(
            f"No observed historical rows match the configured mapping for {tech}."
        )
    if observed['year'].duplicated().any():
        duplicate_years = sorted(
            observed.loc[observed['year'].duplicated(False), 'year'].unique()
        )
        raise ValueError(
            f"Observed historical mapping for {tech} has duplicate years: "
            f"{duplicate_years}"
        )
    if observed['dollar_year'].isna().any():
        raise ValueError(
            f"Observed historical mapping for {tech} requires a dollar_year."
        )

    current_dollar_year = int(settings['dollaryear'])
    converted_values = {}
    for row in observed.itertuples(index=False):
        source_dollar_year = int(row.dollar_year)
        if source_dollar_year not in deflator.index:
            raise KeyError(
                f"Deflator table has no value for observed dollar year "
                f"{source_dollar_year}."
            )
        converted_values[int(row.year)] = (
            float(row.value)
            * float(deflator[source_dollar_year])
            / float(deflator[current_dollar_year])
        )
    return converted_values


def _complete_observed_years(
    values, required_years, tech, label=None, report=True
):
    """Fill a real-history series without falling back to manual history.

    Missing years between observations are linearly interpolated. Years before
    the first or after the last observation use the nearest observed endpoint,
    because linear interpolation requires an observation on each side.
    """
    if not values or not required_years:
        return values
    observed_years = np.asarray(sorted(values), dtype=float)
    if len(observed_years) < 2 and not required_years.issubset(values):
        raise ValueError(
            f"Observed historical mapping for {label or tech} needs at least "
            "two observations to fill missing years."
        )
    observed_values = np.asarray(
        [values[int(year)] for year in observed_years], dtype=float
    )
    missing_years = sorted(required_years - set(values))
    if not missing_years:
        return values

    completed = dict(values)
    completed.update({
        year: float(np.interp(year, observed_years, observed_values))
        for year in missing_years
    })
    if report:
        internal = [
            year for year in missing_years
            if observed_years[0] < year < observed_years[-1]
        ]
        endpoints = sorted(set(missing_years) - set(internal))
        if internal:
            print(
                f"  {label or tech}: linearly interpolated missing real-history "
                f"years {internal}"
            )
        if endpoints:
            print(
                f"  {label or tech}: filled endpoint real-history years "
                f"{endpoints} from the nearest observation"
            )
    return completed


def _assign_observed_values(frame, mask, output_column, values):
    """Write the observed series into the masked rows and report the span."""
    result = frame.copy()
    result.loc[mask, output_column] = result.loc[mask, 't'].map(values)
    return result, sorted(result.loc[mask, 't'].astype(int).unique())


def _year_key(frame):
    """Columns identifying one value slot per year.

    Frames stack every ATB scenario, and historical years are identical across
    them, so repetition across `Scenario` is expected and anything else is not.
    """
    return [column for column in ('Scenario', 't') if column in frame.columns]


def _assert_one_row_per_year(frame, mask, tech, applier_name):
    """Fail loudly when one observed value would address several rows."""
    candidates = frame.loc[mask]
    duplicated = candidates.duplicated(subset=_year_key(frame), keep=False)
    if duplicated.any():
        crowded_years = sorted(
            candidates.loc[duplicated, 't'].astype(int).unique()
        )
        raise ValueError(
            f"{tech} uses the {applier_name} observed-history applier, but "
            f"more than one row shares a scenario and year in {crowded_years}. "
            f"One observed value cannot address them all. Define a "
            f"technology-specific applier for {tech} and register it in "
            f"REAL_HISTORY_APPLIERS."
        )


def _only_series(resolved, tech, applier_name):
    """Unpack a mapping that must resolve to exactly one observed series."""
    if len(resolved) != 1:
        raise ValueError(
            f"{tech} uses the {applier_name} observed-history applier, which "
            f"takes one series, but its mapping resolved {len(resolved)}. "
            f"Use an applier that assigns each series to its own rows."
        )
    return resolved[0]


def _series_target(series, tech):
    """Return the one sub-technology a mapping series describes.

    A series measures one thing, so it names one sub-technology. Sharing a
    series across several made unrelated rows carry identical history and hid
    the fact that no observation existed for the others.
    """
    targets = series.get('turbine_classes') or series.get('technologies') or []
    if len(targets) != 1:
        raise ValueError(
            f"{tech} observed-history mapping series names {targets or 'nothing'}. "
            "Each series describes exactly one sub-technology; give the others "
            "their own series or their own mode in historical_data."
        )
    return targets[0]


def _assign_series_to_target(
    frame, tech, mapping, values, historical_mask, key_column, target
):
    """Assign one observed series to the rows of one sub-technology."""
    if key_column not in frame.columns:
        raise KeyError(
            f"{tech} observed-history applier expects a {key_column!r} column "
            "identifying the sub-technology."
        )
    present = set(frame[key_column].unique())
    if target not in present:
        raise ValueError(
            f"{tech} observed-history mapping targets {target!r}, which is "
            f"absent from the frame. Present: {sorted(present)}."
        )
    mask = (
        historical_mask
        & frame['t'].isin(values)
        & (frame[key_column] == target)
    )
    _assert_one_row_per_year(frame, mask, tech, key_column)
    return _assign_observed_values(
        frame, mask, mapping['output_column'], values
    )


def apply_real_history_single_series(
    frame, tech, mapping, resolved, historical_mask, historical_data, class_column
):
    """Applier for technologies carrying one row per scenario-year.

    upv and wind-ons each select a single ATB DisplayName. The guard catches a
    technology that splits a year into several series being routed here.
    """
    _series, values = _only_series(resolved, tech, 'single-series')
    mask = historical_mask & frame['t'].isin(values)
    _assert_one_row_per_year(frame, mask, tech, 'single-series')
    return _assign_observed_values(frame, mask, mapping['output_column'], values)


def apply_real_history_by_class(
    frame, tech, mapping, resolved, historical_mask, historical_data, class_column
):
    """Applier for technologies carrying one row per sub-technology.

    Offshore wind splits by turbine class and gas by plant configuration; both
    assign each series to the one sub-technology it measures. A sub-technology
    no series describes must select its own mode in historical_data, so none
    falls back to manual values by accident.
    """
    if not class_column:
        raise KeyError(
            f"{tech} uses the per-class observed-history applier but declares "
            "no history_class_column in settings.yaml."
        )
    present = (
        set(frame[class_column].unique()) if class_column in frame.columns else set()
    )
    result = frame
    replaced = set()
    targeted = set()
    for series, values in resolved:
        target = _series_target(series, tech)
        result, years = _assign_series_to_target(
            result, tech, mapping, values, historical_mask, class_column, target
        )
        replaced.update(years)
        targeted.add(target)
    metric = mapping['output_column']
    configured = set(_metric_modes(historical_data, metric)) - {None}
    untouched = sorted(present - targeted - configured)
    if untouched:
        raise KeyError(
            f"{tech}.{metric} selects real history, but {untouched} have no "
            "observed series. Give each one its own mode in historical_data."
        )
    return result, sorted(replaced)


# Each applier owns which rows its series may address, since that depends on
# the technology's row shape. Every metric selecting `real` requires an entry.
# Each receives the metric's historical_data modes so it can tell a row left
# unclaimed by accident from one that explicitly generates its own history.
REAL_HISTORY_APPLIERS = {
    'biopower': apply_real_history_single_series,
    'upv': apply_real_history_single_series,
    'wind-ons': apply_real_history_single_series,
    'wind-ofs': apply_real_history_by_class,
    'gas': apply_real_history_by_class,
}


def _resolve_observed_series(
    tech,
    mapping,
    settings,
    deflator,
    required_years,
    report_fill=True,
):
    """Resolve a mapping into [(series_config, {year: value}), ...].

    A mapping describes one series directly or several under `series`. Entries
    inherit the mapping's other keys, so shared settings are written once.
    """
    entries = mapping.get('series') or [mapping]
    inherited = {key: value for key, value in mapping.items() if key != 'series'}
    resolved = []
    for entry in entries:
        merged = {**inherited, **entry}
        values = _observed_values_by_year(tech, merged, settings, deflator)
        targets = merged.get('technologies') or merged.get('turbine_classes')
        label = f"{tech} {', '.join(targets)}" if targets else tech
        values = _complete_observed_years(
            values,
            required_years,
            tech,
            label,
            report=report_fill,
        )
        resolved.append((merged, values))
    return resolved


def _apply_real_historical_costs(frame, tech, settings, deflator):
    """Replace mapped pre-projection metrics with normalized observed values."""
    technology_config = _technology_smoothing_config(tech, settings)
    if technology_config is None:
        return frame
    real_metrics = _real_historical_metrics(technology_config)
    if not real_metrics:
        return frame
    smoothing = settings['config']['processing']['smooth_cost_curves']
    source_settings = settings['config'].get('historical_cost_sources', {})
    technology_mappings = source_settings.get('reeds_mappings', {}).get(tech, {})
    applier = REAL_HISTORY_APPLIERS.get(tech)
    if applier is None:
        raise NameError(
            f"{tech} selects a real historical metric but has no observed-history "
            "applier. Define one and add it to REAL_HISTORY_APPLIERS."
        )
    projection_start_year = int(smoothing.get('projection_start_year', 2022))
    before_projection = frame['t'] < projection_start_year
    required_years = set(frame.loc[before_projection, 't'].astype(int).unique())
    historical_mask = before_projection
    if technology_config['fill_historical']:
        historical_mask = frame['t'] <= int(settings['atbyear'])
    result = frame
    for metric in real_metrics:
        mapping = technology_mappings.get(metric)
        if mapping is None:
            raise ValueError(
                f"{tech}.{metric} selects historical_data: real but has no "
                "reviewed mapping under historical_cost_sources.reeds_mappings."
            )
        if metric not in result.columns:
            raise KeyError(
                f"Observed-history metric {metric!r} is absent for {tech}."
            )
        metric_mapping = {**mapping, 'output_column': metric}
        resolved = _resolve_observed_series(
            tech,
            metric_mapping,
            settings,
            deflator,
            required_years,
        )
        result, replaced_years = applier(
            result,
            tech,
            metric_mapping,
            resolved,
            historical_mask,
            technology_config['historical_data'],
            settings['techs'][tech].get('history_class_column'),
        )
        if replaced_years:
            current_dollar_year = int(settings['dollaryear'])
            print(
                f"Applied real historical {metric} for {tech}: "
                f"{replaced_years[0]}-{replaced_years[-1]} "
                f"({current_dollar_year}$)"
            )
    return result


def validate_real_historical_data(settings, techs, deflator):
    """Fail early when a selected ``real`` technology lacks observed data.

    The normal per-technology checks still run while values are applied. This
    preflight check gathers configuration and source-data problems before any
    technology output is processed, and gives the user one recovery command.
    """
    smoothing = settings['config']['processing'].get('smooth_cost_curves', {})
    if not smoothing.get('enabled', False):
        return
    real_metrics = []
    for tech in techs:
        technology_config = _technology_smoothing_config(tech, settings)
        if technology_config is not None:
            real_metrics.extend(
                (tech, metric)
                for metric in _real_historical_metrics(technology_config)
            )
    if not real_metrics:
        return
    real_labels = [f"{tech}.{metric}" for tech, metric in real_metrics]

    source_settings = settings['config'].get('historical_cost_sources', {})
    missing_source_settings = [
        key for key in ('directory', 'normalized_filename')
        if not source_settings.get(key)
    ]
    if missing_source_settings:
        raise ValueError(
            "Historical data validation failed for technologies configured "
            f"with historical_data: real: {real_labels}. Missing settings under "
            f"historical_cost_sources: {missing_source_settings}."
        )

    source_path = _observed_history_source_path(settings)
    if not os.path.isfile(source_path):
        raise FileNotFoundError(
            "Historical data validation failed for technologies configured "
            f"with historical_data: real: {real_labels}.\n"
            f"The normalized historical-cost file does not exist: {source_path}\n"
            "Run 'python scripts/scrape_historical_costs.py' from the atb "
            "directory, then run the formatter again."
        )

    mappings = source_settings.get('reeds_mappings', {})
    if not isinstance(mappings, dict):
        raise TypeError("historical_cost_sources.reeds_mappings must be a mapping.")
    projection_start_year = int(smoothing.get('projection_start_year', 2022))
    required_years = set(range(
        int(settings['reeds_start_year']), projection_start_year
    ))
    missing_choices = []
    failures = []
    for tech, metric in real_metrics:
        mapping = mappings.get(tech, {}).get(metric)
        if mapping is None:
            missing_choices.append(
                f"{tech}.{metric}: no mapping under "
                "historical_cost_sources.reeds_mappings"
            )
            continue
        if REAL_HISTORY_APPLIERS.get(tech) is None:
            missing_choices.append(
                f"{tech}.{metric}: no observed-history applier in "
                "REAL_HISTORY_APPLIERS"
            )
            continue
        if metric not in settings['techs'][tech]['cols']:
            missing_choices.append(
                f"{tech}.{metric}: metric is not in its ReEDS schema"
            )
            continue
        try:
            _resolve_observed_series(
                tech,
                {**mapping, 'output_column': metric},
                settings,
                deflator,
                required_years,
                report_fill=False,
            )
        except KeyError as error:
            missing_choices.append(f"{tech}.{metric}: {error}")
        except (FileNotFoundError, TypeError, ValueError) as error:
            failures.append(f"{tech}.{metric}: {error}")

    if missing_choices:
        details = "\n".join(f"  - {failure}" for failure in missing_choices)
        raise KeyError(
            "A selected real historical metric is unavailable:\n"
            f"{details}\n"
            "Choose an available option in config.yaml or add and review the "
            "required source mapping."
        )
    if failures:
        details = "\n".join(f"  - {failure}" for failure in failures)
        raise ValueError(
            "Historical data validation failed for one or more technologies "
            "configured with historical_data: real:\n"
            f"{details}\n"
            "The combined historical CSV may be missing a source. Run "
            "'python scripts/scrape_historical_costs.py --no-download' from "
            "the atb directory to rebuild it from all local source files, or "
            "review the mappings in config.yaml."
        )

    print(
        "Validated observed historical data for metrics configured as real: "
        f"{real_labels}"
    )


def merge_historical_atb_data(
    tech_data, tech, settings, dollaryear, deflator
):
    """Combine versioned local history with current scraped ATB projections.

    The local history is stored in a fixed dollar year. For each active series,
    only rows before the first current scraped year are used. Series no longer
    published by ATB are retained from history. The first current projection
    year is appended to the stored history for the next annual update.
    """
    tech_settings = settings['techs'][tech]
    idcols = [
        col for col in tech_settings['indexcols']
        if col not in ['Scenario', 't']
    ]
    combined = []
    current_dollar_year = settings['dollaryear']
    history_dollar_year = settings['history_dollar_year']

    for scenario in tech_data['Scenario'].unique():
        current = tech_data.loc[tech_data['Scenario'] == scenario].copy()
        history_path = _history_file_path(tech, scenario, settings)
        if os.path.isfile(history_path):
            history_stored = _normalize_reeds_history(
                pd.read_csv(history_path), tech, settings
            )
        elif settings['seed_missing_history']:
            history_stored = _seed_history_from_reeds(
                tech, scenario, current, history_path, settings, dollaryear, deflator
            )
        else:
            raise FileNotFoundError(
                f"Historical baseline is missing: {history_path}. "
                "Enable historical_data.seed_missing_from_reeds or add the file."
            )

        history_for_output = _select_pre_projection_history(
            history_stored, current, idcols
        )
        costcols = _cost_columns(history_for_output)
        adjustment = (
            deflator[history_dollar_year] / deflator[current_dollar_year]
        )
        history_for_output[costcols] = history_for_output[costcols] * adjustment
        history_for_output['Scenario'] = scenario
        combined.extend([history_for_output, current])

        boundary = _projection_boundary_rows(current, idcols)[tech_settings['cols']]
        boundary = boundary.copy().round(
            tech_settings.get('decimals', settings['decimals'])
        )
        boundary_costcols = _cost_columns(boundary)
        boundary[boundary_costcols] = boundary[boundary_costcols] / adjustment
        updated_history = pd.concat(
            [history_stored, boundary], ignore_index=True
        ).drop_duplicates(subset=[*idcols, 't'], keep='last')
        updated_history = updated_history.sort_values(
            tech_settings['cols']
        ).reset_index(drop=True)
        updated_history.to_csv(
            history_path, index=False, lineterminator=CSV_LINE_TERMINATOR
        )

    output = pd.concat(combined, ignore_index=True)
    output = output.drop_duplicates(
        subset=tech_settings['indexcols'], keep='last'
    )
    output = output.reset_index(drop=True)
    output = _apply_real_historical_costs(output, tech, settings, deflator)
    _validate_year_continuity(output, tech, settings)
    return output

def normalize_cf(tech, settings, df):
    """
    function to normalize capacity factor to relevant base year and scenario (specified by 'cfbase')

    Parameters
    ----------
    tech: str
        Technology key in settings['techs'] for which cf_improvement values will be normalized
    settings: dict
        Parsed settings.yaml values. Expects settings['techs'][tech]['cfbase'] to define the base
        selection. Example cfbase entries:
            - keys with exact match values to select the base row(s)
            - optional 'keepcols' list to indicate merge keys when multiple columns define the base
            - 't' may be used to indicate the base year
    """
    print("Normalizing cf_improvement")
    tech_settings = settings['techs'][tech]
    cf_base = df.copy()
    for k,v in tech_settings['cfbase'].items():
        if k in df.columns:
            print(f"...base {k}={v}")
            cf_base = cf_base.loc[(cf_base[k] == v)]
    if 'keepcols' in tech_settings['cfbase']:
        cf_base = cf_base[tech_settings['cfbase']['keepcols'] + ['cf_improvement']].rename(columns={"cf_improvement":"cf_base"})
        df = df.merge(cf_base, on=tech_settings['cfbase']['keepcols'], how='left')
        df['cf_improvement'] = df['cf_improvement'] / df['cf_base']
        df = df.drop(columns='cf_base')
    else:
        if len(cf_base) > 1:
            raise ValueError("Error: more than one value found for cf_improvement without specifying which columns to keep.")
        else:
            df['cf_improvement'] = df['cf_improvement'] / cf_base['cf_improvement'].values[0]

    # check that base level is properly normalized to 1
    assert (df.loc[cf_base.index, 'cf_improvement'] == 1).all(), "Check cf_improvement normalization."

    return df

def apply_offshore_cost_multipliers(tech, settings, df):
    """Apply ReEDS offshore configuration adjustments to ATB class proxies."""
    multiplier_path = os.path.join(
        ATBDIR,
        "manual_input",
        f"offshore_cost_multipliers_{settings['atbyear']}.csv",
    )
    multipliers = pd.read_csv(multiplier_path).set_index("turbine")
    missing = set(df["turbine"].unique()) - set(multipliers.index)
    if missing:
        raise ValueError(
            f"Missing offshore cost multiplier(s) for turbine(s): {sorted(missing)}"
        )
    for metric in ("capcost", "fom"):
        df[metric] *= df["turbine"].map(multipliers[metric])
    return df


def apply_coal_projection_overrides(tech, settings, df):
    """Preserve the published ReEDS 2024 coal values changed by ATB v3."""
    override_path = os.path.join(
        ATBDIR,
        "manual_input",
        f"coal_projection_overrides_{settings['atbyear']}.csv",
    )
    overrides = pd.read_csv(override_path)
    overrides = overrides.loc[overrides["tech"] == tech].drop(columns="tech")
    keys = ["Scenario", "i", "t"]
    values = [column for column in overrides if column not in keys]
    indexed = df.set_index(keys)
    override_indexed = overrides.set_index(keys)
    missing = override_indexed.index.difference(indexed.index)
    if not missing.empty:
        raise ValueError(f"Coal override rows not found in ATB data: {missing.tolist()}")
    indexed.update(override_indexed[values])
    return indexed.reset_index()

def smooth_hist_cf(tech, settings, df):
    """
    function to interpolate historical capacity factor values through 2035

    Parameters
    ----------
    tech: str
        Technology key in settings['techs'] to use when looking up cfbase/base year
    settings: dict
        Expects settings['techs'][tech]['cfbase']['t'] to specify the base year for interpolation
    df: pd.DataFrame
        Input dataframe containing ['Scenario', 't', 'cf_improvement'] columns
    """
    # sort by scenario and year
    df = df.sort_values(by=['Scenario', 't'])
    # remove values between 2022 (ATB starts in 2023) and atb base year (usually 2035)
    tech_settings = settings['techs'][tech]
    baseyear = tech_settings['cfbase']['t']
    df['cf_improvement'] = np.where((df['t'] > 2022) & (df['t'] < baseyear), np.nan, df['cf_improvement'])
    # interpolate to fill dropped values
    df['cf_improvement'] = df.groupby('Scenario')['cf_improvement'].transform(lambda x: x.interpolate(method='linear'))

    return df


# Divide-by-zero floor when expressing a change relative to its own value.
RELATIVE_SCALE_FLOOR = 1e-9


def _relative_change(left, right, epsilon):
    """Return a scale-independent change between two finite values."""
    return abs(right - left) / max(abs(left), abs(right), epsilon)


def _smooth_curve_segment(
    years,
    values,
    slope_change_threshold,
    max_kink_years,
    major_step_threshold,
):
    """Smooth compact kink clusters in one time segment.

    Runs of equal values are left alone. A flat stretch is how ATB states that
    a cost stops improving, so bridging one would replace a published
    trajectory with a slope ATB never gave.
    """
    years = np.asarray(years, dtype=float)
    smoothed = np.asarray(values, dtype=float).copy()
    if len(smoothed) < 3:
        return smoothed

    year_deltas = np.diff(years)
    if np.any(year_deltas <= 0):
        raise ValueError("Cost smoothing requires unique, increasing years.")

    year_span = years[-1] - years[0]
    slopes = np.diff(smoothed) / year_deltas
    baseline_slope = abs((smoothed[-1] - smoothed[0]) / year_span)
    epsilon = max(np.max(np.abs(smoothed)) * 1e-12, 1e-12)
    candidate_knots = []
    for knot in range(1, len(smoothed) - 1):
        left_slope = slopes[knot - 1]
        right_slope = slopes[knot]
        scale = max(abs(left_slope), abs(right_slope), baseline_slope, epsilon)
        if abs(right_slope - left_slope) / scale >= slope_change_threshold:
            candidate_knots.append(knot)

    clusters = []
    for knot in candidate_knots:
        if clusters and knot == clusters[-1][-1] + 1:
            clusters[-1].append(knot)
        else:
            clusters.append([knot])
    for cluster in clusters:
        first, last = cluster[0], cluster[-1]
        if len(cluster) < 2 or years[last] - years[first] > max_kink_years:
            continue
        left = max(0, first - 1)
        right = min(len(smoothed) - 1, last + 2)
        step_changes = [
            _relative_change(smoothed[position], smoothed[position + 1], epsilon)
            for position in range(left, right)
        ]
        if any(change >= major_step_threshold for change in step_changes):
            continue
        interpolation_index = np.arange(left, right + 1)
        smoothed[interpolation_index] = np.interp(
            years[interpolation_index],
            [years[left], years[right]],
            [smoothed[left], smoothed[right]],
        )
    return smoothed


def _selective_smooth_cost_values(
    years,
    values,
    projection_start_year,
    slope_change_threshold,
    max_kink_years,
    major_step_threshold,
    historical_data_mode,
    smooth_projection_curve,
):
    """Apply one historical mode and independent future smoothing treatments."""
    years = np.asarray(years, dtype=float)
    smoothed = np.asarray(values, dtype=float).copy()
    if len(smoothed) < 3:
        return smoothed

    anchor_candidates = np.flatnonzero(years == projection_start_year)
    if len(anchor_candidates) != 1:
        raise ValueError(
            "Selective cost smoothing requires exactly one row for "
            f"projection_start_year={projection_start_year}."
        )
    anchor = int(anchor_candidates[0])

    # Broadcast the first ATB projection value backward across the complete
    # historical period. This makes `broadcast` an exclusive generated history
    # rather than a selective treatment mixed with manual input values.
    if historical_data_mode == 'broadcast':
        smoothed[:anchor] = smoothed[anchor]
    if not smooth_projection_curve:
        return smoothed

    smoothed[anchor:] = _smooth_curve_segment(
        years[anchor:],
        smoothed[anchor:],
        slope_change_threshold,
        max_kink_years,
        major_step_threshold,
    )
    return smoothed


FUTURE_SMOOTHING_TREATMENTS = (
    'smooth_projection_curve',
)
HISTORICAL_DATA_MODES = ('real', 'manual', 'broadcast')
NON_HISTORY_COLUMNS = {'Scenario', 'i', 't', 'turbine', 'type'}


def _metric_modes(historical_data, metric):
    """Return one metric's configured modes keyed by sub-technology class.

    A metric configured as a plain mode applies to every row and is returned
    under the ``None`` key. A metric split by class returns one entry per
    class, which lets an observed series describe some rows of a technology
    while the rest generate their own history.
    """
    try:
        configured = historical_data[metric]
    except KeyError as error:
        raise KeyError(
            f"Historical data has no configured entry for metric {metric!r}."
        ) from error
    if isinstance(configured, dict):
        return dict(configured)
    return {None: configured}


def _historical_mode_for_metric(historical_data, metric, class_value=None):
    """Return one metric history mode, resolved for one sub-technology class."""
    modes = _metric_modes(historical_data, metric)
    if None in modes:
        return modes[None]
    try:
        return modes[class_value]
    except KeyError as error:
        raise KeyError(
            f"Historical data for metric {metric!r} has no entry for "
            f"{class_value!r}. Configured classes: {sorted(modes)}."
        ) from error


def _real_historical_classes(historical_data, metric):
    """Return the sub-technology classes one metric reads from observations."""
    return sorted(
        class_value
        for class_value, mode in _metric_modes(historical_data, metric).items()
        if mode == 'real' and class_value is not None
    )


def _real_historical_metrics(technology_config):
    """Return explicitly configured observed metrics for one technology.

    A metric split by sub-technology class qualifies when any class selects
    `real`; its applier then assigns observations to only those classes.
    """
    historical_data = technology_config['historical_data']
    return sorted(
        metric
        for metric in historical_data
        if 'real' in _metric_modes(historical_data, metric).values()
    )


def _mapping_target_classes(mapping):
    """Return the sub-technologies a reviewed mapping claims, one per series."""
    entries = mapping.get('series') or [mapping]
    inherited = {key: value for key, value in mapping.items() if key != 'series'}
    targets = set()
    for entry in entries:
        merged = {**inherited, **entry}
        targets.update(
            merged.get('turbine_classes') or merged.get('technologies') or []
        )
    return targets


def _validate_historical_data_config(tech, historical_data, settings):
    """Validate one technology's explicit metric-level history choices."""
    label = (
        f"processing.smooth_cost_curves.technologies.{tech}.historical_data"
    )
    if not isinstance(historical_data, dict):
        raise TypeError(
            f"{label} must be a mapping with one entry for every modeled metric."
        )
    valid_metrics = set(settings['techs'][tech]['cols']) - NON_HISTORY_COLUMNS
    unknown = sorted(set(historical_data) - valid_metrics)
    if unknown:
        raise KeyError(f"Unknown historical metrics in {label}: {unknown}")
    missing = sorted(valid_metrics - set(historical_data))
    if missing:
        raise KeyError(f"Missing historical metrics in {label}: {missing}")
    class_column = settings['techs'][tech].get('history_class_column')
    split_metrics = sorted(
        metric for metric in historical_data
        if isinstance(historical_data[metric], dict)
    )
    if split_metrics and not class_column:
        raise KeyError(
            f"Metrics {split_metrics} in {label} are split by sub-technology "
            f"class, but {tech} declares no history_class_column in "
            "settings.yaml naming the column that identifies the class."
        )
    invalid = {
        f"{metric}.{class_value}" if class_value else metric: mode
        for metric in historical_data
        for class_value, mode in _metric_modes(historical_data, metric).items()
        if mode not in HISTORICAL_DATA_MODES
    }
    if invalid:
        raise KeyError(
            f"Unavailable historical metric modes in {label}; choose from "
            f"{list(HISTORICAL_DATA_MODES)}: {invalid}"
        )
    mappings = (
        settings['config']
        .get('historical_cost_sources', {})
        .get('reeds_mappings', {})
        .get(tech, {})
    )
    real_metrics = sorted(
        metric for metric in historical_data
        if 'real' in _metric_modes(historical_data, metric).values()
    )
    unavailable_real = [
        metric for metric in real_metrics if metric not in mappings
    ]
    if unavailable_real:
        raise KeyError(
            f"Observed history is unavailable for metrics in {label}: "
            f"{unavailable_real}"
        )
    # A class-split metric states twice which rows observations describe: here
    # and in the mapping that selects them. Disagreement would silently leave
    # one class on the wrong history, so require the two to name the same set.
    for metric in real_metrics:
        real_classes = set(_real_historical_classes(historical_data, metric))
        if not real_classes:
            continue
        targeted = _mapping_target_classes(mappings[metric])
        if targeted != real_classes:
            raise ValueError(
                f"{label}.{metric} selects real history for classes "
                f"{sorted(real_classes)}, but its mapping under "
                f"historical_cost_sources.reeds_mappings.{tech}.{metric} "
                f"targets {sorted(targeted)}. Make the two agree."
            )
    return dict(historical_data)


def _technology_smoothing_config(tech, settings):
    """Merge common smoothing parameters with one technology's switches.

    ``technologies`` maps a technology name to its switches; every technology
    present in the mapping is processed and every other one is skipped.
    """
    smoothing = settings['config']['processing'].get('smooth_cost_curves', {})
    if not smoothing.get('enabled', False):
        return None

    configured_techs = smoothing.get('technologies', {})
    if not isinstance(configured_techs, dict):
        raise TypeError(
            "processing.smooth_cost_curves.technologies must map each "
            "technology name to its switches."
        )
    if tech not in configured_techs:
        return None
    technology_overrides = configured_techs[tech] or {}
    if not isinstance(technology_overrides, dict):
        raise TypeError(
            "Each processing.smooth_cost_curves.technologies entry must "
            "be a mapping."
        )

    config = {
        key: value for key, value in smoothing.items()
        if key not in {'technologies', 'future_smoothing_treatments'}
    }
    config.update({
        key: value for key, value in technology_overrides.items()
        if key != 'future_smoothing_treatments'
    })

    future_treatments = {name: True for name in FUTURE_SMOOTHING_TREATMENTS}
    common_treatments = smoothing.get('future_smoothing_treatments', {})
    technology_treatments = technology_overrides.get(
        'future_smoothing_treatments', {}
    )
    for label, overrides in (
        ('processing.smooth_cost_curves.future_smoothing_treatments',
         common_treatments),
        (f'processing.smooth_cost_curves.technologies.{tech}.'
         'future_smoothing_treatments',
         technology_treatments),
    ):
        if not isinstance(overrides, dict):
            raise TypeError(f"{label} must be a mapping.")
        unknown = sorted(set(overrides) - set(FUTURE_SMOOTHING_TREATMENTS))
        if unknown:
            raise ValueError(
                f"Unknown future smoothing treatments in {label}: {unknown}"
            )
        nonboolean = sorted(
            name for name, enabled in overrides.items()
            if not isinstance(enabled, bool)
        )
        if nonboolean:
            raise TypeError(
                f"Smoothing treatment switches in {label} must be boolean: "
                f"{nonboolean}"
            )
        future_treatments.update(overrides)
    config['future_smoothing_treatments'] = future_treatments

    fill_historical = config.get('fill_historical', False)
    if not isinstance(fill_historical, bool):
        raise TypeError(
            f"processing.smooth_cost_curves.technologies.{tech}."
            "fill_historical must be true or false."
        )
    config['fill_historical'] = fill_historical

    config['historical_data'] = _validate_historical_data_config(
        tech,
        config.get('historical_data', {}),
        settings,
    )
    return config


def _smoothing_columns(tech, df, smoothing):
    """Return the columns one technology smooths.

    Capacity-factor multipliers ride along with whatever else is selected,
    since a rounded multiplier shows the same artifacts a rounded cost does.
    """
    configured = smoothing.get('columns', 'all')
    if configured in ('all', ['all']):
        columns = _cost_columns(df)
    elif configured == 'capital_costs':
        columns = [c for c in df.columns if c.startswith('capcost')]
    elif isinstance(configured, list):
        columns = list(configured)
    else:
        raise TypeError(
            "processing.smooth_cost_curves.columns must be 'all', "
            "'capital_costs', or a list."
        )
    if 'cf_improvement' in df.columns and 'cf_improvement' not in columns:
        columns.append('cf_improvement')
    if not columns:
        raise ValueError(
            f"Cannot smooth {tech}; no monetary cost columns were selected."
        )
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(
            f"Cannot smooth {tech}; configured cost columns are missing: "
            f"{missing}."
        )
    return columns


def _smoothing_parameters(smoothing):
    """Parse and range-check the numeric smoothing switches."""
    method = smoothing.get('method', 'selective')
    if method not in ('selective', 'linear_bridge'):
        raise ValueError(
            "processing.smooth_cost_curves.method must be 'selective' "
            "or 'linear_bridge'."
        )
    anchor_year = int(smoothing.get('anchor_year', 2022))
    target_year = int(smoothing.get('target_year', 2035))
    if method == 'linear_bridge' and target_year <= anchor_year:
        raise ValueError(
            "processing.smooth_cost_curves.target_year must be later than "
            "anchor_year."
        )
    parameters = {
        'method': method,
        'anchor_year': anchor_year,
        'target_year': target_year,
        'projection_start_year': int(
            smoothing.get('projection_start_year', anchor_year)
        ),
        'slope_change_threshold': float(
            smoothing.get('slope_change_threshold', 0.5)
        ),
        'max_kink_years': int(smoothing.get('max_kink_years', 4)),
        'major_step_threshold': float(
            smoothing.get('major_step_relative_threshold', 0.1)
        ),
        'minimum_adjustment_threshold': float(
            smoothing.get('minimum_adjustment_relative_threshold', 0.005)
        ),
    }
    if parameters['slope_change_threshold'] <= 0:
        raise ValueError("slope_change_threshold must be greater than zero.")
    if parameters['max_kink_years'] < 1:
        raise ValueError("max_kink_years must be at least one.")
    if parameters['major_step_threshold'] <= 0:
        raise ValueError(
            "major_step_relative_threshold must be greater than zero."
        )
    if parameters['minimum_adjustment_threshold'] < 0:
        raise ValueError(
            "minimum_adjustment_relative_threshold must be nonnegative."
        )
    return parameters


def smooth_cost_curve(tech, settings, df):
    """Smooth monetary costs without erasing normal ATB trajectory changes.

    ``selective`` mode fills the historical period from each metric's
    configured source, then bridges clusters of adjacent slope changes in the
    projection. A single slope change, a major step, and a flat stretch are
    each retained as a published statement. ``linear_bridge`` preserves the
    earlier flat-history/anchor-to-target behavior.
    """
    smoothing = _technology_smoothing_config(tech, settings)
    if smoothing is None:
        return df

    columns = _smoothing_columns(tech, df, smoothing)
    parameters = _smoothing_parameters(smoothing)
    method = parameters['method']
    anchor_year = parameters['anchor_year']
    target_year = parameters['target_year']
    projection_start_year = parameters['projection_start_year']
    slope_change_threshold = parameters['slope_change_threshold']
    max_kink_years = parameters['max_kink_years']
    major_step_threshold = parameters['major_step_threshold']
    minimum_adjustment_threshold = parameters['minimum_adjustment_threshold']

    future_treatments = smoothing['future_smoothing_treatments']
    historical_data = smoothing['historical_data']
    history_columns = [
        column for column in historical_data if column in df.columns
    ]
    # Modes are resolved per group below, because a metric may select one mode
    # per sub-technology class and each group is one class.
    mode_columns = list(dict.fromkeys([*history_columns, *columns]))
    configured_modes = {
        column: historical_data[column] for column in mode_columns
    }

    tech_settings = settings['techs'][tech]
    idcols = [
        column for column in tech_settings['indexcols']
        if column not in ['Scenario', 't']
    ]
    groupcols = ['Scenario', *idcols]
    class_column = tech_settings.get('history_class_column')
    output = df.sort_values([*groupcols, 't']).copy()
    # Interpolation generally creates fractional values even when an input
    # frame happened to infer an integer dtype.
    output[columns] = output[columns].astype(float)
    original_values = output[columns].copy()
    groups = output.groupby(groupcols, dropna=False, sort=False).groups

    enabled_future_treatments = [
        name for name, enabled in future_treatments.items() if enabled
    ]
    print(
        f"Smoothing {tech} cost curves: method={method}, columns={columns}, "
        f"historical={configured_modes}, "
        f"minimum_adjustment={minimum_adjustment_threshold:.3%}, "
        f"future_smoothing_treatments={enabled_future_treatments}"
    )
    for group_values, index in groups.items():
        group = output.loc[index].sort_values('t')
        label_values = (
            group_values
            if isinstance(group_values, tuple)
            else (group_values,)
        )
        label = dict(zip(groupcols, label_values))
        # Each group is one sub-technology class, so a metric split by class
        # resolves to a single mode here.
        group_modes = {
            column: _historical_mode_for_metric(
                historical_data, column, label.get(class_column)
            )
            for column in mode_columns
        }
        anchor_rows = group.loc[group['t'] == projection_start_year]
        if len(anchor_rows) != 1:
            raise ValueError(
                f"Cannot apply historical data modes for {tech} series "
                f"{label}: expected exactly one row for projection start "
                f"year {projection_start_year}."
            )
        anchor_index = anchor_rows.index[0]
        historical_index = group.index[group['t'] < projection_start_year]
        broadcast_columns = [
            column for column in history_columns
            if group_modes[column] == 'broadcast'
        ]
        if broadcast_columns:
            output.loc[historical_index, broadcast_columns] = output.loc[
                anchor_index, broadcast_columns
            ].to_numpy()
            group = output.loc[index].sort_values('t')
        if method == 'selective':
            for column in columns:
                output.loc[group.index, column] = _selective_smooth_cost_values(
                    group['t'],
                    group[column],
                    projection_start_year,
                    slope_change_threshold,
                    max_kink_years,
                    major_step_threshold,
                    group_modes[column],
                    future_treatments['smooth_projection_curve'],
                )
            continue

        anchor_rows = group.loc[group['t'] == anchor_year]
        target_rows = group.loc[group['t'] == target_year]
        if len(anchor_rows) != 1 or len(target_rows) != 1:
            raise ValueError(
                f"Cannot smooth {tech} series {label}: expected exactly one row "
                f"for anchor year {anchor_year} and target year {target_year}."
            )

        anchor_index = anchor_rows.index[0]
        target_index = target_rows.index[0]
        broadcast_columns = [
            column for column in columns
            if group_modes[column] == 'broadcast'
        ]
        if broadcast_columns:
            historical_index = group.index[group['t'] <= anchor_year]
            output.loc[historical_index, broadcast_columns] = output.loc[
                anchor_index, broadcast_columns
            ].to_numpy()
        bridge_index = group.index[
            (group['t'] > anchor_year) & (group['t'] < target_year)
        ]
        for column in columns:
            anchor_value = float(output.at[anchor_index, column])
            target_value = float(output.at[target_index, column])
            bridge_years = output.loc[bridge_index, 't'].astype(float)
            output.loc[bridge_index, column] = anchor_value + (
                (target_value - anchor_value)
                * (bridge_years - anchor_year)
                / (target_year - anchor_year)
            )

    # Respect the published ATB trajectory when smoothing proposes only a
    # negligible future adjustment. Historical mode changes (including a full
    # broadcast) are intentionally outside this filter.
    future_mask = output['t'] >= projection_start_year
    candidate_values = output.loc[future_mask, columns]
    source_values = original_values.loc[future_mask, columns]
    relative_scale = source_values.abs().clip(lower=RELATIVE_SCALE_FLOOR)
    relative_adjustment = (
        (candidate_values - source_values).abs() / relative_scale
    )
    keep_original = relative_adjustment < minimum_adjustment_threshold
    output.loc[future_mask, columns] = candidate_values.mask(
        keep_original, source_values
    )

    return output.sort_index()

def add_beccs_techs(tech, settings, df, techcol='i'):
    """
    function to copy costs for beccs_mod to beccs_max

    Parameters
    ----------
    tech: str
        Technology key in settings['techs']
    df: pd.DataFrame
        Must include column `techcol` for beccs technologies
    techcol: str, optional
        Column name identifying the technology label in `df` (default 'i')
    """
    # copy beccs_max from beccs_mod
    df_add = df.copy()
    df_add['i'] = "beccs_max"
    df_out = pd.concat([df, df_add])
    
    return df_out

def format_continuous_battery(tech, settings, df):
    """
    function to format battery technology data

    Parameters
    ----------
    tech: str
        Technology key from settings (used for logging)
    settings: dict
        Parsed settings.yaml values (for expected column names)
    df: pd.DataFrame
        Input dataframe with columns that will be replaced by the battery cost fields (the function drops ['capcost', 'fom'] before merging)
    """
    # Battery power/energy capital costs are extracted from the raw workbook
    # downloaded by scrape_atb_inputs.py. The formatter itself never downloads
    # raw data. A manual CSV remains available for pre-release ATB years.
    atbyear = settings['atbyear']
    from battery_workbook import extract_battery_costs
    workbook = settings['workbook_path']
    if os.path.isfile(workbook):
        battery_costs = extract_battery_costs(workbook)
        # round to 2 decimals to match the historical battery_costs_<year>.csv
        yearcols = [c for c in battery_costs.columns if c not in ('cost', 'Scenario')]
        battery_costs[yearcols] = battery_costs[yearcols].round(2)
        battery_costs.columns = [str(c) for c in battery_costs.columns]
    else:
        fallback = os.path.join(ATBDIR, 'manual_input', f"battery_costs_{atbyear}.csv")
        print(f"Raw ATB workbook not found for {atbyear}; using manual fallback {fallback}")
        if not os.path.isfile(fallback):
            raise FileNotFoundError(
                f"Neither raw workbook nor manual battery fallback exists.\n"
                f"Expected workbook: {workbook}\nExpected fallback: {fallback}\n"
                "Run 'python scrape_atb_inputs.py --only workbook'."
            )
        battery_costs = pd.read_csv(fallback)
    # reshape and format
    battery_costs = pd.melt(battery_costs, id_vars=['cost','Scenario'], var_name='t')
    battery_costs = battery_costs.pivot(index=['Scenario', 't'], columns='cost', values='value').reset_index().rename_axis(None, axis=1)
    battery_costs['t'] = pd.to_numeric(battery_costs['t'], errors='coerce').astype('Int64')
    # assign FOM (assumed to be 2.5% of capital costs)
    fom_mult = 0.025
    battery_costs['fom'] = battery_costs['capcost'] * fom_mult
    battery_costs['fom_energy'] = battery_costs['capcost_energy'] * fom_mult
    battery_costs[['fom', 'fom_energy']] = battery_costs[
        ['fom', 'fom_energy']
    ].round(2)
    # merge with placeholder data and return
    df = df.drop(['capcost', 'fom'], axis=1).merge(battery_costs)
    # if rte is missing from the ATB data, assign 0.85 as the default value
    if 'rte' not in df.columns:
        print("Warning: 'Round-Trip Efficiency' not found in ATB data for battery. Assigning default rte = 0.85.")
        df['rte'] = 0.85
    return df 

def add_csp_techs(tech, settings, df, techcol='i'):
    """
    function to create and append CSP tech types by scaling base tech values

    Parameters
    ----------
    tech: str
        Technology key in settings['techs']
    settings: dict
        Parsed settings.yaml values to determine index columns and cost column names
    df: pd.DataFrame
        Must include `techcol` and cost columns listed in settings['cost_cols'])
    techcol: str, optional
        Column name identifying the technology label in `df` (default 'i').
    """
    # load cost ratios for csp techs
    csp_ratios = pd.read_csv(
        os.path.join(ATBDIR, "manual_input", f"csp_cost_ratios_{settings['atbyear']}.csv")
    ).dropna(subset=['type', 'ratio', 'base_tech'])
    print("updating csp tech costs using the following ratios:")
    print(csp_ratios[['type','ratio']])

    # merge with data
    base_csp_tech = csp_ratios.loc[csp_ratios.base_tech==1, "type"].squeeze()
    if isinstance(base_csp_tech, pd.Series):
        raise ValueError("Multiple base csp techs specified in 'csp_cost_ratios.csv")
    
    # loop through non-base csp techs
    df_add_all = []
    for _,row in csp_ratios.loc[csp_ratios.base_tech==0].iterrows():
        print(f"...updating cost data for {row['type']} using multiplier of {row['ratio']}")
        # get data for baseline csp tech
        df_add = df.loc[df.type == base_csp_tech].copy()
        # update tech name
        df_add['type'] = row['type']
        # apply multiplier to cost fields
        df_add[settings['cost_cols']] *= row['ratio']
        # add to list
        df_add_all.append(df_add)
    
    # combine entries and merge with original data
    df_add_all = pd.concat(df_add_all)
    df_out = pd.concat([df, df_add_all])
    # drop duplicates. keep values added here since previous ones
    # come from the historic files and we want to preserve the newer ATB when available.
    tech_settings = settings['techs'][tech]
    df_out = df_out.drop_duplicates(subset=tech_settings['indexcols'], keep="last")

    return df_out

def format_reeds_output(df, tech_settings):
    """
    function to apply the ReEDS output schema to a formatted scenario table

    ReEDS reads some plant characteristic files by column position rather than by
    column name, so those technologies declare an 'output_cols' mapping of
    internal column name -> ReEDS header in settings.yaml. Technologies without
    that mapping are written using the internal names in 'cols'.

    Parameters
    ----------
    df: pd.DataFrame
        Formatted single-scenario data using the internal column names
    tech_settings: dict
        Settings for the technology being written (settings['techs'][tech])
    """
    output_cols = tech_settings.get('output_cols')
    if not output_cols:
        return df
    missing_cols = [c for c in output_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(
            f"The following columns are specified as 'output_cols' but are not in "
            f"the data: {missing_cols}. Please update your 'settings.yaml'."
        )
    dropped_cols = [c for c in df.columns if c not in output_cols]
    if dropped_cols:
        raise ValueError(
            f"'output_cols' does not cover every output column; missing "
            f"{dropped_cols}. Please update your 'settings.yaml'."
        )
    return df[list(output_cols)].rename(columns=output_cols)


def write_technology_outputs(
    tech_data,
    tech,
    settings,
    tech_settings,
    sensitivity_name,
    outfolder,
    filenames=None,
    output_label="",
):
    """Write one technology's scenario files using the ReEDS output schema."""
    tech_data = tech_data.copy()
    tech_data['Scenario'] = tech_data['Scenario'].str.lower()
    scenarios = list(tech_data['Scenario'].unique())
    if 'moderate' in scenarios:
        scenarios.insert(0, scenarios.pop(scenarios.index('moderate')))

    filename_root = tech_settings.get('reeds_name', tech)
    baseline = None
    written = []
    os.makedirs(outfolder, exist_ok=True)

    for scenario in scenarios:
        if sensitivity_name is not None:
            filename = (
                f"{filename_root}_ATB_{settings['atbyear']}_{scenario}_"
                f"{sensitivity_name}.csv"
            )
        else:
            filename = f"{filename_root}_ATB_{settings['atbyear']}_{scenario}.csv"

        check_columns(tech_data, tech_settings['cols'], 'cols', tech)
        scendata = tech_data.loc[
            tech_data.Scenario == scenario, tech_settings['cols']
        ]
        scendata = scendata.round(
            tech_settings.get('decimals', settings['decimals'])
        )
        scendata = scendata.sort_values(
            by=scendata.columns.to_list()
        ).reset_index(drop=True)

        if baseline is None:
            baseline = scendata.copy()
        elif scendata.equals(baseline):
            print(
                f"...{scenario} is identical to the {scenarios[0]} scenario, "
                "skipping."
            )
            continue

        scendata_out = format_reeds_output(scendata, tech_settings)
        label = f" {output_label}" if output_label else ""
        print(f"Saving{label} {filename}")
        scendata_out.to_csv(
            os.path.join(outfolder, filename),
            index=False,
            lineterminator=CSV_LINE_TERMINATOR,
        )
        written.append(filename)
        if filenames is not None:
            filenames.append(filename)

    return written


def process_tech_file(atb_data, tech, settings, filenames, dollaryear, deflator, sensitivity_name, outfolder, args):
    """
    function to format per-technology output files

    Parameters
    ----------
    atb_data: pd.DataFrame
        Technology-level data from ATB flat-file
    tech: str
        Technology-specific key in settings['techs'] to process
    settings: dict
        Parsed settings.yaml values
    filenames: list
        List to which produced filenames will be appended.
    dollaryear: pd.Series
        Mapping of scenario -> dollar year
    deflator: pd.Series
        Mapping of dollar year -> deflator multiplier
    sensitivity_name: str or None
        Optional suffix added to output filenames for sensitivity runs
    outfolder: str
        Directory path where output CSVs will be written
    args: argparse.Namespace
        CLI arguments for debug mode and run options
    """
    # subset to relevant technology
    print(f"Processing {tech} file.")
    tech_settings = settings['techs'][tech]
    tech_data = atb_data.loc[atb_data['Technology'] == tech_settings['Technology']]
    # apply tech-specific subset_rows (Case, CRPYears)
    if 'subset_rows' in tech_settings:
        tech_data, _ = subset_atb_rows(tech_settings['subset_rows'], tech_data)
    else:
        raise ValueError(f"Missing 'subset_rows' for {tech} in settings.yaml")
    # get subtech(s), specified either as a dictionary or a single string
    if 'DisplayName' in tech_settings:
        if isinstance(tech_settings['DisplayName'], dict):
            tech_data = tech_data.loc[tech_data['DisplayName'].isin(tech_settings['DisplayName'].keys())]
            missing_subtechs = (
                set(tech_settings['DisplayName']) - set(tech_data['DisplayName'].unique())
            )
            tech_data = tech_data.replace({'DisplayName': tech_settings['DisplayName']})
            # verify all expected subtechs were found in the data
            if missing_subtechs:
                print(f"Warning: the following DisplayName(s) for {tech} were not found in the ATB subset: {sorted(missing_subtechs)}")
        else:
            print(f"Subsetting to {tech_settings['DisplayName']}")
            tech_data = tech_data.loc[tech_data['DisplayName'] == tech_settings['DisplayName']]
    if tech_data.empty:
        if args.debug:
            breakpoint()
        else:
            raise EmptyDataError(f"\n\nNo data found for {tech} in ATB data. Check file for values for specified subset_rows.")
    
    # add in columns as needed
    if 'addcols' in tech_settings:
        print(f"Adding columns {tech_settings['addcols']}")
        for col in tech_settings['addcols']:
            tech_data[col] = tech_settings['addcols'][col]

    # rename columns as needed
    if 'renamecols' in tech_settings:
        print(f"Renaming columns {tech_settings['renamecols']}")
        tech_data = tech_data.rename(columns=tech_settings['renamecols'])

    # grab columns needed for this particular tech
    atb_params = [k for k, v in settings['param_names'].items() if v in tech_settings['cols']]
    tech_data_out = tech_data.loc[tech_data.Parameter.isin(atb_params)].copy()
    # check if empty    
    if tech_data_out.empty:
        raise EmptyDataError("\n\n"
                            "Dataframe is empty after subsetting; check 'param_names' in settings.yaml."
                            f"Current values: {settings['param_names']}"
        )
    # check columns and then pivot to wide format
    check_columns(tech_data_out, tech_settings['indexcols'], 'indexcols', tech)
    tech_data_out = tech_data_out.pivot(index=tech_settings['indexcols'], columns='Parameter', values='value')
    tech_data_out = tech_data_out.rename_axis(None, axis=1)
    # map parameters to new column names and format
    tech_data_out = tech_data_out.rename(columns=settings['param_names'])
    # convert numeric columns 
    tech_data_out = tech_data_out.apply(pd.to_numeric).reset_index()

    # if vom is missing in the input ATB data, assign zero
    if tech in zero_vom_techs and 'vom' not in tech_data_out.columns:
        print(f"Warning: 'Variable O&M' missing in ATB data for {tech}. Assigning vom = 0.0")
        tech_data_out['vom'] = 0.0

    # if processing continuous battery techs update energy cost data
    if 'battery' in tech:
        tech_data_out = format_continuous_battery(tech, settings, tech_data_out)

    # Apply technology transformations to the current scraped projections first.
    # History is merged afterward so derived historic series are not overwritten.
    if 'functions' in tech_settings:
        for function_name in tech_settings['functions']:
            selected_function = FUNCTION_MAPPING.get(function_name.lower())
            if selected_function:
                print(f"Running {function_name}")
                tech_data_out = selected_function(tech, settings, tech_data_out)
            else:
                raise NameError(f"'{function_name}' is not a supported function. "
                                "Define and add to FUNCTION_MAPPING.")

    # Combine the current projections with the stable local historical baseline.
    tech_data_out = merge_historical_atb_data(
        tech_data_out, tech, settings, dollaryear, deflator
    )
    tech_data_unsmoothed = tech_data_out.copy()

    # This user-facing option is intentionally applied after history is merged,
    # so it can bridge the history/current-ATB boundary without changing the
    # versioned historical source files. It is disabled by default.
    tech_data_out = smooth_cost_curve(tech, settings, tech_data_out)

    # for fuel cells, backfill capcost values as 9999 for all pre-2035 years
    if tech == 'fuelcell' and 'capcost' in tech_data_out.columns:
        print("Assigning capcost = 9999 for fuelcell years before 2035")
        tech_data_out.loc[tech_data_out['t'] < 2035, 'capcost'] = 9999
        tech_data_unsmoothed.loc[
            tech_data_unsmoothed['t'] < 2035, 'capcost'
        ] = 9999

    smoothing = settings['config']['processing'].get('smooth_cost_curves', {})
    baseline_directory = settings.get('unsmoothed_output_dir')
    if smoothing.get('enabled', False) and baseline_directory:
        baseline_folder = (
            baseline_directory
            if os.path.isabs(baseline_directory)
            else os.path.join(ATBDIR, baseline_directory)
        )
        write_technology_outputs(
            tech_data_unsmoothed,
            tech,
            settings,
            tech_settings,
            sensitivity_name,
            baseline_folder,
            output_label="temporary pre-smoothing",
        )

    write_technology_outputs(
        tech_data_out,
        tech,
        settings,
        tech_settings,
        sensitivity_name,
        outfolder,
        filenames=filenames,
    )

def update_dollaryear(settings, filenames, dollaryear):
    """
    function to create dollar year entries for new tech files

    Parameters
    ----------
    settings: dict
        Parsed settings['dollaryear'] value as the dollar year to assign to the new files
    filenames: list[str]
        Output filename produced by this run; strips the ".csv" suffix to derive Scenario names
    dollaryear: pd.Series
        Scenario -> Dollar.Year mapping (index=Scenario, name='Dollar.Year')
    """
    # strip .csv from filenames
    scennames = [f[:-4]  for f in filenames]
    # create new dollar year entries
    newdollaryear = pd.Series(data=[settings['dollaryear']] * len(scennames), index=scennames)
    # combine old and new and format for ReEDS
    newdollaryear = pd.concat([dollaryear, newdollaryear]).sort_index()
    # drop duplicates
    newdollaryear = newdollaryear[~newdollaryear.index.duplicated(keep='first')]
    newdollaryear.name = "Dollar.Year"
    newdollaryear.index.name = "Scenario"

    return newdollaryear

def get_financials(atb_data, settings, tech, financial_params, scenarios=["Moderate","*"]):
    """
    function to get full financial parameters for a specific technology

    Parameters
    ----------
    atb_data: pd.DataFrame
        ATB flat file data
    settings: dict
        Parsed 'decimals' and 'reeds_start_year' values
    tech: str
        ATB technology name to filter to
    financial_params: dict
        Mapping of ATB parameter name -> target column name for pivoting
    scenarios: list[str], optional
        List of scenario names to include (default ["Moderate","*"])
    """
    techfinancial = atb_data.loc[(atb_data.Technology == tech) 
                                & (atb_data.Scenario.isin(scenarios))
                                & (atb_data.Parameter.isin(financial_params))
                                ].copy()
    if techfinancial.empty:
        avail_params = atb_data.loc[(atb_data.Technology == tech) 
                                    & (atb_data.Scenario.isin(scenarios)), "Parameter"
                                    ].unique()
        if args.debug:
            breakpoint()
        else:

            raise EmptyDataError(f"Looking for the following financial parameters for {tech}:\n{list(financial_params.keys())}.\n"
                                 f"Found the following parameters:\n{list(avail_params)}"
                                 )
    techfinancial = techfinancial.rename(columns={"variable":"t"})
    # reorder columns
    techfinancial = techfinancial.pivot(index="t", columns='Parameter', values='value').rename(columns=financial_params)
    colorder = [v for k,v in financial_params.items()]
    # convert to numeric and round
    techfinancial = techfinancial[colorder].apply(pd.to_numeric).round(settings['decimals'])
    # reindindex to include years before atb
    atb_end_year = techfinancial.index.values.max()
    techfinancial = techfinancial.reindex(index=list(range(settings['reeds_start_year'],atb_end_year+1))).bfill().reset_index()

    return techfinancial

def update_financials(settings, atb_data, outfolder):
    """
    function to write updated system and tech financial outputs

    Parameters
    ----------
    settings: dict
        Expects keys such as 'atbyear', 'reedspath', 'wacc_base_tech', 'decimals', and tech-specific configs under settings['techs'])
    atb_data: pd.DataFrame
        ATB flat file data used to extract financial parameters and WACC
    outfolder: str
        Output directory path
    """
    print("Processing system financials.")

    financial_params = {
        'Interest Rate Nominal':'interest_rate_nom',
        'Rate of Return on Equity Nominal':'rroe_nom',
        'Debt Fraction':'debt_fraction',
        'Tax Rate (Federal and State)':'tax_rate'
    }

    ## system financial file
    # uses base tech specified in settings (typically natural gas)
    sysfinancial = get_financials(atb_data, settings, settings['wacc_base_tech'], financial_params)
    
    ## tech financials
    # get old tech financials file
    print("Processing tech financials.")
    prevyear = settings['atbyear']-1
    filename_old = f"financials_tech_ATB{prevyear}.csv"
    financials_old = pd.read_csv(os.path.join(settings['reedspath'], 'inputs', 'financials', filename_old))

    wacc = atb_data.loc[(atb_data['Parameter'] == "WACC Real") & (atb_data['Scenario'] == "Moderate"), ["Technology", "variable", "value"]]
    wacc = wacc.rename(columns={"variable": "t", "value":"wacc_real"})
    wacc['wacc_real'] = pd.to_numeric(wacc['wacc_real'])
    # use natural gas as baseline
    wacc_baseline = wacc.loc[wacc.Technology == settings['wacc_base_tech']]
    wacc_baseline = wacc_baseline.rename(columns={"wacc_real":"wacc_real_base"}).drop("Technology", axis=1)
    
    wacc = wacc.merge(wacc_baseline, on=['t'])
    # get financing difference relative to base
    wacc['finance_diff_real_update'] = wacc['wacc_real'] - wacc['wacc_real_base']

    # get WACC values for techs that were processed
    financials_update = []
    for tech in settings['techs']: 
        print(f"Loading financials for {tech}")

        # for batteries we need to calculate the WACC Real value
        if tech == "battery":
            # batteries are missing WACC, so need to get individual components and recalculate
            # since we don't have debt fraction for batteries we take the value from PV for now
            battery_params = financial_params.copy()
            battery_params.pop('Debt Fraction')
            battery_params.update({'Inflation Rate':'inflation_rate'})
            pv_param = {'Debt Fraction': financial_params['Debt Fraction']}
            battfinancial = get_financials(atb_data, settings, settings['techs'][tech]['Technology'], battery_params)
            pvfinancial = get_financials(atb_data, settings, settings['techs']['upv']['Technology'], pv_param)
            # merge and calculate WACC in two steps:
            # 1. WACC Nominal = Debt Fraction * Interest Rate Nominal * (1 - Tax Rate) + (1 - Debt Fraction) * Rate of Return
            # 2. WACC Real = (1 + WACC Nominal) / (1 + Inflation Rate) - 1
            wacc_tech = battfinancial.merge(pvfinancial)
            wacc_tech['wacc_nom'] = (wacc_tech['debt_fraction'] 
                                     * wacc_tech['interest_rate_nom'] 
                                     * (1 - wacc_tech['tax_rate']) 
                                     + (1 - wacc_tech['debt_fraction'])
                                     * wacc_tech['rroe_nom']
            )
            wacc_tech['wacc_real'] = (
                (1 + wacc_tech['wacc_nom']) / (1 + wacc_tech['inflation_rate']) - 1 
            ) 
            wacc_tech = wacc_tech.merge(wacc_baseline, on=['t'])
            wacc_tech['finance_diff_real_update'] = wacc_tech['wacc_real'] - wacc_tech['wacc_real_base']

        else:
            wacc_tech = wacc.loc[wacc.Technology == settings['techs'][tech]['Technology']].copy()

        if wacc_tech.empty:
            print(f"...no wacc found for {tech}, skipping.")
        else:
            # overwrite tech with new financial name if specified
            if 'financialname' in settings['techs'][tech]:
                wacc_tech['i'] = settings['techs'][tech]['financialname']
                wacc_tech = wacc_tech.drop_duplicates()
            # otherwise use exist tech name
            else:
                wacc_tech = wacc_tech.rename(columns={'Technology':'i'})
            # add to list of new financials to update
            financials_update.append(wacc_tech[['i', 't', 'finance_diff_real_update']])
            
    financials_update = pd.concat(financials_update)

    # merge with old file and rewrite new year values upto the max available ATB year
    max_atb_year = int(financials_update['t'].max())
    max_old_year = int(financials_old['t'].max())
    if max_atb_year > max_old_year:
        new_years = list(range(max_old_year + 1, max_atb_year + 1))
        # get unique (i, country) combinations from old file
        id_combos = financials_old[['i', 'country']].drop_duplicates()
        # create new rows for each (i, country) × new year
        new_rows = id_combos.merge(pd.DataFrame({'t': new_years}), how='cross')
        # merge old file's non-year columns onto new rows (forward-fill from last known year)
        last_year_vals = financials_old.loc[financials_old['t'] == max_old_year].drop('t', axis=1)
        new_rows = new_rows.merge(last_year_vals, on=['i', 'country'], how='left')
        financials_old = pd.concat([financials_old, new_rows], ignore_index=True)
    financials_out = financials_old.merge(financials_update, on=['i', 't'], how='left')
    # fill forward by country and tech to create values for pre-ATB years
    financials_out['finance_diff_real_update'] = financials_out.groupby(['i','country'])['finance_diff_real_update'].bfill()
    
    # update relevant techs while leaving others in place
    financials_out['finance_diff_real'] = np.where(financials_out['finance_diff_real_update'].isna(), 
                                                   financials_out['finance_diff_real'], 
                                                   financials_out['finance_diff_real_update'].round(settings['decimals'])
                                                   )

    # write system and tech financial files
    sysfile_new = f"financials_sys_ATB{settings['atbyear']}.csv"
    sysfinancial.to_csv(
        os.path.join(outfolder, sysfile_new), index=False,
        lineterminator=CSV_LINE_TERMINATOR,
    )

    techfile_new = f"financials_tech_ATB{settings['atbyear']}.csv"
    financials_out = financials_out[financials_old.columns]
    financials_out.to_csv(
        os.path.join(outfolder, techfile_new), index=False,
        lineterminator=CSV_LINE_TERMINATOR,
    )

    return sysfile_new, techfile_new

#%% ===========================================================================
### --- Main ---
### ===========================================================================
def main(args):    
    settings = load_processing_settings(args.config)
    settings['unsmoothed_output_dir'] = getattr(
        args, 'unsmoothed_output_dir', None
    )
    processing = settings['config']['processing']
    outfolder = settings['output_dir']
    args.skip_costs = args.skip_costs or not processing.get('update_costs', True)
    should_update_financials = processing.get('update_financials', True)
    if args.sensitivity_name is None:
        args.sensitivity_name = processing.get('sensitivity_name')

    smoothing = processing.get('smooth_cost_curves', {})
    if smoothing.get('enabled', False):
        smoothing_techs = smoothing.get('technologies', {})
        if not isinstance(smoothing_techs, dict):
            raise TypeError(
                "processing.smooth_cost_curves.technologies must map each "
                "technology name to its switches."
            )
        invalid_entries = [
            tech for tech, config in smoothing_techs.items()
            if config is not None and not isinstance(config, dict)
        ]
        if invalid_entries:
            raise TypeError(
                "Each processing.smooth_cost_curves.technologies entry "
                f"must be a mapping: {invalid_entries}"
            )
        unknown_smoothing_techs = [
            tech for tech in smoothing_techs
            if tech not in settings['techs']
        ]
        if unknown_smoothing_techs:
            raise ValueError(
                "Unknown technologies in processing.smooth_cost_curves: "
                f"{unknown_smoothing_techs}"
            )
        baseline_directory = settings.get('unsmoothed_output_dir')
        if baseline_directory:
            baseline_folder = (
                baseline_directory
                if os.path.isabs(baseline_directory)
                else os.path.join(ATBDIR, baseline_directory)
            )
            os.makedirs(baseline_folder, exist_ok=True)
            year_marker = f"_ATB_{settings['atbyear']}_"
            for filename in os.listdir(baseline_folder):
                if year_marker in filename and filename.endswith('.csv'):
                    os.remove(os.path.join(baseline_folder, filename))

    # if output folder does not exist, create it
    os.makedirs(outfolder, exist_ok=True)
    
    # get list of techs to process
    configured_techs = processing.get('technologies', 'all')
    requested_techs = args.techs if args.techs is not None else configured_techs
    if requested_techs == 'all' or requested_techs == ['all']:
        techs_to_run = list(settings['techs'].keys())
    else: 
        techs_to_run = requested_techs if isinstance(requested_techs, list) else [requested_techs]
        missing_techs = [t for t in techs_to_run if t not in settings['techs']]
        if missing_techs:
            raise ValueError(f"The following technologies are not in settings['techs']: {missing_techs}")
    
    # check if ReEDS repo is correctly specified
    if not os.path.isdir(settings['reedspath']):
        raise FileNotFoundError(f"Could not find '{settings['reedspath']}'; check config.yaml.")
    
    # load dollaryear file in ReEDS
    dollaryear = pd.read_csv(os.path.join(settings['reedspath'], 'inputs', 'plant_characteristics', 'dollaryear.csv'), 
                             index_col='Scenario').squeeze()
    deflator = pd.read_csv(os.path.join(settings['reedspath'], 'inputs', 'financials', 'deflator.csv'), 
                           index_col='*Dollar.Year').squeeze()
    if not args.skip_costs:
        validate_real_historical_data(settings, techs_to_run, deflator)
    # load ATB flat file
    atb_data = load_atb_flat_file(settings, args, techs_to_run)

    ## process technology files
    filenames = []
    if args.skip_costs:
        print("Skipping cost files.")
    else:
        print("---------------------")
        for tech in techs_to_run:    
            process_tech_file(atb_data, tech, settings, filenames, 
                            dollaryear, deflator, args.sensitivity_name, outfolder, args)
            print("---------------------")
        # update dollaryear file
        newdollaryear = update_dollaryear(settings, filenames, dollaryear)
    
    ## update financials file
    if should_update_financials:
        sysfinancialfile, techfinancialfile = update_financials(settings, atb_data, outfolder)
    else:
        print("Skipping financials (processing.update_financials is false).")

    ## copy new files to ReEDS
    if settings['copy_to_reeds']:

        if not args.skip_costs:
            # copy tech files
            for f in filenames:
                shutil.copy(os.path.join(outfolder,f), os.path.join(settings['reedspath'],'inputs','plant_characteristics',f))
            # update dollaryear file for tech files
            newdollaryear.to_csv(
                os.path.join(settings['reedspath'],'inputs','plant_characteristics','dollaryear.csv'),
                index=True, lineterminator=CSV_LINE_TERMINATOR,
            )
        
        # copy financial files
        if should_update_financials:
            shutil.copy(os.path.join(outfolder,sysfinancialfile), os.path.join(settings['reedspath'],'inputs','financials',sysfinancialfile))
            shutil.copy(os.path.join(outfolder,techfinancialfile), os.path.join(settings['reedspath'],'inputs','financials',techfinancialfile))


if __name__ == "__main__":
    print("Processing ATB files")
    parser = argparse.ArgumentParser(description="Generate ATB files.")
    parser.add_argument('--config', help='path to config.yaml (default: ../config.yaml)')
    parser.add_argument('--techs', '-t', nargs='+', default=None,
                    help='one or more techs; defaults to processing.technologies in config.yaml')
    parser.add_argument('--sensitivity_name', '-s', type=str, 
                    help='suffix to append to file name for sensitivities')
    parser.add_argument('--skip_costs', '-c', action="store_true",
                    help='skip updating cost files for this run')
    parser.add_argument('--debug', '-d', action="store_true",
                    help='option to run in debug mode')
    parser.add_argument(
        '--unsmoothed-output-dir',
        help=(
            'temporary directory for pre-smoothing outputs used by the '
            'comparison stage'
        ),
    )
    args = parser.parse_args()

    # list of supported custom functions to call from settings.yml
    FUNCTION_MAPPING = {
        'normalize_cf': normalize_cf,
        'apply_offshore_cost_multipliers': apply_offshore_cost_multipliers,
        'apply_coal_projection_overrides': apply_coal_projection_overrides,
        'smooth_hist_cf': smooth_hist_cf,
        'add_csp_techs': add_csp_techs,
        'add_beccs_techs': add_beccs_techs,
    }
    main(args)
