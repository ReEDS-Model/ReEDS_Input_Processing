"""Compare generated ATB 2024 CSVs with the current ReEDS input files."""

from __future__ import annotations

import argparse
import math
from pathlib import Path
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ATB_DIR = Path(__file__).resolve().parents[1]
SCRIPT_DIR = ATB_DIR / "scripts"
sys.path.insert(0, str(SCRIPT_DIR))

from atb_config import (
    DEFAULT_CONFIG_PATH,
    load_processing_settings,
    raw_file_path,
    resolve_atb_path,
)


DEFAULT_PLOT_DIR = Path(__file__).resolve().parent / "plot_comparison"

FILENAME_PREFIX_MAP = {
    "wind-ons_": "ons-wind_",
    "wind-ofs_": "ofs-wind_",
}

REEDS_COLUMN_MAP = {
    "Turbine": "turbine",
    "Year": "t",
    "CF_mult": "cf_improvement",
    "Overnight Cap Cost $/kW": "capcost",
    "Fixed O&M $/(kW-yr)": "fom",
    "Var O&M $/MWh": "vom",
}

PREFERRED_KEYS = ["i", "type", "turbine", "t", "rsc_mult"]

# ``{d}`` is replaced by the output dollar year, e.g. "2023$/kW".
METRIC_LABELS = {
    "capcost": "Capital cost ({d}$/kW)",
    "capcost_energy": "Energy capital cost ({d}$/kWh)",
    "fom": "Fixed O&M ({d}$/kW-year)",
    "fom_energy": "Energy fixed O&M ({d}$/kWh-year)",
    "vom": "Variable O&M ({d}$/MWh)",
    "heatrate": "Heat rate (MMBtu/MWh)",
    "cf_improvement": "Capacity-factor multiplier",
    "rte": "Round-trip efficiency",
    "rsc_mult": "Resource-supply-curve multiplier",
}


def metric_label(metric: str, dollar_year, reference_dollar_year=None) -> str:
    """Return the axis label for a metric with the dollar year spelled out.

    When the ReEDS reference is in a different dollar year, a cost axis names
    both, e.g. "Capital cost ($/kW; generated 2023$, ReEDS 2022$)", because
    the two sides share the axis without conversion.
    """
    template = METRIC_LABELS.get(metric, metric)
    if "{d}" not in template:
        return template
    if reference_dollar_year is None or reference_dollar_year == dollar_year:
        return template.format(d="" if dollar_year is None else dollar_year)
    base = template.format(d="")
    return (
        f"{base[:-1]}; generated {dollar_year}$, "
        f"ReEDS {reference_dollar_year}$)"
    )


def escape_dollars(text: str) -> str:
    """Escape ``$`` so matplotlib does not read paired signs as math."""
    return text.replace("$", r"\$")


def reeds_dollar_years(reeds_dir: Path) -> dict[str, int]:
    """Read ReEDS' per-file dollar years (``dollaryear.csv``) if present."""
    table = reeds_dir / "dollaryear.csv"
    if not table.is_file():
        return {}
    frame = pd.read_csv(table)
    return dict(zip(frame["Scenario"], frame["Dollar.Year"].astype(int)))

PROVENANCE_COLORS = {
    "Manual history": "#0072B2",
    "Observed history (real)": "#009E73",
    "Calculated project history": "#117A65",
    "Scaled real history": "#A6761D",
    "Split real history": "#56B4E9",
    "Filled real history": "#D55E00",
    "Broadcast history": "#CC79A7",
    "Unavailable placeholder": "#999999",
    "Indexed history (ATB level, observed trend)": "#009E73",
    "Indexed anchor (observed O&M)": "#00553F",
    "ATB projection (raw)": "#6E6E6E",
    "ATB projection (smoothed)": "#E69F00",
}

PROJECTION_CATEGORIES = {
    "ATB projection (raw)",
    "ATB projection (smoothed)",
}

SCENARIO_ORDER = ["conservative", "moderate", "advanced"]

SCENARIO_COLORS = {
    "conservative": "#D55E00",
    "moderate": "#0072B2",
    "advanced": "#009E73",
}

# Raw ATB projections in the smoothing plots take one color per scenario.
# These hues avoid every provenance color so history and projection never
# share a color; moderate stays near the plain ATB gray.
PROJECTION_SCENARIO_COLORS = {
    "conservative": "#7B3294",
    "moderate": "#4D4D4D",
    "advanced": "#0F8B8D",
}

# Round dots stay reserved for historical input values.
SCENARIO_MARKERS = {
    "conservative": "v",
    "moderate": "s",
    "advanced": "^",
}

EXTRA_MARKERS = ["D", "P", "X"]

HISTORY_MARKER = "o"

# Sub-technology series (turbine classes, CSP types, plant types) share each
# panel and are told apart by line style.
SERIES_LINESTYLES = [
    "-",
    "--",
    "-.",
    ":",
    (0, (5, 1)),
    (0, (3, 1, 1, 1)),
    (0, (1, 1)),
    (0, (5, 2, 1, 2)),
]


def reeds_filename(generated_name: str, atb_year=None, reeds_year=None) -> str:
    """Map a generated filename to its ReEDS filename.

    ReEDS may still carry an earlier ATB vintage than the one generated, so the
    year token is swapped when ``reeds_year`` differs from ``atb_year``.
    """
    name = generated_name
    if reeds_year is not None and atb_year is not None and reeds_year != atb_year:
        name = name.replace(f"_ATB_{atb_year}_", f"_ATB_{reeds_year}_")
    for generated_prefix, reeds_prefix in FILENAME_PREFIX_MAP.items():
        if name.startswith(generated_prefix):
            return reeds_prefix + name[len(generated_prefix):]
    return name


def normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize known legacy ReEDS column names and cell dtypes."""
    frame = frame.rename(columns=REEDS_COLUMN_MAP).copy()
    for column in frame.columns:
        numeric = pd.to_numeric(frame[column], errors="coerce")
        nonempty = frame[column].notna().sum()
        if nonempty and numeric.notna().sum() == nonempty:
            frame[column] = numeric
        elif frame[column].dtype == object:
            frame[column] = frame[column].astype("string").str.strip()
    return frame


def choose_keys(generated: pd.DataFrame, reeds: pd.DataFrame) -> list[str]:
    """Choose stable identifier columns shared by both frames."""
    shared = set(generated.columns) & set(reeds.columns)
    keys = [column for column in PREFERRED_KEYS if column in shared]
    if "t" not in keys and "t" in shared:
        keys.append("t")
    return keys


def compare_file(
    generated_path: Path,
    reeds_path: Path,
    absolute_tolerance: float,
    relative_tolerance: float,
) -> dict:
    """Compare one generated/ReEDS file pair in memory."""
    result = {
        "generated_file": generated_path.name,
        "reeds_file": reeds_path.name,
        "generated_rows": 0,
        "reeds_rows": 0,
        "key_columns": "",
        "columns_only_generated": "",
        "columns_only_reeds": "",
        "duplicate_keys_generated": 0,
        "duplicate_keys_reeds": 0,
        "rows_only_generated": 0,
        "rows_only_reeds": 0,
        "shared_rows": 0,
        "changed_cells": 0,
        "max_absolute_difference": 0.0,
        "max_relative_difference": 0.0,
        "status": "",
    }

    if not reeds_path.exists():
        result["status"] = "MISSING_REEDS_FILE"
        return result

    generated = normalize_frame(pd.read_csv(generated_path))
    reeds = normalize_frame(pd.read_csv(reeds_path))
    result["generated_rows"] = len(generated)
    result["reeds_rows"] = len(reeds)

    generated_columns = set(generated.columns)
    reeds_columns = set(reeds.columns)
    only_generated = sorted(generated_columns - reeds_columns)
    only_reeds = sorted(reeds_columns - generated_columns)
    result["columns_only_generated"] = ";".join(only_generated)
    result["columns_only_reeds"] = ";".join(only_reeds)

    keys = choose_keys(generated, reeds)
    result["key_columns"] = ";".join(keys)
    if not keys:
        result["status"] = "NO_SHARED_KEY"
        return result

    result["duplicate_keys_generated"] = int(generated.duplicated(keys).sum())
    result["duplicate_keys_reeds"] = int(reeds.duplicated(keys).sum())
    if result["duplicate_keys_generated"] or result["duplicate_keys_reeds"]:
        result["status"] = "DUPLICATE_KEYS"
        return result

    value_columns = sorted((generated_columns & reeds_columns) - set(keys))
    merged = generated.merge(
        reeds,
        on=keys,
        how="outer",
        suffixes=("__generated", "__reeds"),
        indicator=True,
    )
    only_generated_rows = merged.loc[merged["_merge"] == "left_only", keys]
    only_reeds_rows = merged.loc[merged["_merge"] == "right_only", keys]
    shared = merged.loc[merged["_merge"] == "both"].copy()
    result["rows_only_generated"] = len(only_generated_rows)
    result["rows_only_reeds"] = len(only_reeds_rows)
    result["shared_rows"] = len(shared)

    differences = []
    max_abs = 0.0
    max_rel = 0.0
    for column in value_columns:
        generated_column = f"{column}__generated"
        reeds_column = f"{column}__reeds"
        generated_values = shared[generated_column]
        reeds_values = shared[reeds_column]
        if pd.api.types.is_numeric_dtype(generated_values) and pd.api.types.is_numeric_dtype(reeds_values):
            generated_numeric = pd.to_numeric(generated_values, errors="coerce")
            reeds_numeric = pd.to_numeric(reeds_values, errors="coerce")
            equal = np.isclose(
                generated_numeric,
                reeds_numeric,
                atol=absolute_tolerance,
                rtol=relative_tolerance,
                equal_nan=True,
            )
            changed = shared.loc[~equal, keys].copy()
            if not changed.empty:
                changed["column"] = column
                changed["generated_value"] = generated_numeric.loc[~equal].values
                changed["reeds_value"] = reeds_numeric.loc[~equal].values
                changed["absolute_difference"] = np.abs(
                    changed["generated_value"] - changed["reeds_value"]
                )
                denominator = np.maximum(np.abs(changed["reeds_value"]), absolute_tolerance)
                changed["relative_difference"] = changed["absolute_difference"] / denominator
                max_abs = max(max_abs, float(changed["absolute_difference"].max()))
                max_rel = max(max_rel, float(changed["relative_difference"].max()))
                differences.append(changed)
        else:
            equal = generated_values.fillna("<NA>").eq(reeds_values.fillna("<NA>"))
            changed = shared.loc[~equal, keys].copy()
            if not changed.empty:
                changed["column"] = column
                changed["generated_value"] = generated_values.loc[~equal].values
                changed["reeds_value"] = reeds_values.loc[~equal].values
                changed["absolute_difference"] = np.nan
                changed["relative_difference"] = np.nan
                differences.append(changed)

    if differences:
        difference_frame = pd.concat(differences, ignore_index=True)
        result["changed_cells"] = len(difference_frame)
    result["max_absolute_difference"] = max_abs
    result["max_relative_difference"] = max_rel

    has_structure_difference = bool(
        only_generated
        or only_reeds
        or result["rows_only_generated"]
        or result["rows_only_reeds"]
    )
    if has_structure_difference:
        result["status"] = "STRUCTURE_DIFFERENCE"
    elif result["changed_cells"]:
        result["status"] = "VALUE_DIFFERENCE"
    else:
        result["status"] = "MATCH"
    return result


def series_columns(frame: pd.DataFrame) -> list[str]:
    """Return categorical columns that identify separate plotted series."""
    excluded = {"t", "rsc_mult"}
    return [
        column
        for column in frame.columns
        if column not in excluded and not pd.api.types.is_numeric_dtype(frame[column])
    ]


def metric_columns(generated: pd.DataFrame, reeds: pd.DataFrame) -> list[str]:
    """Return shared numeric output columns, with familiar metrics first."""
    excluded = {"t"}
    shared = set(generated.columns) & set(reeds.columns)
    numeric = [
        column
        for column in shared
        if column not in excluded
        and pd.api.types.is_numeric_dtype(generated[column])
        and pd.api.types.is_numeric_dtype(reeds[column])
    ]
    preferred = [column for column in METRIC_LABELS if column in numeric]
    return preferred + sorted(set(numeric) - set(preferred))


def series_label(group_values: tuple, columns: list[str]) -> str:
    """Build a compact label for a plotted technology series."""
    if not columns:
        return "all"
    return ", ".join(
        f"{column}={value}" for column, value in zip(columns, group_values)
    )


def grouped_series(frame: pd.DataFrame, columns: list[str]):
    """Split a frame into time-series groups and labels."""
    if not columns:
        return [("all", frame.sort_values("t"))]
    groups = []
    grouper = columns[0] if len(columns) == 1 else columns
    for values, group in frame.groupby(grouper, dropna=False, sort=True):
        values_tuple = values if isinstance(values, tuple) else (values,)
        groups.append((series_label(values_tuple, columns), group.sort_values("t")))
    return groups


def scenario_groups(generated_files: list[Path]) -> dict[str, dict[str, Path]]:
    """Group ``<tech>_ATB_<year>_<scenario>.csv`` files by technology.

    Scenarios are ordered conservative, moderate, advanced so the same
    scenario always gets the same color and line style across plots.
    """
    groups: dict[str, dict[str, Path]] = {}
    for path in generated_files:
        stem, _, scenario = path.stem.rpartition("_")
        groups.setdefault(stem, {})[scenario] = path
    return {
        stem: dict(sorted(
            scenarios.items(),
            key=lambda item: (
                SCENARIO_ORDER.index(item[0])
                if item[0] in SCENARIO_ORDER
                else len(SCENARIO_ORDER),
                item[0],
            ),
        ))
        for stem, scenarios in groups.items()
    }


def scenario_color(scenario: str, index: int):
    """Return the fixed color of a known scenario or a fallback color."""
    return SCENARIO_COLORS.get(scenario, plt.cm.tab10(index % 10))


def projection_colors(scenario: str, index: int) -> dict:
    """Provenance colors with the raw ATB projection colored by scenario."""
    colors = dict(PROVENANCE_COLORS)
    colors["ATB projection (raw)"] = PROJECTION_SCENARIO_COLORS.get(
        scenario, plt.cm.tab10(index % 10)
    )
    return colors


def scenario_marker(scenario: str, index: int) -> str:
    """Return the fixed marker of a known scenario or a fallback marker."""
    return SCENARIO_MARKERS.get(
        scenario, EXTRA_MARKERS[index % len(EXTRA_MARKERS)]
    )


def ordered_union(lists) -> list:
    """Concatenate lists while keeping the first occurrence order."""
    seen = []
    for items in lists:
        seen.extend(item for item in items if item not in seen)
    return seen


def panel_grid(
    title: str,
    metrics: list[str],
    series_count: int = 1,
    dollar_year=None,
    reference_dollar_year=None,
):
    """Create one panel per metric, wrapped into up to three columns.

    Four metrics form a 2x2 grid rather than leaving one orphan panel. Panels
    grow taller with the number of sub-technology series they hold, so
    stacked series separate better.
    """
    columns = 2 if len(metrics) == 4 else min(3, len(metrics))
    rows = math.ceil(len(metrics) / columns)
    row_height = min(4.2 + 1.2 * (series_count - 1), 8.0)
    figure, axes = plt.subplots(
        rows,
        columns,
        figsize=(6.2 * columns, row_height * rows),
        squeeze=False,
        constrained_layout=True,
    )
    figure.suptitle(escape_dollars(title), fontsize=12, fontweight="bold")
    flat = list(axes.flat)
    panels = {}
    for axis, metric in zip(flat, metrics):
        panels[metric] = axis
        label = escape_dollars(
            metric_label(metric, dollar_year, reference_dollar_year)
        )
        axis.set_title(label)
        axis.set_xlabel("Year")
        # A two-dollar-year label is too long for the rotated axis, so the
        # dollar-year note wraps onto a second line there.
        if "; " in label:
            unit, note = label.split("; ", 1)
            axis.set_ylabel(f"{unit})\n{note[:-1]}")
        else:
            axis.set_ylabel(label)
        axis.grid(True, alpha=0.25)
    for axis in flat[len(metrics):]:
        axis.remove()
    return figure, panels


def series_handles(labels: list[str], series_styles: dict) -> list:
    """Legend entries for sub-technology line styles (none for one series)."""
    if labels == ["all"]:
        return []
    return [
        plt.Line2D(
            [0],
            [0],
            color="0.25",
            linewidth=2,
            linestyle=series_styles[label],
            label=label,
        )
        for label in labels
    ]


def output_technology(filename: str, settings: dict) -> str:
    """Return the internal technology key represented by an output filename."""
    matches = []
    for tech, tech_settings in settings["techs"].items():
        filename_root = tech_settings.get("reeds_name", tech)
        if filename.startswith(f"{filename_root}_ATB_"):
            matches.append(tech)
    if len(matches) != 1:
        raise ValueError(
            f"Could not identify one technology for smoothing plot {filename}: "
            f"{matches}"
        )
    return matches[0]


_ATB_START_CACHE: dict = {}


def atb_start_year(
    settings: dict, technology: str, default: int, series_label=None
) -> int:
    """Return the first year ATB publishes one technology.

    ATB starts some technologies later than the release year, and a series is
    historical until its own data begins.
    """
    if not _ATB_START_CACHE:
        flat = raw_file_path(settings["config"], "flat_file")
        if not flat.is_file():
            _ATB_START_CACHE["_missing"] = True
        else:
            raw = pd.read_csv(
                flat,
                usecols=[
                    "technology",
                    "display_name",
                    "core_metric_parameter",
                    "core_metric_variable",
                ],
                low_memory=False,
            )
            # financial parameters span every year even where the cost and
            # performance metrics start later, so only the mapped metrics
            # decide when a series' ATB data begins
            raw = raw.loc[
                raw["core_metric_parameter"].isin(settings["param_names"])
            ]
            for tech, tech_settings in settings["techs"].items():
                names = tech_settings["DisplayName"]
                labels = names if isinstance(names, dict) else {names: None}
                rows = raw.loc[
                    (raw["technology"] == tech_settings["Technology"])
                    & (raw["display_name"].isin(list(labels)))
                ]
                if rows.empty:
                    continue
                starts = {None: int(rows["core_metric_variable"].min())}
                for display_name, label in labels.items():
                    if label is None:
                        continue
                    series = rows.loc[rows["display_name"] == display_name]
                    if not series.empty:
                        starts[label] = int(series["core_metric_variable"].min())
                _ATB_START_CACHE[tech] = starts
    starts = _ATB_START_CACHE.get(technology)
    if not isinstance(starts, dict):
        return default
    return starts.get(series_label, starts.get(None, default))


def smoothing_provenance(settings: dict, generated_path: Path) -> dict:
    """Describe the source/treatment rules for one smoothing comparison."""
    technology = output_technology(generated_path.name, settings)
    smoothing = settings["config"]["processing"].get("smooth_cost_curves", {})
    technology_settings = smoothing.get("technologies", {}).get(technology, {})
    if not isinstance(technology_settings, dict):
        technology_settings = {}
    historical_data = technology_settings.get(
        "historical_data", {}
    )
    from historical_data import load_history
    observed = load_history(settings, 'real')
    observed = observed.loc[observed.scope.eq('reeds') & observed.technology.eq(technology)]
    observed_series = {}
    boundary = atb_start_year(settings, technology, int(smoothing.get('projection_start_year', 2022)))
    default_boundary = int(smoothing.get('projection_start_year', 2022))
    identity = next((c for c in ('i', 'type', 'turbine')
                     if c in settings['techs'][technology]['indexcols']), None)
    for (metric, series), selected in observed.groupby(['metric', 'series']):
        observed_series.setdefault(metric, []).append({
            'years': set(selected.loc[selected.source_type.ne('filled'), 'year']),
            'target_column': identity if series != '*' else None,
            'targets': {series} if series != '*' else None,
        })
    return {
        "technology": technology,
        "projection_start_year": boundary,
        "historical_data": historical_data,
        "fill_atbstartyear2atbyear_with_real": technology_settings.get(
            'fill_atbstartyear2atbyear_with_real', smoothing.get('fill_atbstartyear2atbyear_with_real', False)),
        "observed_series": observed_series,
        "history_class_column": (
            settings["techs"].get(technology, {}).get("history_class_column")
        ),
        "settings": settings,
        "default_boundary": default_boundary,
    }


def resolve_historical_mode(
    metric: str,
    final_group: pd.DataFrame,
    provenance: dict,
) -> str:
    """Return one metric's history mode for the plotted sub-technology class.

    A metric may give one mode per class, so the mode depends on which class
    this series plots rather than on the technology alone.
    """
    historical_data = provenance["historical_data"]
    if metric not in historical_data:
        raise KeyError(
            f"{provenance['technology']} has no historical_data entry for "
            f"metric {metric!r}."
        )
    configured = historical_data[metric]
    if not isinstance(configured, dict):
        return configured
    class_column = provenance["history_class_column"]
    if not class_column or class_column not in final_group.columns:
        raise KeyError(
            f"{provenance['technology']}.{metric} gives one history mode per "
            f"sub-technology class, but the plotted series has no "
            f"{class_column!r} column identifying which class it is."
        )
    classes = set(final_group[class_column].dropna().unique())
    modes = {configured[c] for c in classes if c in configured}
    if len(modes) != 1:
        raise KeyError(
            f"{provenance['technology']}.{metric} resolves to {sorted(modes)} "
            f"for plotted classes {sorted(classes)}; one series must plot one "
            "class so it has a single history mode."
        )
    return modes.pop()


def is_scaled_csp_history(final_group: pd.DataFrame, metric: str, provenance: dict) -> bool:
    """Only csp2 uses the project reference directly; other types are scaled."""
    return (provenance['technology'] == 'csp' and metric == 'capcost'
            and 'type' in final_group and final_group['type'].ne('csp2').all())


def is_observed_history_point(
    final_group: pd.DataFrame,
    metric: str,
    year: int,
    provenance: dict,
) -> bool:
    """Return whether one plotted point was populated from observed history."""
    if provenance['technology'] == 'battery' and metric in ('capcost', 'capcost_energy'):
        return False  # Only the total is observed; both components are estimated.
    if is_scaled_csp_history(final_group, metric, provenance):
        return False
    for series in provenance["observed_series"].get(metric, []):
        if year not in series["years"]:
            continue
        target_column = series["target_column"]
        if target_column is None:
            return True
        if target_column not in final_group.columns:
            continue
        group_targets = set(final_group[target_column].dropna().unique())
        if group_targets & series["targets"]:
            return True
    return False


def is_real_history_target(
    final_group: pd.DataFrame,
    metric: str,
    provenance: dict,
) -> bool:
    """Return whether a plotted series is covered by a selected real mapping."""
    for series in provenance["observed_series"].get(metric, []):
        target_column = series["target_column"]
        if target_column is None:
            return True
        if target_column not in final_group.columns:
            continue
        group_targets = set(final_group[target_column].dropna().unique())
        if group_targets & series["targets"]:
            return True
    return False


def series_boundary(final_group: pd.DataFrame, provenance: dict) -> int:
    """Return the first ATB year for the series being plotted."""
    label = None
    class_column = provenance["history_class_column"]
    if class_column and class_column in final_group.columns:
        values = set(final_group[class_column].dropna().unique())
        if len(values) == 1:
            label = values.pop()
    return atb_start_year(
        provenance["settings"],
        provenance["technology"],
        provenance["default_boundary"],
        label,
    )


def provenance_categories(
    final_group: pd.DataFrame,
    baseline_group: pd.DataFrame | None,
    metric: str,
    provenance: dict,
) -> list[str]:
    """Label each final point by its exclusive data source or treatment."""
    years = pd.to_numeric(final_group["t"], errors="raise").astype(int)
    final_values = pd.to_numeric(final_group[metric], errors="coerce")
    if baseline_group is None:
        baseline_values = pd.Series(np.nan, index=final_group.index)
    else:
        baseline_by_year = (
            baseline_group[["t", metric]]
            .drop_duplicates("t", keep="last")
            .set_index("t")[metric]
        )
        baseline_values = years.map(baseline_by_year)
        baseline_values.index = final_group.index
        baseline_values = pd.to_numeric(baseline_values, errors="coerce")

    changed = ~np.isclose(
        final_values,
        baseline_values,
        rtol=1e-9,
        atol=1e-9,
        equal_nan=True,
    )
    boundary = series_boundary(final_group, provenance)
    historical_mode = resolve_historical_mode(metric, final_group, provenance)
    real_years = set()
    if historical_mode == 'real':
        from historical_data import select_history
        identity = next((c for c in ('i', 'type', 'turbine') if c in final_group), None)
        series = str(final_group[identity].iloc[0]) if identity else '*'
        if provenance['technology'] == 'wind-ons':
            series = '*'
        selected = select_history(provenance['settings'], 'real', provenance['technology'], series, metric)
        anchors = selected.loc[selected.source_type.isin(['real', 'calculated']), 'year']
        real_years = set(anchors)
        # Match the formatter: years between anchors inside the overlap window
        # carry the straight-line fill rather than ATB.
        window = anchors[anchors.between(boundary, provenance['settings']['atbyear'])]
        if not window.empty:
            real_years |= set(selected.loc[selected.year.between(boundary, window.max()), 'year'])
    final_categories = []
    for year, was_changed in zip(years, changed):
        real_overlap = (historical_mode == 'real'
                        and provenance['fill_atbstartyear2atbyear_with_real']
                        and boundary <= year <= provenance['settings']['atbyear']
                        and year in real_years)
        if year < boundary or real_overlap:
            if historical_mode == "manual":
                final_categories.append("Manual history")
            elif historical_mode == "broadcast":
                final_categories.append("Broadcast history")
            elif historical_mode == "unavailable":
                final_categories.append("Unavailable placeholder")
            elif historical_mode == "indexed":
                index_years = set().union(*(
                    series['years'] for series in provenance['observed_series'].get(f'{metric}_index', [])
                ))
                final_categories.append(
                    "Indexed anchor (observed O&M)" if int(year) in index_years
                    else "Indexed history (ATB level, observed trend)"
                )
            elif historical_mode == "real":
                if not is_real_history_target(final_group, metric, provenance):
                    raise KeyError(
                        f"{provenance['technology']}.{metric} selects real "
                        "history, but this plotted series has no real mapping."
                    )
                if (provenance['technology'] == 'nuclear' and metric == 'capcost'
                        and is_observed_history_point(final_group, metric, int(year), provenance)):
                    final_categories.append('Calculated project history')
                    continue
                if is_scaled_csp_history(final_group, metric, provenance) and any(
                    int(year) in series['years']
                    for series in provenance['observed_series'].get(metric, [])
                ):
                    final_categories.append('Scaled real history')
                    continue
                if (provenance['technology'] == 'battery'
                        and metric in ('capcost', 'capcost_energy') and any(
                            int(year) in series['years']
                            for series in provenance['observed_series'].get(metric, [])
                        )):
                    final_categories.append('Split real history')
                    continue
                final_categories.append(
                    "Observed history (real)"
                    if is_observed_history_point(
                        final_group, metric, int(year), provenance
                    )
                    else "Filled real history"
                )
            else:
                raise KeyError(
                    f"Unknown historical mode for "
                    f"{provenance['technology']}.{metric}: {historical_mode!r}"
                )
        else:
            final_categories.append(
                "ATB projection (smoothed)"
                if was_changed
                else "ATB projection (raw)"
            )
    return final_categories


def input_point_categories(
    final_group: pd.DataFrame,
    metric: str,
    provenance: dict,
    final_categories: list[str],
) -> list[str | None]:
    """Show selected source anchors; omit filled and broadcast points.

    ATB markers appear only in years whose final value is an ATB projection,
    so years inside the ATB range that real history replaced get no ATB dot.
    """
    years = pd.to_numeric(final_group["t"], errors="raise").astype(int)
    historical_mode = resolve_historical_mode(metric, final_group, provenance)
    categories = []
    for year, category in zip(years, final_categories):
        if category in ('Observed history (real)', 'Split real history', 'Scaled real history',
                        'Calculated project history', 'Indexed anchor (observed O&M)'):
            categories.append(category)
        elif category in PROJECTION_CATEGORIES:
            categories.append("ATB projection (raw)")
        elif historical_mode == "manual":
            categories.append("Manual history")
        elif historical_mode == "real" and is_observed_history_point(
            final_group, metric, int(year), provenance
        ):
            categories.append("Observed history (real)")
        else:
            categories.append(None)
    return categories


def plot_colored_segments(
    axis,
    years: np.ndarray,
    values: np.ndarray,
    categories: list[str],
    linestyle,
    colors: dict = PROVENANCE_COLORS,
) -> None:
    """Color each interval by the year it ends in.

    The step from the last historical year into the first projection year is
    therefore drawn in the projection color.
    """
    if not len(years):
        return
    if len(years) == 1:
        axis.plot(
            years,
            values,
            color=colors[categories[0]],
            linestyle=linestyle,
            linewidth=2.0,
            alpha=0.95,
            zorder=2,
        )
        return

    interval_categories = list(categories[1:])
    run_start = 0
    for position in range(1, len(interval_categories) + 1):
        if (
            position < len(interval_categories)
            and interval_categories[position] == interval_categories[run_start]
        ):
            continue
        axis.plot(
            years[run_start:position + 1],
            values[run_start:position + 1],
            color=colors[interval_categories[run_start]],
            linestyle=linestyle,
            linewidth=2.0,
            alpha=0.95,
            zorder=2,
        )
        run_start = position


def plot_file_with_provenance(
    stem: str,
    scenario_paths: dict[str, tuple[Path, Path]],
    plot_dir: Path,
    settings: dict,
) -> bool:
    """Plot a smoothing comparison with color encoding source and treatment.

    All scenarios and sub-technology series of one technology share each
    panel. History is identical across scenarios and overlaps; the
    projections fan out and their input markers take one shape per scenario,
    while history keeps round dots. Line style identifies the series.
    """
    frames = {
        scenario: (
            normalize_frame(pd.read_csv(generated_path)),
            normalize_frame(pd.read_csv(baseline_path)),
        )
        for scenario, (generated_path, baseline_path) in scenario_paths.items()
    }
    metrics = ordered_union(
        metric_columns(generated, baseline) for generated, baseline in frames.values()
    )
    if not metrics or any(
        "t" not in generated.columns or "t" not in baseline.columns
        for generated, baseline in frames.values()
    ):
        return False

    first_generated_path = next(iter(scenario_paths.values()))[0]
    provenance = smoothing_provenance(settings, first_generated_path)
    identifiers = sorted(set().union(*(
        set(series_columns(generated)) | set(series_columns(baseline))
        for generated, baseline in frames.values()
    )))
    scenario_groups_ = {
        scenario: (
            grouped_series(generated, identifiers),
            dict(grouped_series(baseline, identifiers)),
        )
        for scenario, (generated, baseline) in frames.items()
    }
    labels = sorted(set().union(*(
        {label for label, _ in generated_groups} | set(baseline_groups)
        for generated_groups, baseline_groups in scenario_groups_.values()
    )))
    scenario_markers = {
        scenario: scenario_marker(scenario, index)
        for index, scenario in enumerate(scenario_paths)
    }
    series_styles = {
        label: SERIES_LINESTYLES[index % len(SERIES_LINESTYLES)]
        for index, label in enumerate(labels)
    }

    dollar_year = settings["dollaryear"]
    figure, panels = panel_grid(
        f"{stem}: final output vs. input data ({dollar_year}$)",
        metrics,
        len(labels),
        dollar_year,
    )

    present_categories = set()
    points_present = False
    boundaries = set()
    for index, (scenario, (generated_groups, baseline_groups)) in enumerate(
        scenario_groups_.items()
    ):
        marker = scenario_markers[scenario]
        colors = projection_colors(scenario, index)
        for label, final_group in generated_groups:
            boundaries.add(series_boundary(final_group, provenance))
            baseline_group = baseline_groups.get(label)
            for metric in metrics:
                axis = panels[metric]
                final_categories = provenance_categories(
                    final_group,
                    baseline_group,
                    metric,
                    provenance,
                )
                present_categories.update(final_categories)
                years = final_group["t"].to_numpy()
                values = final_group[metric].to_numpy()
                plot_colored_segments(
                    axis,
                    years,
                    values,
                    final_categories,
                    series_styles[label],
                    colors,
                )
                if baseline_group is None:
                    continue
                point_categories = input_point_categories(
                    final_group,
                    metric,
                    provenance,
                    final_categories,
                )
                baseline_by_year = (
                    baseline_group[["t", metric]]
                    .drop_duplicates("t", keep="last")
                    .set_index("t")[metric]
                )
                point_years = pd.to_numeric(
                    final_group["t"], errors="raise"
                ).to_numpy()
                point_values = pd.to_numeric(
                    pd.Series(point_years).map(baseline_by_year),
                    errors="coerce",
                ).to_numpy()
                real_points = np.isin(point_categories, [
                    'Observed history (real)', 'Split real history', 'Scaled real history',
                    'Calculated project history', 'Indexed anchor (observed O&M)',
                ])
                point_values[real_points] = values[real_points]
                for category in PROVENANCE_COLORS:
                    point_mask = np.asarray([
                        item == category for item in point_categories
                    ]) & np.isfinite(point_values)
                    if not point_mask.any():
                        continue
                    points_present = True
                    present_categories.add(category)
                    axis.scatter(
                        point_years[point_mask],
                        point_values[point_mask],
                        s=26 if category in PROJECTION_CATEGORIES else 18,
                        marker=(
                            marker
                            if category in PROJECTION_CATEGORIES
                            else HISTORY_MARKER
                        ),
                        facecolors=colors[category],
                        edgecolors="none",
                        alpha=0.95,
                        zorder=3,
                    )
    # One line per distinct series start: floating offshore begins in
    # 2030 while fixed begins in 2022, and a single line would mislabel one.
    for axis in panels.values():
        for start_year in sorted(boundaries):
            axis.axvline(
                start_year,
                color="0.35",
                linestyle=":",
                linewidth=1.0,
                alpha=0.7,
            )

    category_handles = [
        plt.Line2D(
            [0],
            [0],
            color=color,
            linewidth=2.5,
            label=category,
        )
        for category, color in PROVENANCE_COLORS.items()
        if category in present_categories and category != "ATB projection (raw)"
    ]
    point_handles = []
    if points_present:
        point_handles.append(
            plt.Line2D(
                [0],
                [0],
                color="0.25",
                linewidth=0,
                marker=HISTORY_MARKER,
                markerfacecolor="0.25",
                markeredgewidth=0,
                label="Historical input values",
            )
        )
    scenario_handles = [
        plt.Line2D(
            [0],
            [0],
            color=projection_colors(scenario, index)["ATB projection (raw)"],
            linewidth=2.5,
            marker=scenario_markers[scenario],
            markeredgewidth=0,
            label=f"ATB projection ({scenario})",
        )
        for index, scenario in enumerate(scenario_paths)
    ]
    handles = (
        category_handles
        + point_handles
        + scenario_handles
        + series_handles(labels, series_styles)
    )
    figure.legend(
        handles=handles,
        loc="outside lower center",
        ncol=min(4, len(handles)),
        fontsize=8,
        frameon=False,
        handlelength=5.0,
        columnspacing=2.0,
    )
    figure.savefig(
        plot_dir / f"{stem}.png",
        dpi=160,
        bbox_inches="tight",
    )
    plt.close(figure)
    return True


def plot_file(
    stem: str,
    scenario_paths: dict[str, tuple[Path, Path]],
    plot_dir: Path,
    title: str,
    dollar_year=None,
    reference_dollar_year=None,
    solid_label: str = "Generated",
    dashed_label: str = "ReEDS",
) -> bool:
    """Plot all shared numeric metrics for one technology.

    Scenarios and sub-technology series share each panel: color is the
    scenario, line style is the series. Generated values are thin crisp lines
    drawn over the ReEDS reference, which is a thick translucent line, so a
    match shows as a thin line centred in a halo.
    """
    frames = {
        scenario: (
            normalize_frame(pd.read_csv(generated_path)),
            normalize_frame(pd.read_csv(reeds_path)),
        )
        for scenario, (generated_path, reeds_path) in scenario_paths.items()
    }
    metrics = ordered_union(
        metric_columns(generated, reeds) for generated, reeds in frames.values()
    )
    if not metrics or any(
        "t" not in generated.columns or "t" not in reeds.columns
        for generated, reeds in frames.values()
    ):
        return False

    identifiers = sorted(set().union(*(
        set(series_columns(generated)) | set(series_columns(reeds))
        for generated, reeds in frames.values()
    )))
    scenario_groups_ = {
        scenario: (
            grouped_series(generated, identifiers),
            grouped_series(reeds, identifiers),
        )
        for scenario, (generated, reeds) in frames.items()
    }
    labels = sorted(set().union(*(
        {label for label, _ in generated_groups + reeds_groups}
        for generated_groups, reeds_groups in scenario_groups_.values()
    )))
    colors = {
        scenario: scenario_color(scenario, index)
        for index, scenario in enumerate(scenario_paths)
    }
    series_styles = {
        label: SERIES_LINESTYLES[index % len(SERIES_LINESTYLES)]
        for index, label in enumerate(labels)
    }

    figure, panels = panel_grid(
        title, metrics, len(labels), dollar_year, reference_dollar_year
    )

    for scenario, (generated_groups, reeds_groups) in scenario_groups_.items():
        for metric in metrics:
            for label, group in reeds_groups:
                if metric not in group.columns:
                    continue
                panels[metric].plot(
                    group["t"],
                    group[metric],
                    color=colors[scenario],
                    linestyle=series_styles[label],
                    linewidth=5.0,
                    alpha=0.3,
                    zorder=1,
                )
            for label, group in generated_groups:
                if metric not in group.columns:
                    continue
                panels[metric].plot(
                    group["t"],
                    group[metric],
                    color=colors[scenario],
                    linestyle=series_styles[label],
                    linewidth=1.4,
                    alpha=0.95,
                    zorder=2,
                )

    source_handles = [
        plt.Line2D(
            [0], [0], color="0.25", linewidth=1.4, linestyle="-", label=solid_label
        ),
        plt.Line2D(
            [0],
            [0],
            color="0.25",
            linewidth=5,
            alpha=0.3,
            linestyle="-",
            label=dashed_label,
        ),
    ]
    scenario_handles = [
        plt.Line2D([0], [0], color=colors[scenario], linewidth=2, label=scenario)
        for scenario in scenario_paths
    ]
    handles = source_handles + scenario_handles + series_handles(labels, series_styles)
    figure.legend(
        handles=handles,
        loc="outside lower center",
        ncol=min(6, len(handles)),
        fontsize=8,
        frameon=False,
        handlelength=5.0,
        columnspacing=2.0,
    )
    figure.savefig(
        plot_dir / f"{stem}.png",
        dpi=160,
        bbox_inches="tight",
    )
    plt.close(figure)
    return True


def plot_overview(summary: pd.DataFrame, plot_dir: Path, atb_year: int) -> None:
    """Plot file-level validation status and difference counts."""
    plot_data = summary.sort_values(
        ["status", "changed_cells"], ascending=[True, True]
    ).reset_index(drop=True)
    labels = plot_data["generated_file"].str.replace(
        "_ATB_2024_", " | ", regex=False
    )
    y = np.arange(len(plot_data))

    figure, (axis_status, axis_counts) = plt.subplots(
        1,
        2,
        figsize=(18, 14),
        gridspec_kw={"width_ratios": [0.8, 2.2]},
        constrained_layout=True,
    )
    figure.suptitle(
        f"ATB {atb_year} output comparison overview",
        fontsize=16,
        fontweight="bold",
    )
    status_colors = {
        "MATCH": "#2a9d8f",
        "VALUE_DIFFERENCE": "#e9c46a",
        "STRUCTURE_DIFFERENCE": "#e76f51",
        "MISSING_REEDS_FILE": "#9b2226",
    }
    for index, row in plot_data.iterrows():
        axis_status.barh(
            index, 1, color=status_colors.get(row["status"], "0.6")
        )
        axis_status.text(
            0.5,
            index,
            row["status"].replace("_", " "),
            ha="center",
            va="center",
            fontsize=8,
        )
    axis_status.set_yticks(y, labels)
    axis_status.set_xlim(0, 1)
    axis_status.set_xticks([])
    axis_status.set_title("Validation status")
    axis_status.invert_yaxis()

    axis_counts.barh(
        y,
        plot_data["changed_cells"],
        label="Changed cells",
        color="#457b9d",
    )
    axis_counts.barh(
        y,
        plot_data["rows_only_reeds"],
        left=plot_data["changed_cells"],
        label="Rows only in ReEDS",
        color="#e76f51",
    )
    axis_counts.set_yticks([])
    axis_counts.set_xlabel("Difference count")
    axis_counts.set_title("Changed cells and missing generated rows")
    axis_counts.grid(True, axis="x", alpha=0.25)
    axis_counts.invert_yaxis()
    axis_counts.legend(frameon=False, loc="lower right")
    figure.savefig(plot_dir / "comparison_overview.png", dpi=160)
    plt.close(figure)


def write_plots(
    summary: pd.DataFrame | None,
    generated_files: list[Path],
    reeds_dir: Path,
    plot_dir: Path,
    atb_year: int,
    reeds_year: int | None = None,
    solid_label: str = "Generated",
    dashed_label: str = "ReEDS",
    include_overview: bool = True,
    provenance_settings: dict | None = None,
    dollar_year=None,
) -> int:
    """Generate the overview and one technology-level plot per file group.

    All scenario files of a technology are drawn together in one figure. The
    figure title names both sides of the comparison and their dollar years.
    """
    reference_dollar_years = reeds_dollar_years(reeds_dir)
    plot_dir.mkdir(parents=True, exist_ok=True)
    for old_plot in plot_dir.glob("*.png"):
        old_plot.unlink()
    if include_overview:
        if summary is None:
            raise ValueError("A comparison summary is required for the overview plot.")
        plot_overview(summary, plot_dir, atb_year)

    plotted = 0
    skipped = []
    for stem, scenarios in scenario_groups(generated_files).items():
        scenario_paths = {}
        for scenario, generated_path in scenarios.items():
            reeds_path = reeds_dir / reeds_filename(generated_path.name, atb_year, reeds_year)
            if reeds_path.exists():
                scenario_paths[scenario] = (generated_path, reeds_path)
        if not scenario_paths:
            continue
        if provenance_settings is None:
            reference_years = {
                reference_dollar_years.get(reeds_path.stem)
                for _, reeds_path in scenario_paths.values()
            } - {None}
            reference_dollar_year = (
                reference_years.pop() if len(reference_years) == 1 else None
            )
            reference = dashed_label
            if reference_dollar_year is not None:
                reference = f"{dashed_label} ({reference_dollar_year}$)"
            generated = solid_label
            if dollar_year is not None:
                generated = f"{solid_label} ({dollar_year}$)"
            wrote_plot = plot_file(
                stem,
                scenario_paths,
                plot_dir,
                f"{stem}: {generated} vs. {reference}",
                dollar_year,
                reference_dollar_year,
                solid_label=generated,
                dashed_label=reference,
            )
        else:
            wrote_plot = plot_file_with_provenance(
                stem,
                scenario_paths,
                plot_dir,
                provenance_settings,
            )
        if wrote_plot:
            plotted += 1
        else:
            skipped.append(stem)
    if skipped:
        raise RuntimeError(
            "Could not plot generated/ReEDS file pairs because they have no "
            f"shared year-based numeric metrics: {skipped}"
        )
    return plotted


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare generated ATB outputs with current ReEDS inputs."
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--generated-dir", type=Path)
    parser.add_argument(
        "--unsmoothed-dir",
        type=Path,
        help="temporary pre-smoothing outputs supplied by the full pipeline",
    )
    parser.add_argument("--reeds-dir", type=Path)
    parser.add_argument("--plot-dir", type=Path, default=DEFAULT_PLOT_DIR)
    parser.add_argument(
        "--absolute-tolerance",
        type=float,
        default=2e-4,
        help=(
            "Absolute numeric tolerance (default: 2e-4, reflecting the "
            "four-decimal precision of some raw ATB values)"
        ),
    )
    parser.add_argument("--relative-tolerance", type=float, default=1e-9)
    args = parser.parse_args()

    settings = load_processing_settings(args.config)
    smoothing = settings['config']['processing'].get('smooth_cost_curves', {})
    baseline_dir = args.unsmoothed_dir.resolve() if args.unsmoothed_dir else None
    if args.generated_dir:
        generated_dir = args.generated_dir.resolve()
    else:
        generated_dir = Path(settings['output_dir']).resolve()
    reeds_dir = (
        args.reeds_dir.resolve()
        if args.reeds_dir
        else Path(settings['reedspath']).resolve() / "inputs" / "plant_characteristics"
    )
    plot_dir = args.plot_dir.resolve()
    atb_year = int(settings['atbyear'])
    reeds_year = int(settings['config']['processing'].get('reeds_atb_year', atb_year))
    if reeds_year != atb_year:
        print(f"Comparing ATB {atb_year} outputs against ReEDS ATB {reeds_year} files.")

    generated_files = sorted(generated_dir.glob(f"*_ATB_{atb_year}_*.csv"))
    if not generated_files:
        raise FileNotFoundError(
            f"No ATB {atb_year} outputs found in {generated_dir}"
        )
    if not reeds_dir.is_dir():
        raise FileNotFoundError(f"ReEDS plant-characteristics directory not found: {reeds_dir}")

    results = []
    for generated_path in generated_files:
        reeds_path = reeds_dir / reeds_filename(generated_path.name, atb_year, reeds_year)
        result = compare_file(
            generated_path,
            reeds_path,
            args.absolute_tolerance,
            args.relative_tolerance,
        )
        results.append(result)
        print(
            f"{result['status']:>20}  {generated_path.name} -> {reeds_path.name}"
        )

    summary = pd.DataFrame(results)
    expected_reeds_names = {reeds_filename(path.name, atb_year, reeds_year) for path in generated_files}
    reeds_only_files = sorted(
        path.name
        for path in reeds_dir.glob(f"*_ATB_{reeds_year}_*.csv")
        if path.name not in expected_reeds_names
    )
    plotted = write_plots(
        summary,
        generated_files,
        reeds_dir,
        plot_dir,
        atb_year,
        reeds_year=reeds_year,
        solid_label=f"Generated ATB {atb_year}",
        dashed_label=f"ReEDS ATB {reeds_year}",
        dollar_year=int(settings["dollaryear"]),
    )
    print(f"\nWrote overview and {plotted} technology-level plots to {plot_dir}")
    print(summary["status"].value_counts().to_string())
    print(
        f"ReEDS files without a generated counterpart: {len(reeds_only_files)}"
    )
    for filename in reeds_only_files:
        print(f"  {filename}")

    if smoothing.get('enabled', False) and baseline_dir is not None:
        smoothed_dir = Path(settings['output_dir']).resolve()
        smoothed_files = sorted(
            smoothed_dir.glob(f"*_ATB_{atb_year}_*.csv")
        )
        smoothing_plot_setting = settings['config']['plotting'].get(
            'smoothing_comparison_directory',
            'comparison/plot_component',
        )
        smoothing_plot_dir = resolve_atb_path(smoothing_plot_setting)
        smoothing_plotted = write_plots(
            None,
            smoothed_files,
            baseline_dir,
            smoothing_plot_dir,
            atb_year,
            solid_label="After smoothing",
            dashed_label="Before smoothing",
            include_overview=False,
            provenance_settings=settings,
        )
        print(
            f"Wrote {smoothing_plotted} before/after smoothing plots to "
            f"{smoothing_plot_dir}"
        )


if __name__ == "__main__":
    main()
