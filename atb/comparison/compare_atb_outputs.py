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

from atb_config import DEFAULT_CONFIG_PATH, load_processing_settings


DEFAULT_PLOT_DIR = Path(__file__).resolve().parent / "plots"

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

METRIC_LABELS = {
    "capcost": "Capital cost ($/kW)",
    "capcost_energy": "Energy capital cost ($/kWh)",
    "fom": "Fixed O&M ($/kW-year)",
    "fom_energy": "Energy fixed O&M ($/kWh-year)",
    "vom": "Variable O&M ($/MWh)",
    "heatrate": "Heat rate (MMBtu/MWh)",
    "cf_improvement": "Capacity-factor multiplier",
    "rte": "Round-trip efficiency",
    "rsc_mult": "Resource-supply-curve multiplier",
}


def resolve_atb_config_path(value: str | Path) -> Path:
    """Resolve a configured path relative to the ATB workflow directory."""
    path = Path(value)
    return path.resolve() if path.is_absolute() else (ATB_DIR / path).resolve()


def reeds_filename(generated_name: str) -> str:
    """Map a generated filename to its ReEDS filename."""
    for generated_prefix, reeds_prefix in FILENAME_PREFIX_MAP.items():
        if generated_name.startswith(generated_prefix):
            return reeds_prefix + generated_name[len(generated_prefix):]
    return generated_name


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


def plot_file(
    generated_path: Path,
    reeds_path: Path,
    plot_dir: Path,
    solid_label: str = "Generated",
    dashed_label: str = "ReEDS",
    reference_linestyle: str = "--",
    reference_marker: str | None = None,
) -> bool:
    """Plot all shared numeric metrics for one generated/ReEDS file pair."""
    generated = normalize_frame(pd.read_csv(generated_path))
    reeds = normalize_frame(pd.read_csv(reeds_path))
    metrics = metric_columns(generated, reeds)
    if not metrics or "t" not in generated.columns or "t" not in reeds.columns:
        return False

    identifiers = sorted(set(series_columns(generated)) | set(series_columns(reeds)))
    generated_groups = grouped_series(generated, identifiers)
    reeds_groups = grouped_series(reeds, identifiers)
    labels = sorted({label for label, _ in generated_groups + reeds_groups})
    colors = {label: plt.cm.tab10(index % 10) for index, label in enumerate(labels)}

    columns = min(3, len(metrics))
    rows = math.ceil(len(metrics) / columns)
    figure, axes = plt.subplots(
        rows,
        columns,
        figsize=(5.4 * columns, 3.7 * rows),
        squeeze=False,
        constrained_layout=True,
    )
    figure.suptitle(
        generated_path.stem,
        y=1.03,
        fontsize=12,
        fontweight="bold",
    )

    for axis, metric in zip(axes.flat, metrics):
        for label, group in reeds_groups:
            axis.plot(
                group["t"],
                group[metric],
                color=colors[label],
                linestyle=reference_linestyle,
                marker=reference_marker,
                markersize=3.2 if reference_marker else None,
                linewidth=2.0,
                alpha=0.9,
            )
        for label, group in generated_groups:
            axis.plot(
                group["t"],
                group[metric],
                color=colors[label],
                linestyle="-",
                linewidth=1.6,
                alpha=0.9,
            )
        axis.set_title(METRIC_LABELS.get(metric, metric))
        axis.set_xlabel("Year")
        axis.set_ylabel(METRIC_LABELS.get(metric, metric))
        axis.grid(True, alpha=0.25)

    for axis in axes.flat[len(metrics):]:
        axis.remove()

    source_handles = [
        plt.Line2D(
            [0], [0], color="0.25", linewidth=2, linestyle="-", label=solid_label
        ),
        plt.Line2D(
            [0],
            [0],
            color="0.25",
            linewidth=2,
            linestyle=reference_linestyle,
            marker=reference_marker,
            markersize=4 if reference_marker else None,
            label=dashed_label,
        ),
    ]
    series_handles = [
        plt.Line2D([0], [0], color=colors[label], linewidth=2, label=label)
        for label in labels
    ]
    figure.legend(
        handles=source_handles + series_handles,
        loc="outside lower center",
        ncol=min(4, len(source_handles + series_handles)),
        fontsize=8,
        frameon=False,
    )
    figure.savefig(
        plot_dir / f"{generated_path.stem}.png",
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
    solid_label: str = "Generated",
    dashed_label: str = "ReEDS",
    include_overview: bool = True,
    reference_linestyle: str = "--",
    reference_marker: str | None = None,
) -> int:
    """Generate the overview and all available file-level comparison plots."""
    plot_dir.mkdir(parents=True, exist_ok=True)
    for old_plot in plot_dir.glob("*.png"):
        old_plot.unlink()
    if include_overview:
        if summary is None:
            raise ValueError("A comparison summary is required for the overview plot.")
        plot_overview(summary, plot_dir, atb_year)

    plotted = 0
    skipped = []
    for generated_path in generated_files:
        reeds_path = reeds_dir / reeds_filename(generated_path.name)
        if reeds_path.exists():
            wrote_plot = plot_file(
                generated_path,
                reeds_path,
                plot_dir,
                solid_label=solid_label,
                dashed_label=dashed_label,
                reference_linestyle=reference_linestyle,
                reference_marker=reference_marker,
            )
            if wrote_plot:
                plotted += 1
            else:
                skipped.append(generated_path.name)
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
    elif baseline_dir is not None:
        generated_dir = baseline_dir
    else:
        generated_dir = Path(settings['output_dir']).resolve()
    reeds_dir = (
        args.reeds_dir.resolve()
        if args.reeds_dir
        else Path(settings['reedspath']).resolve() / "inputs" / "plant_characteristics"
    )
    plot_dir = args.plot_dir.resolve()
    atb_year = int(settings['atbyear'])

    generated_files = sorted(generated_dir.glob(f"*_ATB_{atb_year}_*.csv"))
    if not generated_files:
        raise FileNotFoundError(
            f"No ATB {atb_year} outputs found in {generated_dir}"
        )
    if not reeds_dir.is_dir():
        raise FileNotFoundError(f"ReEDS plant-characteristics directory not found: {reeds_dir}")

    results = []
    for generated_path in generated_files:
        reeds_path = reeds_dir / reeds_filename(generated_path.name)
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
    expected_reeds_names = {reeds_filename(path.name) for path in generated_files}
    reeds_only_files = sorted(
        path.name
        for path in reeds_dir.glob(f"*_ATB_{atb_year}_*.csv")
        if path.name not in expected_reeds_names
    )
    plotted = write_plots(
        summary,
        generated_files,
        reeds_dir,
        plot_dir,
        atb_year,
        solid_label=(
            "Before smoothing (generated)"
            if baseline_dir is not None
            else "Generated"
        ),
        dashed_label="ReEDS",
    )
    print(f"\nWrote overview and {plotted} file-level plots to {plot_dir}")
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
            'figures/smoothing_comparison',
        )
        smoothing_plot_dir = resolve_atb_config_path(smoothing_plot_setting)
        smoothing_plotted = write_plots(
            None,
            smoothed_files,
            baseline_dir,
            smoothing_plot_dir,
            atb_year,
            solid_label="After smoothing",
            dashed_label="Before smoothing",
            include_overview=False,
            reference_linestyle="None",
            reference_marker="o",
        )
        print(
            f"Wrote {smoothing_plotted} before/after smoothing plots to "
            f"{smoothing_plot_dir}"
        )


if __name__ == "__main__":
    main()
