"""
Unified visualization and validation for the NG regression pipeline.

Consolidates three prior scripts into one entry point:
  - Beta raw-data scatter grid  (was beta_raw_data_visualization.py)
  - Beta/alpha diagnostic plots (was results_visualization.py)
  - Validation: actual vs predicted, parity, alpha comparison (was results_validation.py)

Usage
-----
    python visualization.py --config aeo_pipeline_config.json

Run after:
    1) aeo_beta_regression.py
    2) sync_beta_to_alpha_inputs.py
    3) aeo_alpha_regression.py
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from aeo_functions import (
    CENDIV_CANONICAL,
    load_config,
    normalize_token,
    require,
    resolve_case_insensitive,
    resolve_config_path,
    resolve_path,
    setup_logging,
)

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
except Exception as exc:  # pragma: no cover
    raise RuntimeError("matplotlib is required for visualization.py") from exc

LOGGER = logging.getLogger("visualization")

# ============================================================================
# Constants
# ============================================================================

# Populated at runtime from config["ng"]["cendiv_and_label"] by main().
CENDIV_OUTPUT: dict[str, str] = {}

SCENARIO_COLOR = {
    "reference": "#1f77b4",
    "HOG": "#2ca02c",
    "LOG": "#d62728",
}


# ============================================================================
# Shared helpers
# ============================================================================

def any_to_cendiv(value: str) -> str:
    token = normalize_token(value)
    if token in CENDIV_CANONICAL:
        return CENDIV_CANONICAL[token]
    for cendiv, out_label in CENDIV_OUTPUT.items():
        if token in {normalize_token(cendiv), normalize_token(out_label)}:
            return cendiv
    raise ValueError(f"Unknown region label: {value}")


def cendiv_output_label(cendiv: str) -> str:
    require(cendiv in CENDIV_OUTPUT, f"Unknown cendiv key: {cendiv}")
    return CENDIV_OUTPUT[cendiv]


def _to_region_label(value: Any) -> str:
    """Shorthand for cendiv_output_label(any_to_cendiv(value))."""
    return cendiv_output_label(any_to_cendiv(value))


def region_order_from_config(config: dict[str, Any]) -> list[str]:
    return [_to_region_label(x) for x in config["ng"]["cendiv_and_label"].keys()]


def _zero_first_year(frame: pd.DataFrame, first_model_year: int, *cols: str) -> None:
    """Zero out one or more columns where year == first_model_year (in-place)."""
    mask = frame["year"] == first_model_year
    for col in cols:
        frame.loc[mask, col] = 0.0


def compute_fit_metrics(actual: pd.Series | np.ndarray,
                        predicted: pd.Series | np.ndarray) -> dict[str, float]:
    a = np.asarray(actual, dtype=float)
    p = np.asarray(predicted, dtype=float)
    mask = np.isfinite(a) & np.isfinite(p)
    if not np.any(mask):
        return {"n_obs": 0.0, "mae": float("nan"), "rmse": float("nan"),
                "max_abs": float("nan"), "r2": float("nan")}
    a, p = a[mask], p[mask]
    err = a - p
    ss_tot = float(np.sum(np.square(a - np.mean(a))))
    r2 = 1.0 - float(np.sum(np.square(err))) / ss_tot if ss_tot > 0 else float("nan")
    return {
        "n_obs": float(len(a)),
        "mae": float(np.mean(np.abs(err))),
        "rmse": float(np.sqrt(np.mean(np.square(err)))),
        "max_abs": float(np.max(np.abs(err))),
        "r2": r2,
    }


def summarize_fit(
    frame: pd.DataFrame,
    group_cols: list[str],
    actual_col: str,
    predicted_col: str,
    mae_col: str,
    rmse_col: str,
    max_abs_col: str,
    r2_col: str,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for keys, grp in frame.groupby(group_cols, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = {group_cols[i]: keys[i] for i in range(len(group_cols))}
        m = compute_fit_metrics(grp[actual_col], grp[predicted_col])
        row["n_obs"] = int(m["n_obs"])
        row[mae_col] = m["mae"]
        row[rmse_col] = m["rmse"]
        row[max_abs_col] = m["max_abs"]
        row[r2_col] = m["r2"]
        rows.append(row)
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(group_cols).reset_index(drop=True)


# ============================================================================
# Beta / alpha CSV loaders
# ============================================================================

def load_regional_beta_from_csv(source_path: Path) -> dict[str, float]:
    require(source_path.exists(), f"Missing regional beta file: {source_path}")
    df = pd.read_csv(source_path)
    cendiv_col = next((c for c in df.columns if normalize_token(c).endswith("cendiv")), None)
    value_col = next((c for c in df.columns if normalize_token(c) == "value"), None)
    require(cendiv_col is not None and value_col is not None,
            f"Missing cendiv/value columns in {source_path}")
    df = df[[cendiv_col, value_col]].copy()
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=[value_col])
    return {_to_region_label(str(r)): float(v)
            for r, v in zip(df[cendiv_col], df[value_col])}


def load_national_beta_from_csv(source_path: Path) -> float:
    require(source_path.exists(), f"Missing national beta file: {source_path}")
    df = pd.read_csv(source_path)
    require("beta" in df.columns, f"Missing 'beta' column in {source_path}")
    beta_vals = pd.to_numeric(df["beta"], errors="coerce").dropna()
    require(not beta_vals.empty, f"No numeric beta value found in {source_path}")
    return float(beta_vals.iloc[0])


def load_regional_beta_map(alpha_out_dir: Path, alpha_input_dir: Path) -> tuple[dict[str, float], Path]:
    candidates = [alpha_out_dir / "cd_beta0.csv", alpha_input_dir / "cd_beta0.csv"]
    source_path = next((p for p in candidates if p.exists()), None)
    require(source_path is not None, f"Could not find cd_beta0.csv in: {candidates}")
    return load_regional_beta_from_csv(source_path), source_path


def load_national_beta(alpha_out_dir: Path, alpha_input_dir: Path, beta_out_dir: Path) -> tuple[float, Path]:
    candidates = [
        alpha_out_dir / "national_beta.csv",
        alpha_input_dir / "national_beta.csv",
        beta_out_dir / "national_beta.csv",
    ]
    source_path = next((p for p in candidates if p.exists()), None)
    require(source_path is not None, f"Could not find national_beta.csv in: {candidates}")
    return load_national_beta_from_csv(source_path), source_path


def load_alpha_from_beta_step(beta_out_dir: Path) -> tuple[pd.DataFrame, Path, list[str]]:
    source_path = beta_out_dir / "alpha_from_beta_regression.csv"
    require(source_path.exists(),
            "Missing alpha_from_beta_regression.csv. Re-run aeo_beta_regression.py first.")
    df = pd.read_csv(source_path)
    if "scenario_id" in df.columns:
        need = {"scenario_id", "region", "year", "alpha_2004"}
        require(not (need - set(df.columns)),
                f"Missing columns in {source_path}: {sorted(need - set(df.columns))}")
        out = df[["scenario_id", "region", "year", "alpha_2004"]].copy()
        out["scenario_id"] = out["scenario_id"].astype(str)
        merge_cols = ["scenario_id", "region", "year"]
    else:
        need = {"region", "year", "alpha_2004"}
        require(not (need - set(df.columns)),
                f"Missing columns in {source_path}: {sorted(need - set(df.columns))}")
        out = df[["region", "year", "alpha_2004"]].copy()
        merge_cols = ["region", "year"]

    out["region"] = out["region"].astype(str).map(_to_region_label)
    out["year"] = pd.to_numeric(out["year"], errors="coerce")
    out["alpha_2004"] = pd.to_numeric(out["alpha_2004"], errors="coerce")
    require(not out["year"].isna().any(), f"Non-numeric year in {source_path}")
    require(not out["alpha_2004"].isna().any(), f"Non-numeric alpha_2004 in {source_path}")
    out["year"] = out["year"].astype(int)
    out = out.rename(columns={"alpha_2004": "alpha1"})
    return out[merge_cols + ["alpha1"]], source_path, merge_cols


# ============================================================================
# Scenario discovery helpers
# ============================================================================

def scenario_display_label(suffix: str, scenario_id: str) -> str:
    t = normalize_token(suffix)
    if t.startswith("ref"):
        return "reference"
    if t in {"hog", "highogs"}:
        return "HOG"
    if t in {"log", "lowogs"}:
        return "LOG"
    return scenario_id


def discover_output_scenarios_with_labels(
    alpha_out_dir: Path, aeo_year: int,
) -> list[tuple[str, str, str]]:
    """Returns list of (suffix, scenario_id, display_label)."""
    mapping_path = alpha_out_dir / "raw_aeo_data" / "selected_scenarios_outputs.csv"
    suffix_to_scenario: dict[str, str] = {}
    if mapping_path.exists():
        map_df = pd.read_csv(mapping_path)
        if {"file_suffix", "scenario_id"}.issubset(map_df.columns):
            for row in map_df.itertuples(index=False):
                suffix_to_scenario[str(row.file_suffix)] = str(row.scenario_id)

    pattern = re.compile(rf"^alpha_AEO_{aeo_year}_(.+)\.csv$")
    rows: list[tuple[str, str, str]] = []
    for path in sorted(alpha_out_dir.glob(f"alpha_AEO_{aeo_year}_*.csv")):
        match = pattern.match(path.name)
        if not match:
            continue
        suffix = match.group(1)
        scenario_id = suffix_to_scenario.get(suffix, suffix)
        label = scenario_display_label(suffix, scenario_id)
        rows.append((suffix, scenario_id, label))
    require(rows, f"No alpha scenario files found in {alpha_out_dir}")
    order_index = {"reference": 0, "HOG": 1, "LOG": 2}
    rows.sort(key=lambda x: (order_index.get(x[2], 99), x[2], x[1]))
    return rows


def discover_output_scenarios(alpha_out_dir: Path, aeo_year: int) -> list[tuple[str, str]]:
    """Returns list of (suffix, scenario_id)."""
    return [(s, sid) for s, sid, _ in discover_output_scenarios_with_labels(alpha_out_dir, aeo_year)]


# ============================================================================
# Wide-series loader
# ============================================================================

def load_wide_series(
    csv_path: Path,
    value_col: str,
    scenario_id: str,
    region_order: list[str],
    scenario_label: str | None = None,
) -> pd.DataFrame:
    require(csv_path.exists(), f"Missing file: {csv_path}")
    wide = pd.read_csv(csv_path)
    year_col = "t" if "t" in wide.columns else ("year" if "year" in wide.columns else None)
    require(year_col is not None, f"Missing year column (t/year): {csv_path}")
    missing_regions = [c for c in region_order if c not in wide.columns]
    require(not missing_regions, f"Missing region columns in {csv_path}: {missing_regions}")
    long = wide[[year_col, *region_order]].melt(
        id_vars=[year_col], var_name="region", value_name=value_col)
    long["year"] = pd.to_numeric(long[year_col], errors="coerce")
    long[value_col] = pd.to_numeric(long[value_col], errors="coerce")
    require(not long["year"].isna().any(), f"Non-numeric year in {csv_path}")
    require(not long[value_col].isna().any(), f"Non-numeric values in {csv_path}")
    long["year"] = long["year"].astype(int)
    long["scenario_id"] = scenario_id
    cols = ["scenario_id", "region", "year", value_col]
    if scenario_label is not None:
        long["scenario_label"] = scenario_label
        cols = ["scenario_id", "scenario_label", "region", "year", value_col]
    return long[cols]


# ============================================================================
# Part 1: Beta raw-data scatter grid
# ============================================================================

_RAW_KEY_COLS = ["period", "scenario", "regionId"]
_RAW_READ_COLS = ["period", "scenario", "scenarioDescription", "regionId",
                  "regionName", "value"]


def _coerce_numeric(df: pd.DataFrame, col: str) -> pd.DataFrame:
    df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=[col])


def _load_beta_include_scenarios(config: dict[str, Any]) -> list[str]:
    aeo_year = config.get("aeo_year")
    include = (config.get("scenarios") or {}).get("beta_regression", {}).get("include", [])
    if not isinstance(include, list):
        raise ValueError("Expected list at scenarios.beta_regression.include")
    year_str = str(aeo_year) if aeo_year is not None else "{aeo_year}"
    return [str(item).replace("{aeo_year}", year_str) for item in include]


def _filter_to_beta_include(df: pd.DataFrame, include_tokens: list[str]) -> pd.DataFrame:
    if not include_tokens:
        return df
    include_norm = {normalize_token(t) for t in include_tokens}
    scenario_norm = df["scenario"].map(normalize_token)
    desc_norm = df["scenarioDescription"].fillna("").map(normalize_token)
    keep_mask = scenario_norm.isin(include_norm) | desc_norm.isin(include_norm)
    filtered = df[keep_mask].copy()
    if filtered.empty:
        available = sorted(df["scenario"].unique())
        raise ValueError(
            f"No rows matched beta_regression.include. "
            f"Configured: {include_tokens}. Available: {available}")
    return filtered


def _load_and_merge_raw(demand_csv: Path, price_csv: Path) -> pd.DataFrame:
    demand = pd.read_csv(demand_csv, usecols=_RAW_READ_COLS).rename(
        columns={"value": "demand"})
    price = pd.read_csv(price_csv, usecols=_RAW_READ_COLS).rename(
        columns={"value": "price"})
    demand = _coerce_numeric(demand, "demand")
    price = _coerce_numeric(price, "price")
    for df in [demand, price]:
        df["period"] = pd.to_numeric(df["period"], errors="coerce")
        df.dropna(subset=["period"], inplace=True)
        df["period"] = df["period"].astype(int)
    demand = demand.groupby(_RAW_KEY_COLS, as_index=False).agg(
        {"scenarioDescription": "first", "regionName": "first", "demand": "mean"})
    price = price.groupby(_RAW_KEY_COLS, as_index=False).agg({"price": "mean"})
    merged = demand.merge(price, on=_RAW_KEY_COLS, how="inner", validate="one_to_one")
    return merged.sort_values(["period", "scenario", "regionId"]).reset_index(drop=True)


def _fit_line(panel_df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, float] | None:
    x = panel_df["demand"].to_numpy()
    y = panel_df["price"].to_numpy()
    if x.size < 2 or np.unique(x).size < 2:
        return None
    slope, intercept = np.polyfit(x, y, deg=1)
    x_line = np.linspace(x.min(), x.max(), 50)
    y_line = slope * x_line + intercept
    return x_line, y_line, float(slope)


def generate_raw_scatter_grid(
    beta_out_dir: Path,
    config: dict[str, Any],
    output_dir: Path,
    dpi: int = 150,
) -> None:
    """Generate beta raw-data scatter grid (demand vs price by year/region)."""
    raw_dir = beta_out_dir / "raw_aeo_data"
    demand_csv = raw_dir / "raw_ng_demand_elec.csv"
    price_csv = raw_dir / "raw_ng_price.csv"
    if not demand_csv.exists() or not price_csv.exists():
        LOGGER.warning("Skipping raw scatter grid: raw CSVs not found in %s", raw_dir)
        return

    merged = _load_and_merge_raw(demand_csv, price_csv)
    include_scenarios = _load_beta_include_scenarios(config)
    merged = _filter_to_beta_include(merged, include_scenarios)

    years = sorted(merged["period"].unique())
    scenarios = sorted(merged["scenario"].unique())
    region_table = (merged[["regionId", "regionName"]].drop_duplicates()
                    .sort_values(["regionId", "regionName"]).reset_index(drop=True))
    regions = region_table.to_dict("records")
    scenario_colors = {s: plt.cm.tab10(i % 10) for i, s in enumerate(scenarios)}

    n_rows, n_cols = len(years), len(regions)
    fig_w = max(15, n_cols * 2.6)
    fig_h = max(12, n_rows * 1.8)
    fig, axes = plt.subplots(nrows=n_rows, ncols=n_cols,
                             figsize=(fig_w, fig_h), sharex=True, sharey=True, squeeze=False)

    x_min, x_max = merged["demand"].min(), merged["demand"].max()
    y_min, y_max = merged["price"].min(), merged["price"].max()
    x_pad = 0.05 * (x_max - x_min) if x_max > x_min else 0.1
    y_pad = 0.05 * (y_max - y_min) if y_max > y_min else 0.1

    for row_idx, year in enumerate(years):
        for col_idx, region in enumerate(regions):
            ax = axes[row_idx, col_idx]
            panel = merged[(merged["period"] == year) & (merged["regionId"] == region["regionId"])]
            ax.scatter(panel["demand"], panel["price"], s=20, alpha=0.95,
                       c=panel["scenario"].map(scenario_colors), edgecolors="none")
            fit = _fit_line(panel)
            if fit is not None:
                x_line, y_line, slope = fit
                is_negative = slope < 0
                line_color = "#c62828" if is_negative else "#4d4d4d"
                ax.plot(x_line, y_line, color=line_color, linewidth=1.1)
                slope_text = f"slope={slope:.3f}"
                if is_negative:
                    slope_text += " (NEG)"
                ax.text(0.03, 0.97, slope_text, transform=ax.transAxes,
                        ha="left", va="top", fontsize=6,
                        color="#8b0000" if is_negative else "#303030",
                        bbox={"boxstyle": "round,pad=0.2",
                              "facecolor": "#ffe6e6" if is_negative else "white",
                              "alpha": 0.85,
                              "edgecolor": "#c62828" if is_negative else "none",
                              "linewidth": 0.6 if is_negative else 0.0})
                if is_negative:
                    ax.set_facecolor("#fff5f5")
                    for spine in ax.spines.values():
                        spine.set_edgecolor("#c62828")
                        spine.set_linewidth(0.9)
            ax.set_xlim(x_min - x_pad, x_max + x_pad)
            ax.set_ylim(y_min - y_pad, y_max + y_pad)
            ax.grid(True, alpha=0.25, linewidth=0.5)
            ax.tick_params(axis="both", labelsize=6, length=2)
            if row_idx == 0:
                ax.set_title(str(region["regionName"]), fontsize=8, pad=3)
            if col_idx == 0:
                ax.set_ylabel(str(year), fontsize=8, rotation=0, labelpad=18, va="center")

    legend_handles = [
        Line2D([0], [0], marker="o", linestyle="", markersize=5,
               markerfacecolor=scenario_colors[s], markeredgecolor="none", label=s)
        for s in scenarios
    ]
    fig.supxlabel("Natural Gas Demand (quads)", fontsize=11)
    fig.supylabel("Natural Gas Price (2024 $/MMBtu)", fontsize=11)
    fig.suptitle(
        "Raw AEO NG Data: Demand vs Price\n"
        "(Scatter + Linear Regression by Year and Region; negative slopes highlighted)",
        fontsize=13, y=0.995)
    fig.legend(handles=legend_handles, loc="upper center",
               bbox_to_anchor=(0.5, 0.978), ncol=min(len(scenarios), 5),
               fontsize=8, frameon=False, title="Scenario", title_fontsize=8)
    fig.tight_layout(rect=[0.04, 0.02, 1, 0.95])

    output_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_dir / "beta_raw_data_scatter_linear_grid.png", dpi=dpi)
    plt.close(fig)
    LOGGER.info("Wrote raw scatter grid to %s", output_dir)


# ============================================================================
# Part 2: Beta / alpha diagnostic plots
# ============================================================================

def generate_beta_plots(beta_out_dir: Path, region_order: list[str], plots_dir: Path) -> None:
    plots_dir.mkdir(parents=True, exist_ok=True)
    summary_path = beta_out_dir / "beta_regression_summary.csv"
    points_path = beta_out_dir / "regression_points.csv"
    require(summary_path.exists(), f"Missing file: {summary_path}")
    require(points_path.exists(), f"Missing file: {points_path}")

    summary = pd.read_csv(summary_path)
    points = pd.read_csv(points_path)
    nat_y_col = "dp_partial_nat"
    reg_x_col = "dq_reg"
    for col in ["scope", "region", "beta"]:
        require(col in summary.columns, f"Missing '{col}' in {summary_path}")
    for col in ["region", "dq_nat", nat_y_col, reg_x_col, "dp_partial_reg"]:
        require(col in points.columns, f"Missing '{col}' in {points_path}")

    national_rows = summary[summary["scope"].astype(str).str.lower() == "national"]
    require(not national_rows.empty, f"No national row found in {summary_path}")
    nrow = national_rows.iloc[0]
    beta_nat = float(pd.to_numeric([nrow["beta"]], errors="coerce")[0])
    beta_nat_r2 = (float(pd.to_numeric([nrow["r2"]], errors="coerce")[0])
                   if "r2" in summary.columns else float("nan"))
    model_r2 = (float(pd.to_numeric([nrow["r2_full"]], errors="coerce")[0])
                if "r2_full" in summary.columns else float("nan"))

    # National beta plot
    nat_points = points[["dq_nat", nat_y_col]].dropna()
    require(not nat_points.empty,
            f"No valid national plotting rows in {points_path}")
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.scatter(nat_points["dq_nat"], nat_points[nat_y_col], s=12, alpha=0.5, color="#1f77b4")
    x_min, x_max = float(nat_points["dq_nat"].min()), float(nat_points["dq_nat"].max())
    x_range = np.linspace(x_min, x_max, 200)
    ax.plot(x_range, beta_nat * x_range, color="#d62728", linewidth=2)
    ax.axhline(0, color="#888888", linewidth=0.8)
    ax.axvline(0, color="#888888", linewidth=0.8)
    ax.set_xlabel("Demeaned national demand (partial)")
    ax.set_ylabel("Price residual net of regional term (partial)")
    ax.set_title(f"National beta | beta={beta_nat:.6f}, partial R2={beta_nat_r2:.3f}, model R2={model_r2:.3f}")
    fig.tight_layout()
    fig.savefig(plots_dir / "national_beta_regression.png", dpi=220)
    plt.close(fig)

    # Regional beta plot
    regional = summary[summary["scope"].astype(str).str.lower() == "regional"].copy()
    regional["region"] = regional["region"].astype(str)
    beta_map: dict[str, float] = {}
    r2_map: dict[str, float] = {}
    for row in regional.itertuples(index=False):
        beta_val = pd.to_numeric([getattr(row, "beta")], errors="coerce")[0]
        r2_val = (pd.to_numeric([getattr(row, "r2")], errors="coerce")[0]
                  if "r2" in regional.columns else float("nan"))
        if pd.notna(beta_val):
            beta_map[str(getattr(row, "region"))] = float(beta_val)
        if pd.notna(r2_val):
            r2_map[str(getattr(row, "region"))] = float(r2_val)

    regions = [r for r in region_order if r in beta_map]
    if not regions:
        regions = sorted(points["region"].dropna().astype(str).unique().tolist())

    ncols = 3
    nrows = int(np.ceil(len(regions) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(5.2 * ncols, 4.2 * nrows))
    axes = np.atleast_1d(axes).ravel()
    for i, region in enumerate(regions):
        ax = axes[i]
        reg_r = points[points["region"] == region][[reg_x_col, "dp_partial_reg"]].dropna()
        beta = beta_map.get(region, float("nan"))
        r2 = r2_map.get(region, float("nan"))
        ax.scatter(reg_r[reg_x_col], reg_r["dp_partial_reg"], s=10, alpha=0.65, color="#1f77b4")
        if not reg_r.empty:
            x_line = np.linspace(float(reg_r[reg_x_col].min()), float(reg_r[reg_x_col].max()), 120)
            ax.plot(x_line, beta * x_line, color="#d62728", linewidth=1.6)
        ax.axhline(0, color="#999999", linewidth=0.6)
        ax.axvline(0, color="#999999", linewidth=0.6)
        ax.set_title(f"{region}\nbeta={beta:.4f}, partial R2={r2:.3f}", fontsize=10)
        ax.set_xlabel(f"{reg_x_col} (partial)")
        ax.set_ylabel("dp_reg (partial)")
    for i in range(len(regions), len(axes)):
        axes[i].axis("off")
    fig.suptitle("Regional betas (joint fixed-effects regression)", fontsize=14)
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    fig.savefig(plots_dir / "regional_beta_regression.png", dpi=220)
    plt.close(fig)
    LOGGER.info("Wrote beta plots to %s", plots_dir)


def generate_alpha_plots(
    alpha_out_dir: Path,
    alpha_input_dir: Path,
    beta_out_dir: Path,
    plots_dir: Path,
    region_order: list[str],
    aeo_year: int,
    deflator_to_2004: float,
    first_model_year: int,
) -> None:
    plots_dir.mkdir(parents=True, exist_ok=True)
    beta_regional, _ = load_regional_beta_map(alpha_out_dir, alpha_input_dir)
    beta_national, _ = load_national_beta(alpha_out_dir, alpha_input_dir, beta_out_dir)
    missing_regions = [r for r in region_order if r not in beta_regional]
    require(not missing_regions, f"Missing regional beta values: {missing_regions}")

    scenarios = discover_output_scenarios(alpha_out_dir, aeo_year)

    for suffix, scenario_id in scenarios:
        alpha_path = alpha_out_dir / f"alpha_AEO_{aeo_year}_{suffix}.csv"
        price_path = alpha_out_dir / f"ng_AEO_{aeo_year}_{suffix}.csv"
        demand_path = alpha_out_dir / f"ng_demand_AEO_{aeo_year}_{suffix}.csv"
        if not (alpha_path.exists() and price_path.exists() and demand_path.exists()):
            LOGGER.warning("Skipping suffix '%s' due to missing files.", suffix)
            continue

        alpha_df = load_wide_series(alpha_path, "alpha_2004", scenario_id, region_order)
        price_df = load_wide_series(price_path, "ng_price", scenario_id, region_order)
        demand_df = load_wide_series(demand_path, "demand_elec_quads", scenario_id, region_order)

        merged = alpha_df.merge(price_df, on=["scenario_id", "region", "year"], how="inner")
        merged = merged.merge(demand_df, on=["scenario_id", "region", "year"], how="inner")
        require(not merged.empty, f"No merged data for scenario suffix '{suffix}'")

        q_nat = (demand_df.groupby(["scenario_id", "year"], as_index=False)["demand_elec_quads"]
                 .sum().rename(columns={"demand_elec_quads": "q_nat"}))
        merged = merged.merge(q_nat, on=["scenario_id", "year"], how="left")
        merged["price_2004"] = merged["ng_price"] * deflator_to_2004
        merged["beta_reg"] = merged["region"].map(beta_regional)
        merged["term_reg"] = merged["beta_reg"] * merged["demand_elec_quads"]
        merged["term_nat"] = beta_national * merged["q_nat"]
        _zero_first_year(merged, first_model_year, "term_reg", "term_nat")

        ncols = 3
        nrows = int(np.ceil(len(region_order) / ncols))
        fig, axes = plt.subplots(nrows, ncols, figsize=(5.5 * ncols, 4.0 * nrows), sharex=True)
        axes = np.atleast_1d(axes).ravel()
        for i, region in enumerate(region_order):
            ax = axes[i]
            reg = merged[merged["region"] == region].sort_values("year")
            if reg.empty:
                continue
            years = reg["year"].to_numpy()
            ax.fill_between(years, 0, reg["alpha_2004"].to_numpy(),
                            alpha=0.5, color="#2ca02c", label="Alpha")
            ax.fill_between(years, reg["alpha_2004"].to_numpy(),
                            reg["alpha_2004"].to_numpy() + reg["term_reg"].to_numpy(),
                            alpha=0.5, color="#1f77b4", label="Beta_reg * Q_reg")
            ax.fill_between(years,
                            reg["alpha_2004"].to_numpy() + reg["term_reg"].to_numpy(),
                            reg["alpha_2004"].to_numpy() + reg["term_reg"].to_numpy() + reg["term_nat"].to_numpy(),
                            alpha=0.5, color="#ff7f0e", label="Beta_nat * Q_nat")
            ax.plot(years, reg["price_2004"].to_numpy(), color="#d62728",
                    linewidth=1.5, linestyle="--", label="Actual price")
            ax.set_title(region, fontsize=12, pad=6)
            ax.set_ylabel("2004$/MMBtu", fontsize=11)
            ax.grid(True, alpha=0.3)
        for i in range(len(region_order), len(axes)):
            axes[i].axis("off")
        handles, labels = axes[0].get_legend_handles_labels()
        fig.legend(handles, labels, loc="lower center", ncol=4, fontsize=10,
                   bbox_to_anchor=(0.5, -0.02))
        fig.suptitle(f"Price decomposition: {scenario_id}\n"
                     "Price = Alpha + Beta_reg*Q_reg + Beta_nat*Q_nat", fontsize=15)
        fig.tight_layout(rect=[0, 0.04, 1, 0.95])
        safe_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", scenario_id)
        fig.savefig(plots_dir / f"alpha_price_decomposition_{safe_id}.png",
                    dpi=220, bbox_inches="tight")
        plt.close(fig)
        LOGGER.info("Wrote alpha plot for %s", scenario_id)


# ============================================================================
# Part 3: Validation
# ============================================================================

def build_validation_frame(
    alpha_out_dir: Path,
    aeo_year: int,
    region_order: list[str],
) -> pd.DataFrame:
    LOGGER.info("Building validation frame from alpha outputs in %s", alpha_out_dir)
    frames: list[pd.DataFrame] = []
    for suffix, scenario_id, scenario_label in discover_output_scenarios_with_labels(alpha_out_dir, aeo_year):
        alpha_path = alpha_out_dir / f"alpha_AEO_{aeo_year}_{suffix}.csv"
        price_path = alpha_out_dir / f"ng_AEO_{aeo_year}_{suffix}.csv"
        demand_path = alpha_out_dir / f"ng_demand_AEO_{aeo_year}_{suffix}.csv"
        if not (alpha_path.exists() and price_path.exists() and demand_path.exists()):
            LOGGER.warning("Skipping suffix '%s' due to missing files.", suffix)
            continue
        alpha_df = load_wide_series(alpha_path, "alpha_2004", scenario_id, region_order, scenario_label)
        price_df = load_wide_series(price_path, "ng_price", scenario_id, region_order, scenario_label)
        demand_df = load_wide_series(demand_path, "demand_elec_quads", scenario_id, region_order, scenario_label)
        merged = alpha_df.merge(price_df, on=["scenario_id", "scenario_label", "region", "year"], how="inner")
        merged = merged.merge(demand_df, on=["scenario_id", "scenario_label", "region", "year"], how="inner")
        require(not merged.empty, f"No merged rows for scenario '{scenario_id}' ({suffix})")
        frames.append(merged)
    require(frames, "No scenario frames available for validation.")
    out = pd.concat(frames, ignore_index=True)
    q_nat = (out.groupby(["scenario_id", "scenario_label", "year"], as_index=False)["demand_elec_quads"]
             .sum().rename(columns={"demand_elec_quads": "q_nat"}))
    return out.merge(q_nat, on=["scenario_id", "scenario_label", "year"], how="left")


def load_beta_regression_points(beta_out_dir: Path) -> tuple[pd.DataFrame, Path]:
    path = beta_out_dir / "regression_points.csv"
    require(path.exists(), f"Missing regression_points.csv: {path}")
    df = pd.read_csv(path)
    need = {"scenario_id", "year", "region", "demand", "demand_nat", "price_2004", "dp", "dp_hat"}
    require(not (need - set(df.columns)), f"Missing columns in {path}: {sorted(need - set(df.columns))}")
    out = df[list(need)].copy()
    out["scenario_id"] = out["scenario_id"].astype(str)
    out["region"] = out["region"].astype(str).map(_to_region_label)
    for c in ["year", "demand", "demand_nat", "price_2004", "dp", "dp_hat"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    require(not out[["year", "demand", "demand_nat", "price_2004", "dp", "dp_hat"]].isna().any().any(),
            f"Non-numeric value(s) in {path}")
    out["year"] = out["year"].astype(int)
    return out, path


def build_step1_beta_validation_frame(
    beta_out_dir: Path,
    beta_reg_step_map: dict[str, float],
    beta_nat_step: float,
    alpha1_frame: pd.DataFrame,
    alpha1_merge_cols: list[str],
    first_model_year: int,
) -> tuple[pd.DataFrame, Path]:
    points, points_path = load_beta_regression_points(beta_out_dir)
    frame = points.rename(columns={
        "demand": "demand_elec_quads", "demand_nat": "q_nat",
        "price_2004": "actual_2004", "dp": "actual_dprice", "dp_hat": "predicted_dprice",
    }).copy()
    frame["scenario_label"] = frame["scenario_id"]
    frame["beta_reg"] = frame["region"].map(beta_reg_step_map)
    require(not frame["beta_reg"].isna().any(),
            "Missing regional beta mapping in step-1 validation.")
    frame["beta_nat"] = beta_nat_step
    frame = frame.merge(alpha1_frame, on=alpha1_merge_cols, how="left")
    require(not frame["alpha1"].isna().any(),
            "Missing alpha1 rows in step-1 validation.")
    _zero_first_year(frame, first_model_year, "beta_reg", "beta_nat")
    frame["predicted_2004"] = (frame["alpha1"]
                               + frame["beta_reg"] * frame["demand_elec_quads"]
                               + frame["beta_nat"] * frame["q_nat"])
    frame["error"] = frame["actual_2004"] - frame["predicted_2004"]
    frame["error_dprice"] = frame["actual_dprice"] - frame["predicted_dprice"]
    return frame, points_path


def _parity_panel(ax, frame, title, actual_col, predicted_col, xlabel, ylabel):
    m = compute_fit_metrics(frame[actual_col], frame[predicted_col])
    x = frame[actual_col].to_numpy(dtype=float)
    y = frame[predicted_col].to_numpy(dtype=float)
    mask = np.isfinite(x) & np.isfinite(y)
    x, y = x[mask], y[mask]
    ax.scatter(x, y, s=8, alpha=0.35, color="#1f77b4", edgecolors="none")
    if len(x) > 0:
        lo = float(min(np.min(x), np.min(y)))
        hi = float(max(np.max(x), np.max(y)))
        if hi <= lo:
            hi = lo + 1.0
        pad = 0.05 * (hi - lo)
        ax.plot([lo - pad, hi + pad], [lo - pad, hi + pad], color="#333333",
                linestyle="--", linewidth=1.2)
        ax.set_xlim(lo - pad, hi + pad)
        ax.set_ylim(lo - pad, hi + pad)
    ax.set_title(title, fontsize=11, pad=4)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.25)
    ax.text(0.02, 0.98,
            f"N={int(m['n_obs'])}\nR2={m['r2']:.4f}\nRMSE={m['rmse']:.4f}\nMAE={m['mae']:.4f}",
            transform=ax.transAxes, va="top", ha="left", fontsize=9,
            bbox={"boxstyle": "round,pad=0.2", "facecolor": "white",
                  "alpha": 0.75, "edgecolor": "#bbbbbb"})
    return m


def plot_overall_parity(step1_frame, step2_frame, out_path):
    fig, axes = plt.subplots(1, 2, figsize=(12.8, 5.6))
    step1_m = _parity_panel(axes[0], step1_frame,
                            "Step 1: beta-regression fit\n(demeaned price: dp vs dp_hat)",
                            "actual_dprice", "predicted_dprice",
                            "Actual demeaned price (2004$/MMBtu)",
                            "Predicted demeaned price (2004$/MMBtu)")
    step2_m = _parity_panel(axes[1], step2_frame,
                            "Step 2: alpha-regression scenarios\n(level price: alpha2 + beta x demand)",
                            "actual_2004", "predicted_2004",
                            "Actual (2004$/MMBtu)", "Predicted (2004$/MMBtu)")
    fig.suptitle("Results validation parity: step1 demeaned fit vs step2 level fit",
                 fontsize=14, y=0.98)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    return step1_m, step2_m


def plot_actual_vs_predicted(frame, region_order, out_path):
    scenarios = list(dict.fromkeys(frame["scenario_label"].astype(str).tolist()))
    ncols = 3
    nrows = int(np.ceil(len(region_order) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(5.3 * ncols, 4.0 * nrows), sharex=True)
    axes = np.atleast_1d(axes).ravel()
    for i, region in enumerate(region_order):
        ax = axes[i]
        reg = frame[frame["region"] == region].copy()
        if reg.empty:
            ax.set_title(region, fontsize=10)
            ax.grid(True, alpha=0.25)
            continue
        for scen in scenarios:
            sdf = reg[reg["scenario_label"] == scen].sort_values("year")
            if sdf.empty:
                continue
            color = SCENARIO_COLOR.get(scen)
            ax.plot(sdf["year"], sdf["actual_2004"], color=color, linewidth=1.6)
            ax.plot(sdf["year"], sdf["predicted_2004"], color=color,
                    linewidth=3.0, linestyle="--", alpha=0.98, zorder=4)
        ax.set_title(region, fontsize=10, pad=4)
        ax.set_ylabel("2004$/MMBtu")
        ax.grid(True, alpha=0.25)
    for i in range(len(region_order), len(axes)):
        axes[i].axis("off")
    legend_handles = []
    for scen in scenarios:
        color = SCENARIO_COLOR.get(scen, "#333333")
        legend_handles.append(Line2D([0], [0], color=color, lw=2.0, linestyle="-",
                                     label=f"{scen} actual"))
        legend_handles.append(Line2D([0], [0], color=color, lw=2.0, linestyle="--",
                                     alpha=0.7, label=f"{scen} predicted"))
    fig.legend(handles=legend_handles, loc="lower center",
               ncol=max(2, min(6, len(legend_handles))), fontsize=8,
               bbox_to_anchor=(0.5, -0.005))
    fig.suptitle("Results validation: actual price vs predicted\n"
                 "(predicted = alpha(region,year,scenario) + beta x scenario demand)",
                 fontsize=14, y=0.98)
    fig.tight_layout(rect=[0, 0.04, 1, 0.95])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=220, bbox_inches="tight")
    plt.close(fig)


def plot_alpha_vs_alpha1(frame, region_order, out_path):
    scenarios = list(dict.fromkeys(frame["scenario_label"].astype(str).tolist()))
    alpha1_shared = bool(
        frame.groupby(["region", "year"], as_index=False)["alpha1"].nunique()["alpha1"].max() <= 1)
    ncols = 3
    nrows = int(np.ceil(len(region_order) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(5.3 * ncols, 4.0 * nrows), sharex=True)
    axes = np.atleast_1d(axes).ravel()
    for i, region in enumerate(region_order):
        ax = axes[i]
        reg = frame[frame["region"] == region].copy()
        if reg.empty:
            ax.set_title(region, fontsize=10)
            ax.grid(True, alpha=0.25)
            continue
        for scen in scenarios:
            sdf = reg[reg["scenario_label"] == scen].sort_values("year")
            if sdf.empty:
                continue
            color = SCENARIO_COLOR.get(scen)
            ax.plot(sdf["year"], sdf["alpha_2004"], color=color, linewidth=1.7)
            if not alpha1_shared:
                ax.plot(sdf["year"], sdf["alpha1"], color=color, linewidth=2.6,
                        linestyle="--", alpha=0.98, zorder=4)
        if alpha1_shared:
            a1 = reg[["year", "alpha1"]].drop_duplicates(subset=["year"]).sort_values("year")
            ax.plot(a1["year"], a1["alpha1"], color="#222222", linewidth=2.8,
                    linestyle="--", alpha=0.98, zorder=5, label="alpha1 shared")
        ax.set_title(region, fontsize=10, pad=4)
        ax.set_ylabel("alpha")
        ax.grid(True, alpha=0.25)
    for i in range(len(region_order), len(axes)):
        axes[i].axis("off")
    legend_handles = []
    for scen in scenarios:
        color = SCENARIO_COLOR.get(scen, "#333333")
        legend_handles.append(Line2D([0], [0], color=color, lw=2.2, linestyle="-",
                                     label=f"{scen} alpha2"))
        if not alpha1_shared:
            legend_handles.append(Line2D([0], [0], color=color, lw=2.6, linestyle="--",
                                         alpha=0.98, label=f"{scen} alpha1"))
    if alpha1_shared:
        legend_handles.append(Line2D([0], [0], color="#222222", lw=2.8, linestyle="--",
                                     alpha=0.98, label="alpha1 shared"))
    fig.legend(handles=legend_handles, loc="lower center",
               ncol=max(2, min(6, len(legend_handles))), fontsize=8,
               bbox_to_anchor=(0.5, -0.005))
    fig.suptitle("Results validation: alpha2 vs alpha1\n"
                 "(alpha2 = alpha regression output, alpha1 = beta regression output)",
                 fontsize=14, y=0.98)
    fig.tight_layout(rect=[0, 0.04, 1, 0.95])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=220, bbox_inches="tight")
    plt.close(fig)


def run_validation(
    config: dict[str, Any],
    base_dir: Path,
    beta_out_dir: Path,
    alpha_out_dir: Path,
    alpha_input_dir: Path,
    validation_dir: Path,
    region_order: list[str],
) -> None:
    """Run full validation: CSV metrics + plots."""
    aeo_year = int(config["aeo_year"])
    first_model_year = int(config["start_year"])
    deflator = float(config["ng"]["price_deflator_to_2004"])
    validation_dir.mkdir(parents=True, exist_ok=True)

    # Load betas
    beta_reg_map, beta_reg_src = load_regional_beta_map(alpha_out_dir, alpha_input_dir)
    beta_nat, beta_nat_src = load_national_beta(alpha_out_dir, alpha_input_dir, beta_out_dir)
    missing_regions = [r for r in region_order if r not in beta_reg_map]
    require(not missing_regions, f"Missing regional beta: {missing_regions}")

    beta_reg_step_map = load_regional_beta_from_csv(beta_out_dir / "cd_beta0.csv")
    beta_nat_step = load_national_beta_from_csv(beta_out_dir / "national_beta.csv")

    # Beta comparison CSV
    beta_compare_rows = [{
        "scope": "national", "region": "ALL",
        "beta1_from_beta_step": beta_nat_step,
        "beta2_used_in_alpha_step": beta_nat,
        "diff_beta1_minus_beta2": beta_nat_step - beta_nat,
    }]
    for region in region_order:
        b1, b2 = beta_reg_step_map[region], beta_reg_map[region]
        beta_compare_rows.append({
            "scope": "regional", "region": region,
            "beta1_from_beta_step": b1,
            "beta2_used_in_alpha_step": b2,
            "diff_beta1_minus_beta2": b1 - b2,
        })
    pd.DataFrame(beta_compare_rows).to_csv(
        validation_dir / "results_validation_beta_comparison.csv",
        index=False, float_format="%.6f")

    # Build step-2 validation frame
    frame = build_validation_frame(alpha_out_dir, aeo_year, region_order)
    frame["actual_2004"] = frame["ng_price"] * deflator
    frame["beta_reg"] = frame["region"].map(beta_reg_map)
    frame["beta_nat"] = beta_nat
    _zero_first_year(frame, first_model_year, "beta_reg", "beta_nat")
    frame["predicted_2004"] = (frame["alpha_2004"]
                               + frame["beta_reg"] * frame["demand_elec_quads"]
                               + frame["beta_nat"] * frame["q_nat"])
    frame["error"] = frame["actual_2004"] - frame["predicted_2004"]

    # Alpha1 from beta step
    alpha1_frame, _, alpha1_merge_cols = load_alpha_from_beta_step(beta_out_dir)
    step1_frame, _ = build_step1_beta_validation_frame(
        beta_out_dir, beta_reg_step_map, beta_nat_step,
        alpha1_frame, alpha1_merge_cols, first_model_year)

    frame = frame.merge(alpha1_frame, on=alpha1_merge_cols, how="left")
    frame["alpha_vs_alpha1_error"] = frame["alpha_2004"] - frame["alpha1"]
    alpha_cmp_frame = frame[~frame["alpha1"].isna()].copy()

    # Write detail/summary CSVs
    step1_frame[[
        "scenario_id", "scenario_label", "region", "year",
        "actual_2004", "predicted_2004", "error",
        "actual_dprice", "predicted_dprice", "error_dprice",
        "alpha1", "beta_reg", "beta_nat", "demand_elec_quads", "q_nat",
    ]].to_csv(validation_dir / "results_validation_step1_beta_actual_vs_predicted_detail.csv",
              index=False, float_format="%.6f")

    summarize_fit(step1_frame, ["scenario_label", "region"],
                  "actual_dprice", "predicted_dprice",
                  "mae_error", "rmse_error", "max_abs_error", "r2").to_csv(
        validation_dir / "results_validation_step1_beta_actual_vs_predicted_summary.csv",
        index=False, float_format="%.6f")

    frame[[
        "scenario_id", "scenario_label", "region", "year",
        "actual_2004", "predicted_2004", "error",
        "alpha_2004", "beta_reg", "beta_nat", "demand_elec_quads", "q_nat",
        "alpha1", "alpha_vs_alpha1_error",
    ]].to_csv(validation_dir / "results_validation_actual_vs_predicted_detail.csv",
              index=False, float_format="%.6f")

    summarize_fit(frame, ["scenario_label", "region"],
                  "actual_2004", "predicted_2004",
                  "mae_error", "rmse_error", "max_abs_error", "r2").to_csv(
        validation_dir / "results_validation_actual_vs_predicted_summary.csv",
        index=False, float_format="%.6f")

    if not alpha_cmp_frame.empty:
        summarize_fit(alpha_cmp_frame, ["scenario_label", "region"],
                      "alpha_2004", "alpha1",
                      "mae_alpha1_error", "rmse_alpha1_error",
                      "max_abs_alpha1_error", "r2_alpha1").to_csv(
            validation_dir / "results_validation_alpha_vs_alpha1_summary.csv",
            index=False, float_format="%.6f")

        alpha_spread = (alpha_cmp_frame.groupby(["region", "year"], as_index=False)["alpha_2004"]
                        .agg(alpha_min="min", alpha_max="max"))
        alpha_spread["alpha_spread"] = alpha_spread["alpha_max"] - alpha_spread["alpha_min"]
        alpha_spread.to_csv(validation_dir / "results_validation_alpha_vs_alpha1_spread.csv",
                            index=False, float_format="%.6f")

        alpha_cmp_frame[[
            "scenario_id", "scenario_label", "region", "year",
            "alpha_2004", "alpha1", "alpha_vs_alpha1_error",
        ]].to_csv(validation_dir / "results_validation_alpha_vs_alpha1_detail.csv",
                   index=False, float_format="%.6f")

    # Plots
    plot_actual_vs_predicted(frame, region_order,
                             validation_dir / "results_validation_actual_vs_predicted.png")
    if not alpha_cmp_frame.empty:
        plot_alpha_vs_alpha1(alpha_cmp_frame, region_order,
                             validation_dir / "results_validation_alpha_vs_alpha1.png")
    step1_m, step2_m = plot_overall_parity(step1_frame, frame,
                                           validation_dir / "results_validation_parity_overall.png")

    pd.DataFrame([
        {"step": "step1_beta_regression_scenarios", "metric_basis": "demeaned_price_dp",
         "n_obs": int(step1_m["n_obs"]), "mae_error": step1_m["mae"],
         "rmse_error": step1_m["rmse"], "max_abs_error": step1_m["max_abs"], "r2": step1_m["r2"]},
        {"step": "step2_alpha_regression_scenarios", "metric_basis": "level_price_2004_per_mmbtu",
         "n_obs": int(step2_m["n_obs"]), "mae_error": step2_m["mae"],
         "rmse_error": step2_m["rmse"], "max_abs_error": step2_m["max_abs"], "r2": step2_m["r2"]},
    ]).to_csv(validation_dir / "results_validation_overall_metrics.csv",
              index=False, float_format="%.6f")

    LOGGER.info("Validation complete. Outputs in %s", validation_dir)


# ============================================================================
# CLI
# ============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Unified visualization and validation for NG regression pipeline.")
    parser.add_argument("--config", default="aeo_pipeline_config.json")
    parser.add_argument("--beta-output-dir", default="outputs of beta regression")
    parser.add_argument("--alpha-output-dir", default=None)
    parser.add_argument("--alpha-input-dir", default=None)
    parser.add_argument("--output-dir", default="results validation",
                        help="Directory for all plots and validation CSVs.")
    parser.add_argument("--skip-raw-scatter", action="store_true",
                        help="Skip beta raw-data scatter grid.")
    parser.add_argument("--skip-beta", action="store_true",
                        help="Skip beta diagnostic plots.")
    parser.add_argument("--skip-alpha", action="store_true",
                        help="Skip alpha decomposition plots.")
    parser.add_argument("--skip-validation", action="store_true",
                        help="Skip validation plots and CSVs.")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    setup_logging(args.log_level)

    script_dir = Path(__file__).resolve().parent
    cfg_path = resolve_config_path(args.config, script_dir)

    config = load_config(cfg_path)
    base_dir = cfg_path.parent

    # Load shared region mapping from config
    global CENDIV_OUTPUT
    CENDIV_OUTPUT = {
        k.replace(" ", ""): v for k, v in config["ng"]["cendiv_and_label"].items()
    }

    beta_out_dir = resolve_path(base_dir, args.beta_output_dir)
    alpha_out_dir = resolve_path(base_dir,
                                 args.alpha_output_dir or config["paths"]["output_dir"])
    alpha_input_dir = resolve_path(base_dir,
                                   args.alpha_input_dir or config["paths"].get("input_dir", "inputs for alpha regression"))
    output_dir = resolve_path(base_dir, args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    region_order = region_order_from_config(config)
    aeo_year = int(config["aeo_year"])
    first_model_year = int(config["start_year"])
    deflator = float(config["ng"]["price_deflator_to_2004"])

    if not args.skip_raw_scatter:
        generate_raw_scatter_grid(beta_out_dir, config, output_dir)

    if not args.skip_beta:
        generate_beta_plots(beta_out_dir, region_order, output_dir)

    if not args.skip_alpha:
        generate_alpha_plots(
            alpha_out_dir=alpha_out_dir,
            alpha_input_dir=alpha_input_dir,
            beta_out_dir=beta_out_dir,
            plots_dir=output_dir,
            region_order=region_order,
            aeo_year=aeo_year,
            deflator_to_2004=deflator,
            first_model_year=first_model_year,
        )

    if not args.skip_validation:
        run_validation(
            config=config,
            base_dir=base_dir,
            beta_out_dir=beta_out_dir,
            alpha_out_dir=alpha_out_dir,
            alpha_input_dir=alpha_input_dir,
            validation_dir=output_dir,
            region_order=region_order,
        )

    LOGGER.info("All visualization and validation complete.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        LOGGER.exception("Failed: %s", exc)
        sys.exit(1)
