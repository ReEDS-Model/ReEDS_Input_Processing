"""
AEO Natural Gas Price Preprocessing Pipeline for ReEDS Inputs
=============================================================

This script automates the preprocessing of AEO (Annual Energy Outlook)
natural gas data from the EIA API into input files for the ReEDS capacity
expansion model.

NG Supply Curve Model
---------------------
ReEDS models regional natural gas prices using a linear supply curve
framework. The regional delivered price of natural gas in each census
division is decomposed into three components:

    Price(r,t,s) = Alpha(r,t,s) + Beta_regional(r) × Demand_regional(r,t,s)
                                 + Beta_national × Demand_national(t,s)

Where:
    - Price(r,t,s)         : AEO NG price for region r, year t, scenario s (2024$/MMBtu)
    - Alpha(r,t,s)         : Intercept / base price component (2004$/MMBtu)
    - Beta_regional(r)     : Region-specific demand sensitivity (2004$/MMBtu per Quad)
    - Demand_regional(r,t,s) : Electric sector NG demand in region r (Quads)
    - Beta_national         : National demand sensitivity (2004$/MMBtu per Quad)
    - Demand_national(t,s) : Total national electric sector NG demand (Quads)

Alpha is solved as the residual after removing demand-driven price effects:

    Alpha(r,t,s) = Price(r,t,s) × deflator - Beta_regional(r) × Demand_regional(r,t,s)
                                            - Beta_national × Demand_national(t,s)

Alpha is solved at scenario level and indexed by (region, year, scenario),
so each scenario keeps its own residual alpha path.

The deflator converts from the AEO's native dollar year (e.g., 2024$) to
2004$, the base dollar year used internally by ReEDS for NG pricing.

Data Sources
------------
- Prices and demand projections: EIA AEO API (3 scenarios: Reference, High/Low O&G)
- Historical backfill: Local CSV files for pre-projection years
- Betas: Pre-computed regional and national sensitivity coefficients

Output Files (per scenario)
---------------------------
- alpha_AEO_{year}_{scenario}.csv  : Regional alpha values (2004$/MMBtu)
- ng_AEO_{year}_{scenario}.csv     : Regional NG prices (AEO dollar year)
- ng_demand_AEO_{year}_{scenario}.csv : Electric sector demand (Quads)
- ng_tot_demand_AEO_{year}_{scenario}.csv : Total sector demand (Quads)
- cd_beta0.csv                      : Electric sector regional betas

Usage
-----
    python aeo_alpha_regression.py --config aeo_pipeline_config.json

Example config: see aeo_pipeline_config.json
"""

from __future__ import annotations

import argparse
import logging
import re
import shutil
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from aeo_functions import (
    CENDIV_CANONICAL,
    NG_SERIES_NAMES,
    EiaClient,
    cfg_list,
    cfg_section,
    list_aeo_scenarios,
    load_config,
    match_scenario,
    normalize_token,
    require,
    resolve_api_key,
    resolve_case_insensitive,
    resolve_config_path,
    resolve_path,
    resolve_region_ids,
    resolve_series_ids,
    setup_logging,
)

LOGGER = logging.getLogger("aeo_pipeline")

# ============================================================================
# Constants
# ============================================================================

# Mapping from internal keys to ReEDS output format (underscore-separated).
# Populated at runtime from config["ng"]["cendiv_and_label"] by run_ng_pipeline().
CENDIV_OUTPUT: dict[str, str] = {}

# Required output scenarios and their EIA API aliases
NG_OUTPUT_SCENARIOS = {
    "reference": ["ref{aeo_year}", "Reference case", "Reference Case"],
    "HOG": ["highogs", "High Oil and Gas Supply"],
    "LOG": ["lowogs", "Low Oil and Gas Supply"],
}

HISTORY_SUFFIX = "historical"


# ============================================================================
# Utility Functions
# ============================================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="AEO NG update pipeline")
    p.add_argument("--config", default="aeo_pipeline_config.json")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument("--aeo-year", type=int, default=None)
    return p.parse_args()


def canonical_cendiv(value: Any) -> str:
    """Convert a region name to its canonical internal key."""
    key = normalize_token(value)
    return CENDIV_CANONICAL.get(key, re.sub(r"[^A-Za-z0-9]", "", str(value)))


def cendiv_output_label(cendiv: str) -> str:
    """Convert an internal key to ReEDS output format (e.g., 'New_England')."""
    require(cendiv in CENDIV_OUTPUT,
            f"Unsupported census division for NG output: {cendiv}")
    return CENDIV_OUTPUT[cendiv]


def output_label_to_cendiv(label: str) -> str:
    """Convert a ReEDS output label back to the internal key."""
    norm = normalize_token(label.replace("_", " "))
    for cendiv, output in CENDIV_OUTPUT.items():
        if normalize_token(output.replace("_", " ")) == norm:
            return cendiv
    return canonical_cendiv(label)


def resolve_ng_scenarios(
    client: EiaClient,
    aeo_year: int,
    include_scenarios: list[str] | None = None,
) -> pd.DataFrame:
    """
    Discover available AEO scenarios for NG data.

    If include_scenarios is provided, only those scenarios are kept
    (matching by scenario id or scenario name, case-insensitive).
    """
    rows = list_aeo_scenarios(client, aeo_year)

    if include_scenarios:
        picked: list[dict[str, str]] = []
        missing: list[str] = []
        seen_ids: set[str] = set()
        for raw in include_scenarios:
            found = match_scenario(rows, raw, aeo_year)
            if found is None:
                missing.append(str(raw))
            elif found["scenario_id"] not in seen_ids:
                picked.append(found)
                seen_ids.add(found["scenario_id"])
        require(not missing,
                f"Configured alpha_regression.fetch scenarios not found: {missing}")
        out = pd.DataFrame(picked).reset_index(drop=True)
    else:
        out = (pd.DataFrame(rows)
               .drop_duplicates(subset=["scenario_id"])
               .sort_values("scenario_id")
               .reset_index(drop=True))

    LOGGER.info("Selected NG scenarios (%d): %s", len(out), out["scenario_id"].tolist())
    return out


def resolve_output_scenario_aliases(
    aeo_year: int,
    configured_outputs: Any,
) -> dict[str, list[str]]:
    """Build output scenario alias map from config (defaults to NG_OUTPUT_SCENARIOS)."""
    if configured_outputs is None:
        raw_map: dict[str, list[str]] = NG_OUTPUT_SCENARIOS
    else:
        require(isinstance(configured_outputs, dict),
                "alpha_regression.outputs must be an object mapping suffix -> list of aliases.")
        raw_map = {}
        for suffix, aliases in configured_outputs.items():
            require(isinstance(aliases, list),
                    f"alpha_regression.outputs['{suffix}'] must be a list.")
            raw_map[str(suffix)] = [str(a) for a in aliases]

    out: dict[str, list[str]] = {}
    for suffix, aliases in raw_map.items():
        suffix_txt = str(suffix).strip()
        require(suffix_txt, "alpha_regression.outputs contains an empty file_suffix.")
        clean = [str(a).strip() for a in aliases if str(a).strip()]
        require(clean, f"alpha_regression.outputs['{suffix_txt}'] has no aliases.")
        out[suffix_txt] = clean
    require(out, "alpha_regression.outputs resolved to an empty mapping.")
    LOGGER.info("Configured alpha output scenario map for AEO %d: %s", aeo_year, out)
    return out


def resolve_output_scenarios(
    available_scenarios: pd.DataFrame,
    aeo_year: int,
    output_aliases: dict[str, list[str]],
) -> pd.DataFrame:
    """Match available scenarios to configured output scenario aliases."""
    require(not available_scenarios.empty,
            "No available scenarios to resolve NG output scenarios.")
    rows_dict = available_scenarios.to_dict(orient="records")
    matched: list[dict[str, str]] = []
    for suffix, aliases in output_aliases.items():
        found = next((r for alias in aliases
                      if (r := match_scenario(rows_dict, alias, aeo_year)) is not None),
                     None)
        require(found is not None,
                f"Could not resolve required NG output scenario: '{suffix}'")
        matched.append({
            "scenario_id": str(found["scenario_id"]),
            "scenario_name": str(found["scenario_name"]),
            "file_suffix": str(suffix),
        })
    out = pd.DataFrame(matched).reset_index(drop=True)
    require(out["scenario_id"].nunique() == len(out),
            "alpha_regression.outputs resolved to duplicate scenario_id values. "
            "Use distinct output scenarios.")
    LOGGER.info("Resolved NG output scenarios: %s",
                out[["scenario_id", "file_suffix"]].to_dict(orient="records"))
    return out


def resolve_ng_region_order(config: dict[str, Any]) -> list[str]:
    """Get the ordered list of census divisions from the config."""
    configured = list(config["ng"]["cendiv_and_label"].keys())
    order = [output_label_to_cendiv(x) for x in configured]
    require(len(order) == len(set(order)), "NG regions contains duplicates.")
    return order


# ============================================================================
# Data Fetching
# ============================================================================

def fetch_aeo_series_by_scenario(
    client: EiaClient,
    aeo_year: int,
    series_name: str,
    scenario_ids: list[str],
    region_ids: list[str],
    value_col: str,
    start_year: int,
    end_year: int,
    raw_output_path: Path | None = None,
) -> pd.DataFrame:
    """
    Fetch a single AEO series (price/demand) for all scenarios and regions.

    Returns a DataFrame with columns:
        [scenario_id, cendiv, year, <value_col>]
    """
    series_ids = resolve_series_ids(client, aeo_year, series_name)
    params: list[tuple[str, str]] = [
        ("data[]", "value"),
        ("start", str(start_year)),
        ("end", str(end_year)),
    ]
    params.extend(("facets[scenario][]", sid) for sid in scenario_ids)
    params.extend(("facets[regionId][]", rid) for rid in region_ids)
    params.extend(("facets[seriesId][]", sid) for sid in series_ids)

    df = client.get_data(f"/aeo/{aeo_year}/data", params=params)
    if raw_output_path is not None:
        raw_output_path.parent.mkdir(parents=True, exist_ok=True)
        raw_export = df.copy()
        raw_export.insert(0, "series_name", series_name)
        raw_export.to_csv(raw_output_path, index=False, float_format="%.6f")
        LOGGER.info(
            "Wrote raw AEO rows for '%s' to %s (%d rows)",
            series_name, raw_output_path, len(raw_export)
        )
    need = {"scenario", "regionName", "period", "value"}
    require(
        not (need - set(df.columns)),
        f"Missing columns for series '{series_name}': "
        f"{sorted(need - set(df.columns))}",
    )

    df["scenario_id"] = df["scenario"].astype(str)
    df["cendiv"] = df["regionName"].map(canonical_cendiv)
    df["year"] = pd.to_numeric(df["period"], errors="coerce")
    df[value_col] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["scenario_id", "cendiv", "year", value_col]).copy()
    df["year"] = df["year"].astype(int)
    df = df[df["scenario_id"].isin(scenario_ids)].copy()

    # Verify no conflicting duplicate values
    uniq = df.groupby(["scenario_id", "cendiv", "year"])[value_col].nunique()
    require(
        (uniq <= 1).all(),
        f"Conflicting duplicate values for series '{series_name}'. "
        f"Samples: {uniq[uniq > 1].head().to_dict()}",
    )
    out = df.groupby(
        ["scenario_id", "cendiv", "year"], as_index=False
    )[value_col].mean()
    LOGGER.info("Fetched %s rows for series '%s': %d",
                value_col, series_name, len(out))
    return out


# ============================================================================
# Data Validation and Filtering
# ============================================================================

def filter_complete_ng_scenarios(
    scenario_table: pd.DataFrame,
    series_frames: dict[str, pd.DataFrame],
    region_order: list[str],
    start_year: int,
    end_year: int,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """Drop scenarios that have incomplete region Ã— year coverage."""
    expected_count = len(region_order) * (end_year - start_year + 1)
    keep = set(scenario_table["scenario_id"].tolist())

    for name, frame in series_frames.items():
        counts = frame.groupby("scenario_id").size().to_dict()
        complete = {sid for sid in keep
                    if counts.get(sid, 0) == expected_count}
        dropped = sorted(keep - complete)
        if dropped:
            LOGGER.warning(
                "Series '%s' has incomplete coverage for %s-%s. "
                "Dropping scenarios: %s",
                name, start_year, end_year, dropped,
            )
        keep = complete

    require(keep, "No NG scenarios remain after coverage filtering.")
    out_scen = (scenario_table[scenario_table["scenario_id"].isin(keep)]
                .copy().reset_index(drop=True))
    out_frames = {
        name: frame[frame["scenario_id"].isin(keep)].copy()
        for name, frame in series_frames.items()
    }
    return out_scen, out_frames


def validate_ng_coverage(
    frame: pd.DataFrame,
    scenarios: list[str],
    region_order: list[str],
    start_year: int,
    end_year: int,
    label: str,
) -> None:
    """Verify that a DataFrame has complete scenario Ã— region Ã— year coverage."""
    expected = {
        (sid, reg, yr)
        for sid in scenarios
        for reg in region_order
        for yr in range(start_year, end_year + 1)
    }
    actual = {
        (r.scenario_id, r.cendiv, int(r.year))
        for r in frame[["scenario_id", "cendiv", "year"]].itertuples(index=False)
    }
    missing = expected - actual
    require(
        not missing,
        f"{label} missing scenario/region/year combos. "
        f"Sample: {list(sorted(missing))[:10]}",
    )


# ============================================================================
# Historical Data Handling
# ============================================================================

def load_history_wide_file(
    file_path: Path,
    value_col: str,
    region_order: list[str],
) -> pd.DataFrame:
    """
    Load a wide-format historical CSV and melt to long format.

    Expected CSV format: year/t column + one column per region (output labels).
    Returns: DataFrame with [cendiv, year, <value_col>].
    """
    require(file_path.exists(),
            f"History source file not found: {file_path}")
    df = pd.read_csv(file_path)
    year_col = ("year" if "year" in df.columns
                else ("t" if "t" in df.columns else None))
    require(year_col is not None,
            f"History file missing 'year' or 't' column: {file_path}")
    df = df.rename(columns={year_col: "year"})
    for col in df.columns:
        if col != "year":
            df[col] = pd.to_numeric(df[col], errors="coerce")
    melted = (df.melt(id_vars=["year"], var_name="region_out",
                      value_name=value_col)
              .dropna(subset=[value_col]).copy())
    melted["cendiv"] = melted["region_out"].map(output_label_to_cendiv)
    melted["year"] = pd.to_numeric(melted["year"], errors="coerce").astype(int)
    melted = melted[melted["cendiv"].isin(region_order)].copy()
    return melted[["cendiv", "year", value_col]]


def apply_reference_history_to_all_scenarios(
    frame: pd.DataFrame,
    value_col: str,
    history_frame: pd.DataFrame,
    scenario_ids: list[str],
    projection_start_year: int,
) -> pd.DataFrame:
    """
    Backfill pre-projection years with historical data.

    Historical data is replicated identically across all scenarios,
    since AEO scenarios share the same historical period.
    """
    hist = history_frame[history_frame["year"] < projection_start_year].copy()
    if hist.empty:
        return frame
    replicated = pd.concat(
        [hist.assign(scenario_id=sid) for sid in scenario_ids],
        ignore_index=True,
    )
    future = frame[frame["year"] >= projection_start_year].copy()
    out = pd.concat([future, replicated], ignore_index=True)
    out = out.groupby(
        ["scenario_id", "cendiv", "year"], as_index=False
    )[value_col].mean()
    return out


def _append_year_to_history_csv(
    csv_path: Path,
    year: int,
    data_frame: pd.DataFrame,
    scenario_id: str,
    value_col: str,
) -> None:
    """Append a single year from the reference scenario to a history CSV if missing."""
    if not csv_path.exists():
        return
    existing = pd.read_csv(csv_path)
    year_col = "year" if "year" in existing.columns else "t"
    if year_col not in existing.columns:
        return
    if year in existing[year_col].values:
        return  # Already present

    row_data = data_frame[
        (data_frame["scenario_id"] == scenario_id)
        & (data_frame["year"] == year)
    ].copy()
    if row_data.empty:
        LOGGER.warning("No data for year %d to append to %s.", year, csv_path.name)
        return

    row_data["region_out"] = row_data["cendiv"].map(cendiv_output_label)
    wide = row_data.pivot_table(
        index="year", columns="region_out", values=value_col, aggfunc="mean",
    ).reset_index().rename(columns={"year": year_col})

    for col in existing.columns:
        if col not in wide.columns:
            wide[col] = float("nan")
    wide = wide[existing.columns]

    updated = pd.concat([existing, wide], ignore_index=True)
    updated = updated.sort_values(year_col).reset_index(drop=True)
    updated.to_csv(csv_path, index=False, float_format="%.5f")
    LOGGER.info("Appended year %d to %s.", year, csv_path.name)


# ============================================================================
# Beta Loading
# ============================================================================

def load_regional_betas(beta_path: Path,
                        region_order: list[str]) -> dict[str, float]:
    """
    Load regional beta coefficients from a CSV file.

    The beta file (cd_beta0.csv) contains electric-sector-only betas,
    representing the price sensitivity to regional electric sector demand:
        Beta_regional(r) in units of 2004$/MMBtu per Quad
    """
    require(beta_path.exists(), f"Regional beta file not found: {beta_path}")
    df = pd.read_csv(beta_path)
    cendiv_col = next((c for c in df.columns if normalize_token(c).endswith("cendiv")), None)
    value_col = next((c for c in df.columns if normalize_token(c) == "value"), None)
    require(cendiv_col is not None and value_col is not None,
            "Regional beta file missing cendiv/value columns.")
    df = df[[cendiv_col, value_col]].copy()
    df["cendiv"] = df[cendiv_col].map(output_label_to_cendiv)
    df["value"] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=["cendiv", "value"])
    betas = dict(zip(df["cendiv"], df["value"].astype(float)))
    missing = [c for c in region_order if c not in betas]
    require(not missing, f"Regional beta file missing regions: {missing}")
    return betas


def load_national_beta(beta_path: Path) -> float:
    """Load the national beta value from national_beta.csv."""
    require(beta_path.exists(), f"National beta file not found: {beta_path}")
    df = pd.read_csv(beta_path)
    require("beta" in df.columns, f"National beta file missing 'beta' column: {beta_path}")
    vals = pd.to_numeric(df["beta"], errors="coerce").dropna()
    require(not vals.empty, f"National beta file has no numeric beta value: {beta_path}")
    return float(vals.iloc[0])


# ============================================================================
# Alpha Computation (Core Calculation)
# ============================================================================

def compute_ng_alpha(
    price_2004: pd.DataFrame,
    demand_elec: pd.DataFrame,
    beta_regional: dict[str, float],
    beta_national: float,
    first_model_year: int,
) -> pd.DataFrame:
    """
    Compute NG alpha with a scenario-specific index alpha(region, year, scenario).

    For each scenario we compute an implied residual:

        alpha_2004 = price_2004 - beta_reg * q_reg - beta_nat * q_nat

    ReEDS sets both NG beta terms to zero in the first model year, so the
    first-year alpha must carry the full converted price level.

    Parameters
    ----------
    price_2004 : DataFrame
        Columns: [scenario_id, cendiv, year, price_2004]
    demand_elec : DataFrame
        Columns: [scenario_id, cendiv, year, demand_elec_quads]
    beta_regional : dict
        {cendiv: beta_value} for each census division
    beta_national : float
        National beta coefficient (2004$/MMBtu per Quad)
    first_model_year : int
        First modeled year in ReEDS. NG elasticity is disabled in this year.

    Returns
    -------
    DataFrame with columns: [scenario_id, cendiv, year, alpha_2004].
    """
    # Merge price and regional demand.
    merged = price_2004.merge(
        demand_elec,
        on=["scenario_id", "cendiv", "year"],
        how="inner",
    )

    # Compute national demand: sum of regional electric demands per scenario-year.
    q_nat = (
        demand_elec
        .groupby(["scenario_id", "year"], as_index=False)["demand_elec_quads"]
        .sum()
        .rename(columns={"demand_elec_quads": "q_nat"})
    )
    merged = merged.merge(q_nat, on=["scenario_id", "year"], how="left")

    # Map regional betas.
    merged["beta_reg"] = merged["cendiv"].map(beta_regional)
    require(
        not merged["beta_reg"].isna().any(),
        "Missing regional beta values while computing NG alpha.",
    )
    require(
        not merged["q_nat"].isna().any(),
        "Missing national demand values while computing NG alpha.",
    )

    # Scenario-level residual alpha.
    merged["alpha_2004"] = (
        merged["price_2004"]
        - merged["beta_reg"] * merged["demand_elec_quads"]
        - beta_national * merged["q_nat"]
    )
    first_year_mask = merged["year"] == first_model_year
    merged.loc[first_year_mask, "alpha_2004"] = merged.loc[first_year_mask, "price_2004"]
    out = merged[["scenario_id", "cendiv", "year", "alpha_2004"]].copy()
    out = out.sort_values(["scenario_id", "cendiv", "year"]).reset_index(drop=True)
    return out


# ============================================================================
# Output Writing
# ============================================================================
def pivot_ng_series(
    frame: pd.DataFrame,
    scenario_id: str,
    value_col: str,
    region_order: list[str],
) -> pd.DataFrame:
    """Pivot a long-format series into wide format (year Ã— region)."""
    subset = frame[frame["scenario_id"] == scenario_id].copy()
    subset["region_out"] = subset["cendiv"].map(cendiv_output_label)
    pivot = subset.pivot_table(
        index="year", columns="region_out",
        values=value_col, aggfunc="mean",
    )
    ordered_cols = [cendiv_output_label(c) for c in region_order]
    pivot = pivot.reindex(columns=ordered_cols)
    pivot = pivot.sort_index().reset_index()
    return pivot


def write_ng_outputs(
    scenario_table: pd.DataFrame,
    price_raw: pd.DataFrame,
    demand_elec: pd.DataFrame,
    demand_total: pd.DataFrame,
    alpha_2004: pd.DataFrame,
    beta_regional: dict[str, float],
    region_order: list[str],
    config: dict[str, Any],
    base_dir: Path,
) -> None:
    """
    Write all NG output files to the configured output directory.

    Output files per scenario:
        - ng_AEO_{year}_{suffix}.csv         : NG prices (AEO native dollar year)
        - ng_demand_AEO_{year}_{suffix}.csv   : Electric sector demand (Quads)
        - ng_tot_demand_AEO_{year}_{suffix}.csv : Total sector demand (Quads)
        - alpha_AEO_{year}_{suffix}.csv       : Alpha values (2004$/MMBtu)

    Shared output files:
        - cd_beta0.csv            : Electric sector regional betas
    """
    ng_cfg = config["ng"]
    aeo_year = int(config["aeo_year"])
    out_dir = resolve_path(base_dir, config["paths"]["output_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)

    written_files: list[Path] = []

    # --- Per-scenario output files ---
    output_specs = [
        ("ng_AEO",            price_raw,    "ng_price",            None),
        ("ng_demand_AEO",     demand_elec,  "demand_elec_quads",   None),
        ("ng_tot_demand_AEO", demand_total, "demand_total_quads",  None),
        ("alpha_AEO",         alpha_2004,   "alpha_2004",          {"year": "t"}),
    ]
    for row in scenario_table.itertuples(index=False):
        scenario_id = str(row.scenario_id)
        file_suffix = getattr(row, "file_suffix", None)
        require(bool(file_suffix),
                f"Missing output suffix for NG scenario '{scenario_id}'")
        suffix = str(file_suffix)
        for stem, frame, value_col, rename in output_specs:
            wide = pivot_ng_series(frame, scenario_id, value_col, region_order)
            if rename:
                wide = wide.rename(columns=rename)
            path = out_dir / f"{stem}_{aeo_year}_{suffix}.csv"
            wide.to_csv(path, index=False, float_format="%.6f")
            written_files.append(path)

    # --- Beta output files (copy from input) ---
    input_dir = resolve_path(
        base_dir,
        config["paths"].get("input_dir", config["paths"]["output_dir"]),
    )
    beta_files = ["cd_beta0.csv", "national_beta.csv"]
    for beta_name in beta_files:
        src = resolve_case_insensitive(input_dir / beta_name)
        dst = out_dir / beta_name
        if src.exists():
            shutil.copy2(src, dst)
            written_files.append(dst)
        else:
            LOGGER.warning("%s not found at %s; skipping copy.", beta_name, src)

    LOGGER.info("Wrote NG outputs to %s (%d files)", out_dir, len(written_files))


# ============================================================================
# Main Pipeline
# ============================================================================

def run_ng_pipeline(config: dict[str, Any], base_dir: Path) -> None:
    """
    Execute the full NG preprocessing pipeline:

    1. Connect to EIA API and discover available scenarios
    2. Fetch price & demand series for all scenarios and regions
    3. Backfill historical data for pre-projection years
    4. Validate completeness of all data
    5. Convert prices from AEO dollar year to 2004$ using the deflator
    6. Compute alpha values using the supply curve inversion:
           Alpha = Price_2004 - Beta_reg Ã— Q_reg - Beta_nat Ã— Q_nat
    7. Write output CSV files for ReEDS consumption
    """
    api_key = resolve_api_key(config)
    client = EiaClient(config["api"], api_key)

    ng_cfg = config["ng"]

    # Load shared region mapping from config
    global CENDIV_OUTPUT
    CENDIV_OUTPUT = {
        k.replace(" ", ""): v for k, v in ng_cfg["cendiv_and_label"].items()
    }

    aeo_year = int(config["aeo_year"])
    start_year = int(config["start_year"])
    end_year = int(config["end_year"])
    region_order = resolve_ng_region_order(config)
    out_dir = resolve_path(base_dir, config["paths"]["output_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = out_dir / "raw_aeo_data"
    raw_dir.mkdir(parents=True, exist_ok=True)

    scenarios_cfg = cfg_section(config, "scenarios")
    alpha_scen_cfg = cfg_section(scenarios_cfg, "alpha_regression")
    fetch_scenarios = [str(x).strip() for x in cfg_list(alpha_scen_cfg, "fetch") if str(x).strip()]
    output_aliases = resolve_output_scenario_aliases(
        aeo_year,
        alpha_scen_cfg.get("outputs"),
    )

    # ---- Step 1: Discover and resolve scenarios ----
    LOGGER.info("Step 1: Resolving AEO %d scenarios...", aeo_year)
    all_scenarios = resolve_ng_scenarios(
        client,
        aeo_year,
        include_scenarios=fetch_scenarios or None,
    )
    output_scenarios = resolve_output_scenarios(
        available_scenarios=all_scenarios,
        aeo_year=aeo_year,
        output_aliases=output_aliases,
    )
    pd.DataFrame(client.get_facets(aeo_year, "scenario")).to_csv(
        raw_dir / "scenario_facets.csv", index=False
    )
    all_scenarios.to_csv(raw_dir / "selected_scenarios_all.csv", index=False)
    output_scenarios.to_csv(raw_dir / "selected_scenarios_outputs.csv", index=False)
    scenario_ids = all_scenarios["scenario_id"].tolist()
    region_ids = list(
        resolve_region_ids(client, aeo_year, list(ng_cfg["cendiv_and_label"].keys())).values()
    )
    pd.DataFrame(client.get_facets(aeo_year, "regionId")).to_csv(
        raw_dir / "region_facets.csv", index=False
    )

    # ---- Step 2: Fetch price and demand data from EIA API ----
    LOGGER.info("Step 2: Fetching AEO data from EIA API...")
    fetch_specs = [
        ("price",        "ng_price",            "aeo_raw_ng_price.csv"),
        ("demand_elec",  "demand_elec_quads",   "aeo_raw_ng_demand_electric_power.csv"),
        ("demand_total", "demand_total_quads",  "aeo_raw_ng_demand_total.csv"),
    ]
    fetched: dict[str, pd.DataFrame] = {}
    for series_key, value_col, raw_filename in fetch_specs:
        fetched[series_key] = fetch_aeo_series_by_scenario(
            client, aeo_year, NG_SERIES_NAMES[series_key],
            scenario_ids, region_ids, value_col, start_year, end_year,
            raw_output_path=raw_dir / raw_filename,
        )
    price_raw = fetched["price"]
    demand_elec = fetched["demand_elec"]
    demand_total = fetched["demand_total"]

    # ---- Step 3: Backfill historical data ----
    LOGGER.info("Step 3: Backfilling historical data...")
    projection_start_year = int(min(
        price_raw["year"].min(),
        demand_elec["year"].min(),
        demand_total["year"].min(),
    ))
    validation_start_year = projection_start_year

    if start_year < projection_start_year:
        input_dir = resolve_path(
            base_dir,
            config["paths"].get("input_dir", config["paths"]["output_dir"]),
        )
        history_specs = [
            ("ng_AEO",            "ng_price",           price_raw),
            ("ng_demand_AEO",     "demand_elec_quads",  demand_elec),
            ("ng_tot_demand_AEO", "demand_total_quads", demand_total),
        ]
        try:
            backfilled: list[pd.DataFrame] = []
            for stem, value_col, frame in history_specs:
                hist_path = input_dir / f"{stem}_{HISTORY_SUFFIX}.csv"
                require(hist_path.exists(), f"History source file not found: {hist_path}")
                hist = load_history_wide_file(hist_path, value_col, region_order)
                backfilled.append(apply_reference_history_to_all_scenarios(
                    frame, value_col, hist, scenario_ids, projection_start_year,
                ))
            price_raw, demand_elec, demand_total = backfilled
            validation_start_year = start_year
            LOGGER.info(
                "Backfilled %d-%d from historical files in %s.",
                start_year, projection_start_year - 1, input_dir,
            )
        except Exception as exc:
            LOGGER.warning(
                "Could not backfill NG history (%s). "
                "Using available years %d-%d only.",
                exc, projection_start_year, end_year,
            )
            validation_start_year = projection_start_year

    # ---- Step 4: Filter and validate data completeness ----
    LOGGER.info("Step 4: Validating data completeness...")
    all_scenarios, series_frames = filter_complete_ng_scenarios(
        scenario_table=all_scenarios,
        series_frames={
            "price": price_raw,
            "demand_elec": demand_elec,
            "demand_total": demand_total,
        },
        region_order=region_order,
        start_year=validation_start_year,
        end_year=end_year,
    )
    price_raw = series_frames["price"]
    demand_elec = series_frames["demand_elec"]
    demand_total = series_frames["demand_total"]
    scenario_ids = all_scenarios["scenario_id"].tolist()
    output_scenarios = (
        output_scenarios[output_scenarios["scenario_id"].isin(scenario_ids)]
        .copy().reset_index(drop=True)
    )
    require(
        len(output_scenarios) == len(output_aliases),
        "One or more required output scenarios were dropped by coverage filtering.",
    )
    for label, frame in [("NG price", price_raw),
                         ("NG electric demand", demand_elec),
                         ("NG total demand", demand_total)]:
        validate_ng_coverage(frame, scenario_ids, region_order,
                             validation_start_year, end_year, label)

    # ---- Step 5: Convert prices to 2004$ ----
    LOGGER.info("Step 5: Converting prices to 2004$ (deflator=%.6f)...",
                float(ng_cfg["price_deflator_to_2004"]))
    deflator = float(ng_cfg["price_deflator_to_2004"])
    price_2004 = price_raw.copy()
    price_2004["price_2004"] = price_2004["ng_price"] * deflator

    # ---- Step 6: Compute alpha values ----
    LOGGER.info("Step 6: Computing NG alpha values...")
    input_dir = resolve_path(
        base_dir,
        config["paths"].get("input_dir", config["paths"]["output_dir"]),
    )
    beta_path = resolve_case_insensitive(input_dir / "cd_beta0.csv")
    beta_regional = load_regional_betas(beta_path, region_order)
    national_beta_path = resolve_case_insensitive(input_dir / "national_beta.csv")
    beta_national = load_national_beta(national_beta_path)
    LOGGER.info(
        "  Beta_national = %.10f (2004$/MMBtu per Quad, source=%s)",
        beta_national, national_beta_path,
    )
    LOGGER.info(
        "  Beta_regional: %s",
        {cendiv_output_label(k): f"{v:.6f}" for k, v in beta_regional.items()},
    )

    alpha_2004 = compute_ng_alpha(
        price_2004,
        demand_elec,
        beta_regional,
        beta_national,
        first_model_year=start_year,
    )
    validate_ng_coverage(
        alpha_2004, scenario_ids, region_order,
        validation_start_year, end_year, "NG alpha",
    )

    # ---- Step 7: Write output files ----
    LOGGER.info("Step 7: Writing output files...")
    write_ng_outputs(
        scenario_table=output_scenarios,
        price_raw=price_raw,
        demand_elec=demand_elec,
        demand_total=demand_total,
        alpha_2004=alpha_2004,
        beta_regional=beta_regional,
        region_order=region_order,
        config=config,
        base_dir=base_dir,
    )

    # ---- Step 8: Append projection_start_year to history CSVs ----
    ref_rows = output_scenarios[
        output_scenarios["file_suffix"] == "reference"
    ]
    if not ref_rows.empty:
        ref_sid = str(ref_rows.iloc[0]["scenario_id"])
        hist_dir = resolve_path(
            base_dir,
            config["paths"].get("input_dir", config["paths"]["output_dir"]),
        )
        for stem, vcol, df in [
            ("ng_AEO", "ng_price", price_raw),
            ("ng_demand_AEO", "demand_elec_quads", demand_elec),
            ("ng_tot_demand_AEO", "demand_total_quads", demand_total),
        ]:
            _append_year_to_history_csv(
                hist_dir / f"{stem}_{HISTORY_SUFFIX}.csv",
                projection_start_year, df, ref_sid, vcol,
            )


# ============================================================================
# Entry Point
# ============================================================================

def main() -> int:
    args = parse_args()
    setup_logging(args.log_level)

    script_dir = Path(__file__).resolve().parent
    cfg_path = resolve_config_path(args.config, script_dir)

    cfg = load_config(cfg_path)
    if args.aeo_year is not None:
        cfg["aeo_year"] = int(args.aeo_year)

    run_ng_pipeline(cfg, script_dir)
    LOGGER.info("NG pipeline complete.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        LOGGER.exception("Pipeline failed: %s", exc)
        sys.exit(1)


