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

    Price(r,t,s) = Alpha(r,t,s) + Beta_regional(r) Ã— Demand_regional(r,t,s)
                                 + Beta_national Ã— Demand_national(t,s)

Where:
    - Price(r,t,s)         : AEO NG price for region r, year t, scenario s (2024$/MMBtu)
    - Alpha(r,t,s)         : Intercept / base price component (2004$/MMBtu)
    - Beta_regional(r)     : Region-specific demand sensitivity (2004$/MMBtu per Quad)
    - Demand_regional(r,t,s) : Electric sector NG demand in region r (Quads)
    - Beta_national         : National demand sensitivity (2004$/MMBtu per Quad)
    - Demand_national(t,s) : Total national electric sector NG demand (Quads)

Alpha is solved as the residual after removing demand-driven price effects:

    Alpha(r,t,s) = Price(r,t,s) Ã— deflator - Beta_regional(r) Ã— Demand_regional(r,t,s)
                                            - Beta_national Ã— Demand_national(t,s)

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
- cd_beta0_allsector.csv            : All-sector regional betas

Usage
-----
    python aeo_alpha_regression.py --config aeo_pipeline_config.json

Example config: see aeo_pipeline_config.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning
from urllib3.util.retry import Retry

LOGGER = logging.getLogger("aeo_pipeline")

# ============================================================================
# Constants
# ============================================================================

# Mapping from normalized region names to canonical internal keys
CENDIV_CANONICAL = {
    "newengland": "NewEngland",
    "middleatlantic": "MiddleAtlantic",
    "eastnorthcentral": "EastNorthCentral",
    "westnorthcentral": "WestNorthCentral",
    "southatlantic": "SouthAtlantic",
    "eastsouthcentral": "EastSouthCentral",
    "westsouthcentral": "WestSouthCentral",
    "mountain": "Mountain",
    "pacific": "Pacific",
    "unitedstates": "UnitedStates",
}

# Mapping from internal keys to ReEDS output format (underscore-separated)
CENDIV_OUTPUT = {
    "NewEngland": "New_England",
    "MiddleAtlantic": "Mid_Atlantic",
    "EastNorthCentral": "East_North_Central",
    "WestNorthCentral": "West_North_Central",
    "SouthAtlantic": "South_Atlantic",
    "EastSouthCentral": "East_South_Central",
    "WestSouthCentral": "West_South_Central",
    "Mountain": "Mountain",
    "Pacific": "Pacific",
}

# EIA AEO series names for NG data
NG_SERIES_NAMES = {
    "price": "Energy Prices : Electric Power : Natural Gas",
    "demand_elec": "Energy Use : Electric Power : Natural Gas",
    "demand_total": "Energy Use : Total : Natural Gas",
}

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


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def require(condition: bool, message: str) -> None:
    """Assert a condition with a descriptive error message."""
    if not condition:
        raise ValueError(message)


def normalize_token(value: Any) -> str:
    """Normalize a string for case-insensitive, whitespace-insensitive matching."""
    if value is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "", str(value).replace("\xa0", " ").strip().lower())


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


def resolve_case_insensitive(path: Path) -> Path:
    """Resolve a path with case-insensitive matching on each component."""
    if path.exists():
        return path
    path = path.resolve()
    current = Path(path.anchor)
    for part in path.parts[1:]:
        if not current.exists():
            return path
        try:
            matches = [p for p in current.iterdir()
                       if p.name.lower() == part.lower()]
        except PermissionError:
            return path
        if not matches:
            return path
        current = matches[0]
    return current


def resolve_path(base_dir: Path, configured_path: str) -> Path:
    """Resolve a configured path relative to the base directory."""
    p = Path(configured_path)
    if not p.is_absolute():
        p = base_dir / p
    return resolve_case_insensitive(p)


def load_config(config_path: Path) -> dict[str, Any]:
    """Load and validate the JSON configuration file."""
    require(config_path.exists(), f"Config not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)
    require(isinstance(cfg, dict), f"Config root must be an object: {config_path}")
    return cfg


def resolve_api_key(config: dict[str, Any]) -> str:
    """Resolve the EIA API key from environment, config, or legacy fallback."""
    api_cfg = config["api"]
    env_var = api_cfg.get("key_env_var", "EIA_API_KEY")
    env_key = os.getenv(env_var, "").strip()
    if env_key:
        return env_key
    fallback = api_cfg.get("key_fallback", "").strip()
    if fallback:
        LOGGER.warning("Using key_fallback from config.")
        return fallback
    try:
        from _eia_api_functions import api_key as legacy_api_key  # type: ignore
        if legacy_api_key:
            LOGGER.warning("Using API key from _eia_api_functions.py fallback.")
            return str(legacy_api_key)
    except Exception:
        pass
    raise ValueError(f"Missing EIA API key. Set {env_var} or api.key_fallback.")


# ============================================================================
# EIA API Client
# ============================================================================

class EiaClient:
    """Client for fetching data from the EIA AEO API with retry logic."""

    def __init__(self, api_cfg: dict[str, Any], api_key: str):
        self.base_url = api_cfg["base_url"].rstrip("/")
        self.verify_ssl = bool(api_cfg.get("verify_ssl", True))
        self.timeout = int(api_cfg.get("timeout_seconds", 60))
        self.api_key = api_key
        if not self.verify_ssl:
            disable_warnings(InsecureRequestWarning)
        retries = Retry(
            total=int(api_cfg.get("max_retries", 4)),
            connect=int(api_cfg.get("max_retries", 4)),
            read=int(api_cfg.get("max_retries", 4)),
            status=int(api_cfg.get("max_retries", 4)),
            backoff_factor=float(api_cfg.get("backoff_seconds", 1.0)),
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        self.session = requests.Session()
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def get_json(self, path: str,
                 params: list[tuple[str, str]] | None = None) -> dict[str, Any]:
        full_path = path if path.startswith("/") else f"/{path}"
        query = [("api_key", self.api_key)]
        if params:
            query.extend(params)
        resp = self.session.get(
            f"{self.base_url}{full_path}",
            params=query,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        resp.raise_for_status()
        payload = resp.json()
        require("response" in payload,
                f"Unexpected payload from {path}: {payload}")
        return payload

    def get_facets(self, aeo_year: int, facet: str) -> list[dict[str, Any]]:
        return self.get_json(
            f"/aeo/{aeo_year}/facet/{facet}")["response"]["facets"]

    def get_data(self, path: str,
                 params: list[tuple[str, str]]) -> pd.DataFrame:
        payload = self.get_json(path, params)
        warnings = payload["response"].get("warnings", [])
        for w in warnings:
            LOGGER.warning("EIA warning: %s | %s",
                           w.get("warning"), w.get("description"))
        data = payload["response"].get("data", [])
        require(data, f"No data from endpoint {path}")
        return pd.DataFrame(data)


# ============================================================================
# EIA API Resolution Helpers
# ============================================================================

def resolve_region_ids(client: EiaClient, aeo_year: int,
                       regions: list[str]) -> dict[str, str]:
    """Map region display names to EIA region IDs."""
    facets = client.get_facets(aeo_year, "regionId")
    region_map = {normalize_token(item["name"]): str(item["id"])
                  for item in facets}
    out: dict[str, str] = {}
    for name in regions:
        key = normalize_token(name)
        require(key in region_map, f"Region not found in EIA facets: {name}")
        out[name] = region_map[key]
    return out


def resolve_series_ids(client: EiaClient, aeo_year: int,
                       series_name: str) -> list[str]:
    """Find EIA series IDs matching a given series name."""
    facets = client.get_facets(aeo_year, "seriesId")
    ids = [str(item["id"]) for item in facets
           if normalize_token(item.get("name")) == normalize_token(series_name)]
    require(ids, f"Series not found: {series_name}")
    return ids


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
    facets = client.get_facets(aeo_year, "scenario")
    rows: list[dict[str, str]] = []
    for item in facets:
        scenario_id = str(item.get("id", "")).strip()
        scenario_name = str(item.get("name", "")).strip()
        if not scenario_id:
            continue
        # Skip legacy composite IDs (e.g., "aeo2023ref")
        if normalize_token(scenario_id).startswith("aeo"):
            continue
        rows.append({"scenario_id": scenario_id,
                     "scenario_name": scenario_name})
    require(rows, "No NG scenarios left after filtering legacy IDs.")

    if include_scenarios:
        picked: list[dict[str, str]] = []
        missing: list[str] = []
        for raw in include_scenarios:
            token = normalize_token(str(raw).replace("{aeo_year}", str(aeo_year)))
            found = None
            for row in rows:
                sid = str(row["scenario_id"])
                sname = str(row["scenario_name"])
                if normalize_token(sid) == token or normalize_token(sname) == token:
                    found = row
                    break
            if found is None:
                missing.append(str(raw))
                continue
            if all(str(r["scenario_id"]) != str(found["scenario_id"]) for r in picked):
                picked.append(found)
        require(
            not missing,
            f"Configured alpha_regression.fetch scenarios not found: {missing}",
        )
        out = pd.DataFrame(picked)
        LOGGER.info(
            "Selected NG scenarios from config (%d): %s",
            len(out),
            out["scenario_id"].tolist(),
        )
        return out.reset_index(drop=True)

    out = (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["scenario_id"])
        .sort_values("scenario_id")
        .reset_index(drop=True)
    )
    LOGGER.info("Selected NG scenarios (%d): %s", len(out), out["scenario_id"].tolist())
    return out


def resolve_output_scenario_aliases(
    aeo_year: int,
    configured_outputs: Any,
) -> dict[str, list[str]]:
    """Build output scenario alias map from config, with defaults."""
    if configured_outputs is None:
        raw_map: dict[str, list[str]] = NG_OUTPUT_SCENARIOS
    elif isinstance(configured_outputs, dict):
        raw_map = {}
        for suffix, aliases in configured_outputs.items():
            require(
                isinstance(aliases, list),
                f"alpha_regression.outputs['{suffix}'] must be a list.",
            )
            raw_map[str(suffix)] = [str(a) for a in aliases]
    elif isinstance(configured_outputs, list):
        raw_map = {}
        for row in configured_outputs:
            require(
                isinstance(row, dict),
                "alpha_regression.outputs list entries must be objects.",
            )
            suffix = str(row.get("file_suffix", "")).strip()
            require(suffix, "alpha_regression.outputs entry missing file_suffix.")
            aliases = row.get("aliases", [])
            require(
                isinstance(aliases, list),
                f"alpha_regression.outputs entry '{suffix}' aliases must be a list.",
            )
            vals = [str(a).strip() for a in aliases if str(a).strip()]
            scenario_id = str(row.get("scenario_id", "")).strip()
            if scenario_id:
                vals.insert(0, scenario_id)
            require(vals, f"alpha_regression.outputs entry '{suffix}' has no aliases.")
            raw_map[suffix] = vals
    else:
        raise ValueError(
            "alpha_regression.outputs must be either an object or a list of objects."
        )

    out: dict[str, list[str]] = {}
    for suffix, aliases in raw_map.items():
        suffix_txt = str(suffix).strip()
        require(suffix_txt, "alpha_regression.outputs contains an empty file_suffix.")
        clean_aliases = [str(a).strip() for a in aliases if str(a).strip()]
        require(clean_aliases, f"alpha_regression.outputs['{suffix_txt}'] has no aliases.")
        # Keep original strings for logs and output; matching happens via normalize_token.
        out[suffix_txt] = clean_aliases
    require(out, "alpha_regression.outputs resolved to an empty mapping.")
    LOGGER.info(
        "Configured alpha output scenario map for AEO %d: %s",
        aeo_year,
        {k: v for k, v in out.items()},
    )
    return out


def resolve_output_scenarios(
    available_scenarios: pd.DataFrame,
    aeo_year: int,
    output_aliases: dict[str, list[str]],
) -> pd.DataFrame:
    """Match available scenarios to configured output scenario aliases."""
    require(not available_scenarios.empty,
            "No available scenarios to resolve NG output scenarios.")
    rows: list[dict[str, str]] = []
    for suffix, aliases in output_aliases.items():
        alias_tokens = {normalize_token(a.replace("{aeo_year}", str(aeo_year)))
                        for a in aliases}
        found_row = None
        for row in available_scenarios.itertuples(index=False):
            sid = str(row.scenario_id)
            sname = str(row.scenario_name)
            if (normalize_token(sid) in alias_tokens
                    or normalize_token(sname) in alias_tokens):
                found_row = row
                break
        require(found_row is not None,
                f"Could not resolve required NG output scenario: '{suffix}'")
        rows.append({
            "scenario_id": str(found_row.scenario_id),
            "scenario_name": str(found_row.scenario_name),
            "file_suffix": str(suffix),
        })
    out = pd.DataFrame(rows).reset_index(drop=True)
    require(
        out["scenario_id"].nunique() == len(out),
        "alpha_regression.outputs resolved to duplicate scenario_id values. "
        "Use distinct output scenarios.",
    )
    LOGGER.info("Resolved NG output scenarios: %s",
                out[["scenario_id", "file_suffix"]].to_dict(orient="records"))
    return out


def resolve_ng_region_order(config: dict[str, Any]) -> list[str]:
    """Get the ordered list of census divisions from the config."""
    configured = config["ng"]["regions"]
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

def resolve_history_file(input_dir: Path, stem: str,
                         history_suffix: str) -> Path:
    """Locate a historical data CSV file."""
    file_path = input_dir / f"{stem}_{history_suffix}.csv"
    require(file_path.exists(),
            f"History source file not found: {file_path}")
    return file_path


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

    A separate file (cd_beta0_allsector.csv) contains betas for total
    economy-wide NG demand, used when modeling all-sector price feedback.
    """
    require(beta_path.exists(),
            f"Regional beta file not found: {beta_path}")
    df = pd.read_csv(beta_path)
    cendiv_col = next(
        (c for c in df.columns if normalize_token(c).endswith("cendiv")),
        None,
    )
    value_col = next(
        (c for c in df.columns if normalize_token(c) == "value"),
        None,
    )
    require(cendiv_col is not None and value_col is not None,
            "Regional beta file missing cendiv/value columns.")
    df["cendiv"] = df[cendiv_col].map(output_label_to_cendiv)
    df["value"] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=["cendiv", "value"]).copy()
    betas = {row.cendiv: float(row.value)
             for row in df[["cendiv", "value"]].itertuples(index=False)}
    missing = [c for c in region_order if c not in betas]
    require(not missing, f"Regional beta file missing regions: {missing}")
    return betas


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
        - cd_beta0_allsector.csv  : All-sector regional betas (copied from input)
    """
    ng_cfg = config["ng"]
    aeo_year = int(config["aeo_year"])
    out_dir = resolve_path(base_dir, config["paths"]["output_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)

    written_files: list[Path] = []

    # --- Per-scenario output files ---
    for row in scenario_table.itertuples(index=False):
        scenario_id = str(row.scenario_id)
        file_suffix = getattr(row, "file_suffix", None)
        require(bool(file_suffix),
                f"Missing output suffix for NG scenario '{scenario_id}'")
        suffix = str(file_suffix)

        price_wide = pivot_ng_series(
            price_raw, scenario_id, "ng_price", region_order)
        elec_wide = pivot_ng_series(
            demand_elec, scenario_id, "demand_elec_quads", region_order)
        total_wide = pivot_ng_series(
            demand_total, scenario_id, "demand_total_quads", region_order)
        alpha_wide = pivot_ng_series(
            alpha_2004, scenario_id, "alpha_2004", region_order
        ).rename(columns={"year": "t"})

        fn_price = out_dir / f"ng_AEO_{aeo_year}_{suffix}.csv"
        fn_elec = out_dir / f"ng_demand_AEO_{aeo_year}_{suffix}.csv"
        fn_total = out_dir / f"ng_tot_demand_AEO_{aeo_year}_{suffix}.csv"
        fn_alpha = out_dir / f"alpha_AEO_{aeo_year}_{suffix}.csv"

        price_wide.to_csv(fn_price, index=False, float_format="%.6f")
        elec_wide.to_csv(fn_elec, index=False, float_format="%.6f")
        total_wide.to_csv(fn_total, index=False, float_format="%.6f")
        alpha_wide.to_csv(fn_alpha, index=False, float_format="%.6f")
        written_files.extend([fn_price, fn_elec, fn_total, fn_alpha])

    # --- Beta output files ---

    # Preserve the ordering from the source beta file if possible
    beta_order = list(region_order)
    try:
        beta_src = resolve_path(base_dir, ng_cfg["regional_beta_path"])
        if beta_src.exists():
            src_df = pd.read_csv(beta_src)
            cendiv_col = next(
                (c for c in src_df.columns
                 if normalize_token(c).endswith("cendiv")),
                None,
            )
            if cendiv_col:
                src_order = []
                for label in src_df[cendiv_col].tolist():
                    cendiv = output_label_to_cendiv(str(label))
                    if cendiv in beta_regional and cendiv not in src_order:
                        src_order.append(cendiv)
                if len(src_order) == len(region_order):
                    beta_order = src_order
    except Exception as exc:
        LOGGER.warning("Could not use regional beta source ordering: %s", exc)

    # cd_beta0.csv: electric sector betas
    beta_df = pd.DataFrame({
        "*cendiv": [cendiv_output_label(c) for c in beta_order],
        "value": [beta_regional[c] for c in beta_order],
    })
    beta_file = out_dir / "cd_beta0.csv"
    beta_df.to_csv(beta_file, index=False, float_format="%.6f")
    written_files.append(beta_file)

    # cd_beta0_allsector.csv: all-sector betas (copied from input if available)
    allsector_src = resolve_path(base_dir, ng_cfg["cd_beta0_allsector_path"])
    allsector_dst = out_dir / "cd_beta0_allsector.csv"
    if allsector_src.exists():
        shutil.copy2(allsector_src, allsector_dst)
    else:
        LOGGER.warning(
            "Configured cd_beta0_allsector source not found (%s). "
            "Writing cd_beta0 values instead.",
            allsector_src,
        )
        beta_df = pd.DataFrame({
            "*cendiv": [cendiv_output_label(c) for c in region_order],
            "value": [beta_regional[c] for c in region_order],
        })
        beta_df.to_csv(allsector_dst, index=False, float_format="%.6f")
    written_files.append(allsector_dst)

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
    aeo_year = int(config["aeo_year"])
    start_year = int(config["start_year"])
    end_year = int(config["end_year"])
    region_order = resolve_ng_region_order(config)
    out_dir = resolve_path(base_dir, config["paths"]["output_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = out_dir / "raw_aeo_data"
    raw_dir.mkdir(parents=True, exist_ok=True)

    scenarios_cfg = config.get("scenarios", {})
    if scenarios_cfg is None:
        scenarios_cfg = {}
    require(isinstance(scenarios_cfg, dict), "Config key 'scenarios' must be an object.")
    alpha_scen_cfg = scenarios_cfg.get("alpha_regression", {})
    if alpha_scen_cfg is None:
        alpha_scen_cfg = {}
    require(isinstance(alpha_scen_cfg, dict), "Config key 'scenarios.alpha_regression' must be an object.")

    fetch_cfg = alpha_scen_cfg.get("fetch")
    require(
        fetch_cfg is None or isinstance(fetch_cfg, list),
        "Config key 'scenarios.alpha_regression.fetch' must be a list when provided.",
    )
    fetch_scenarios = [str(x).strip() for x in (fetch_cfg or []) if str(x).strip()]
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
        resolve_region_ids(client, aeo_year, ng_cfg["regions"]).values()
    )
    pd.DataFrame(client.get_facets(aeo_year, "regionId")).to_csv(
        raw_dir / "region_facets.csv", index=False
    )

    # ---- Step 2: Fetch price and demand data from EIA API ----
    LOGGER.info("Step 2: Fetching AEO data from EIA API...")
    price_raw = fetch_aeo_series_by_scenario(
        client, aeo_year,
        NG_SERIES_NAMES["price"],
        scenario_ids, region_ids,
        "ng_price", start_year, end_year,
        raw_output_path=raw_dir / "aeo_raw_ng_price.csv",
    )
    demand_elec = fetch_aeo_series_by_scenario(
        client, aeo_year,
        NG_SERIES_NAMES["demand_elec"],
        scenario_ids, region_ids,
        "demand_elec_quads", start_year, end_year,
        raw_output_path=raw_dir / "aeo_raw_ng_demand_electric_power.csv",
    )
    demand_total = fetch_aeo_series_by_scenario(
        client, aeo_year,
        NG_SERIES_NAMES["demand_total"],
        scenario_ids, region_ids,
        "demand_total_quads", start_year, end_year,
        raw_output_path=raw_dir / "aeo_raw_ng_demand_total.csv",
    )

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
        try:
            history_price = load_history_wide_file(
                resolve_history_file(input_dir, "ng_AEO", HISTORY_SUFFIX),
                "ng_price", region_order,
            )
            history_elec = load_history_wide_file(
                resolve_history_file(
                    input_dir, "ng_demand_AEO", HISTORY_SUFFIX),
                "demand_elec_quads", region_order,
            )
            history_total = load_history_wide_file(
                resolve_history_file(
                    input_dir, "ng_tot_demand_AEO", HISTORY_SUFFIX),
                "demand_total_quads", region_order,
            )
            price_raw = apply_reference_history_to_all_scenarios(
                price_raw, "ng_price", history_price,
                scenario_ids, projection_start_year,
            )
            demand_elec = apply_reference_history_to_all_scenarios(
                demand_elec, "demand_elec_quads", history_elec,
                scenario_ids, projection_start_year,
            )
            demand_total = apply_reference_history_to_all_scenarios(
                demand_total, "demand_total_quads", history_total,
                scenario_ids, projection_start_year,
            )
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
    validate_ng_coverage(
        price_raw, scenario_ids, region_order,
        validation_start_year, end_year, "NG price",
    )
    validate_ng_coverage(
        demand_elec, scenario_ids, region_order,
        validation_start_year, end_year, "NG electric demand",
    )
    validate_ng_coverage(
        demand_total, scenario_ids, region_order,
        validation_start_year, end_year, "NG total demand",
    )

    # ---- Step 5: Convert prices to 2004$ ----
    LOGGER.info("Step 5: Converting prices to 2004$ (deflator=%.6f)...",
                float(ng_cfg["price_deflator_to_2004"]))
    deflator = float(ng_cfg["price_deflator_to_2004"])
    price_2004 = price_raw.copy()
    price_2004["price_2004"] = price_2004["ng_price"] * deflator

    # ---- Step 6: Compute alpha values ----
    LOGGER.info("Step 6: Computing NG alpha values...")
    beta_path = resolve_path(base_dir, ng_cfg["regional_beta_path"])
    beta_regional = load_regional_betas(beta_path, region_order)
    national_beta_path = resolve_case_insensitive(beta_path.parent / "national_beta.csv")
    require(
        national_beta_path.exists(),
        f"National beta file not found: {national_beta_path}",
    )
    national_beta_df = pd.read_csv(national_beta_path)
    require(
        "beta" in national_beta_df.columns,
        f"National beta file missing 'beta' column: {national_beta_path}",
    )
    beta_vals = pd.to_numeric(national_beta_df["beta"], errors="coerce").dropna()
    require(
        not beta_vals.empty,
        f"National beta file has no numeric beta value: {national_beta_path}",
    )
    beta_national = float(beta_vals.iloc[0])
    LOGGER.info(
        "  Beta_national = %.10f (2004$/MMBtu per Quad, source=%s)",
        beta_national,
        national_beta_path,
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


# ============================================================================
# Entry Point
# ============================================================================

def main() -> int:
    args = parse_args()
    setup_logging(args.log_level)

    script_dir = Path(__file__).resolve().parent
    cfg_path = Path(args.config)
    if not cfg_path.is_absolute():
        cwd_candidate = resolve_case_insensitive(
            (Path.cwd() / cfg_path).resolve())
        script_candidate = resolve_case_insensitive(
            (script_dir / cfg_path).resolve())
        cfg_path = (cwd_candidate if cwd_candidate.exists()
                    else script_candidate)

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


