"""
Shared helpers for the AEO natural gas price regression pipeline.

Consumed by:
    - aeo_alpha_regression.py
    - aeo_beta_regression.py
    - sync_beta_to_alpha_inputs.py
    - visualization.py

Only utilities that are duplicated across two or more of those scripts live
here. Regression-specific logic (alpha computation, beta estimation,
plotting, output writing) stays in its respective script.
"""

from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning
from urllib3.util.retry import Retry

LOGGER = logging.getLogger("aeo_functions")


# ============================================================================
# Constants
# ============================================================================

# EIA AEO series names for natural gas data.
NG_SERIES_NAMES = {
    "price": "Energy Prices : Electric Power : Natural Gas",
    "demand_elec": "Energy Use : Electric Power : Natural Gas",
    "demand_total": "Energy Use : Total : Natural Gas",
}

# Mapping from normalized region names to canonical internal cendiv keys.
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


# ============================================================================
# Small utilities
# ============================================================================

def require(condition: bool, message: str) -> None:
    """Raise ValueError when condition is false."""
    if not condition:
        raise ValueError(message)


def normalize_token(value: Any) -> str:
    """Normalize a string for case-insensitive matching by stripping all
    non-alphanumeric characters and lowercasing.
    E.g. 'High Oil and Gas Supply' -> 'highoilandgassupply'.
    """
    if value is None:
        return ""
    return re.sub(
        r"[^a-z0-9]+", "",
        str(value).replace("\xa0", " ").strip().lower(),
    )


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


# ============================================================================
# Path & config helpers
# ============================================================================

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


def resolve_config_path(config_arg: str, script_dir: Path) -> Path:
    """Locate a config file by searching CWD first, then script directory."""
    cfg_path = Path(config_arg)
    if cfg_path.is_absolute():
        return resolve_case_insensitive(cfg_path)
    cwd_candidate = resolve_case_insensitive((Path.cwd() / cfg_path).resolve())
    if cwd_candidate.exists():
        return cwd_candidate
    return resolve_case_insensitive((script_dir / cfg_path).resolve())


def load_config(config_path: Path) -> dict[str, Any]:
    """Load and validate a JSON configuration file."""
    require(config_path.exists(), f"Config not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)
    require(isinstance(cfg, dict),
            f"Config root must be an object: {config_path}")
    return cfg


def cfg_section(cfg: dict[str, Any], key: str) -> dict[str, Any]:
    """Get a sub-dict from cfg. Treats missing or null as empty dict.
    Raises ValueError if present-but-not-an-object.
    """
    val = cfg.get(key)
    if val is None:
        return {}
    require(isinstance(val, dict), f"Config key '{key}' must be an object.")
    return val


def cfg_list(cfg: dict[str, Any], key: str) -> list[Any]:
    """Get a list from cfg. Missing/null becomes []. Raises if wrong type."""
    val = cfg.get(key)
    if val is None:
        return []
    require(isinstance(val, list), f"Config key '{key}' must be a list.")
    return val


# ============================================================================
# API key resolution
# ============================================================================

def resolve_api_key(config: dict[str, Any]) -> str:
    """Resolve the EIA API key from environment, config, or legacy fallback."""
    api_cfg = config["api"]
    env_var = api_cfg.get("key_env_var", "EIA_API_KEY")
    env_key = os.getenv(env_var, "").strip()
    if env_key:
        return env_key
    fallback = str(api_cfg.get("key_fallback", "")).strip()
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
        max_retries = int(api_cfg.get("max_retries", 4))
        retries = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            status=max_retries,
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
        query: list[tuple[str, str]] = [("api_key", self.api_key)]
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

    def get_data(self, path_or_year: Any,
                 params: list[tuple[str, str]]) -> pd.DataFrame:
        """Fetch a /data endpoint as a DataFrame.

        Accepts either an explicit endpoint path (e.g. '/aeo/2025/data')
        or just the AEO year as int.
        """
        if isinstance(path_or_year, int):
            path = f"/aeo/{path_or_year}/data"
        else:
            path = str(path_or_year)
        payload = self.get_json(path, params)
        for w in payload["response"].get("warnings", []):
            LOGGER.warning("EIA warning: %s | %s",
                           w.get("warning"), w.get("description"))
        data = payload["response"].get("data", [])
        require(bool(data), f"No data from endpoint {path}")
        return pd.DataFrame(data)


# ============================================================================
# EIA facet resolution helpers
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
    require(bool(ids), f"Series not found: {series_name}")
    return ids


# ============================================================================
# AEO scenario discovery
# ============================================================================

def list_aeo_scenarios(client: EiaClient, aeo_year: int) -> list[dict[str, str]]:
    """Return [{scenario_id, scenario_name}, ...] for an AEO year, dropping
    legacy composite IDs (e.g. 'aeo2023ref') and blanks."""
    rows: list[dict[str, str]] = []
    for item in client.get_facets(aeo_year, "scenario"):
        sid = str(item.get("id", "")).strip()
        sname = str(item.get("name", "")).strip()
        if not sid or normalize_token(sid).startswith("aeo"):
            continue
        rows.append({"scenario_id": sid, "scenario_name": sname})
    require(bool(rows), "No AEO scenarios available after filtering legacy IDs.")
    return rows


def match_scenario(rows: list[dict[str, str]], alias: str, aeo_year: int) -> dict[str, str] | None:
    """Match an alias (e.g. 'ref{aeo_year}', 'highogs', 'High Oil and Gas Supply')
    to a scenario row by id or name (case/whitespace-insensitive). Returns None if no match.
    """
    token = normalize_token(str(alias).replace("{aeo_year}", str(aeo_year)))
    for row in rows:
        if (normalize_token(row["scenario_id"]) == token
                or normalize_token(row["scenario_name"]) == token):
            return row
    return None
