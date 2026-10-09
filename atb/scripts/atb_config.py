"""Shared configuration and path helpers for the ATB workflow."""

from pathlib import Path

import yaml


SCRIPT_DIR = Path(__file__).resolve().parent
ATB_DIR = SCRIPT_DIR.parent
DEFAULT_CONFIG_PATH = ATB_DIR / "config.yaml"
TECH_SETTINGS_PATH = SCRIPT_DIR / "settings.yaml"

ATBE_COLUMN_MAPPING = {
    "core_metric_parameter": "Parameter",
    "core_metric_case": "Case",
    "tax_credit_case": "TaxCreditCase",
    "crpyears": "CRPYears",
    "technology": "Technology",
    "technology_alias": "TechnologyAlias",
    "techdetail": "TechDetail",
    "techdetail2": "TechDetail2",
    "resourcedetail": "ResourceDetail",
    "display_name": "DisplayName",
    "scenario": "Scenario",
    "core_metric_variable": "variable",
}

# future_data values naming an ATB release file, and the raw_data entry each reads.
ATB_FUTURE_SOURCES = {"atb_flatfile": "flat_file", "atb_workbook": "workbook"}


def technology_atb_source(tech, future_data):
    """Return the raw ATB file (``flat_file`` or ``workbook``) one technology reads.

    ``future_data`` is the technology's config entry: one value, or one value
    per sub-technology. Every ATB-backed series must name the same file, so a
    technology is never assembled from both. ``manual`` series do not count.
    """
    label = f"processing.smooth_cost_curves.technologies.{tech}.future_data"
    if future_data is None:
        return "flat_file"
    values = future_data.values() if isinstance(future_data, dict) else [future_data]
    unknown = sorted({str(v) for v in values if v not in ATB_FUTURE_SOURCES and v != "manual"})
    if unknown:
        raise ValueError(
            f"{label} has unknown values {unknown}; choose from "
            f"{sorted(ATB_FUTURE_SOURCES)} or 'manual'."
        )
    files = sorted({ATB_FUTURE_SOURCES[v] for v in values if v in ATB_FUTURE_SOURCES})
    if len(files) > 1:
        raise ValueError(
            f"{label} mixes {files}; every ATB series of a technology must name the same file."
        )
    return files[0] if files else "flat_file"


def load_config(config_path=None):
    """Load the user-facing workflow configuration."""
    path = Path(config_path).resolve() if config_path else DEFAULT_CONFIG_PATH
    with path.open(encoding="utf-8") as stream:
        config = yaml.safe_load(stream)
    config["_config_path"] = str(path)
    return config


def resolve_atb_path(value):
    """Resolve an absolute path or a path relative to the atb directory."""
    path = Path(value)
    return path if path.is_absolute() else ATB_DIR / path


def raw_file_path(config, kind):
    """Return the configured local path for ``flat_file`` or ``workbook``."""
    raw = config["raw_data"]
    return resolve_atb_path(raw["directory"]) / raw[kind]["filename"]


def load_processing_settings(config_path=None):
    """Merge user config with the internal per-technology settings."""
    config = load_config(config_path)
    with TECH_SETTINGS_PATH.open(encoding="utf-8") as stream:
        settings = yaml.safe_load(stream)

    processing = config["processing"]
    processing.setdefault('smooth_cost_curves', {}).setdefault(
        'projection_start_year', int(config['atb']['year']) - 2)
    cases = processing.get('atb_case_overrides', {})
    unknown = set(cases) - set(settings['techs'])
    if unknown:
        raise ValueError(f'Unknown ATB case override technologies: {sorted(unknown)}')
    configured = processing['smooth_cost_curves'].get('technologies') or {}
    for tech, rules in settings['techs'].items():
        case = cases.get(tech, processing.get('atb_case'))
        if case is not None:
            rules['subset_rows']['Case'] = case
        # Which raw ATB file the loader reads this technology from.
        rules['atb_source'] = technology_atb_source(
            tech, (configured.get(tech) or {}).get('future_data')
        )
    history = config["historical_data"]
    settings.update(
        {
            "atbyear": config["atb"]["year"],
            "dollaryear": config["atb"]["dollar_year"],
            "reedspath": str(Path(processing["reeds_repo"])),
            "copy_to_reeds": bool(processing["copy_to_reeds"]),
            "reeds_start_year": int(processing["reeds_start_year"]),
            "reeds_end_year": int(processing["reeds_end_year"]) if "reeds_end_year" in processing else None,
            "decimals": int(processing["decimals"]),
            "output_dir": str(resolve_atb_path(processing["output_directory"])),
            "flat_file_path": str(raw_file_path(config, "flat_file")),
            "workbook_path": str(raw_file_path(config, "workbook")),
            "history_dir": str(resolve_atb_path(history["directory"])),
            "history_dollar_year": int(history["dollar_year"]),
            "config": config,
        }
    )
    return settings
