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
    settings.update(
        {
            "atbyear": config["atb"]["year"],
            "dollaryear": config["atb"]["dollar_year"],
            "reedspath": str(Path(processing["reeds_repo"])),
            "copy_to_reeds": bool(processing["copy_to_reeds"]),
            "reeds_start_year": int(processing["reeds_start_year"]),
            "decimals": int(processing["decimals"]),
            "output_dir": str(resolve_atb_path(processing["output_directory"])),
            "flat_file_path": str(raw_file_path(config, "flat_file")),
            "workbook_path": str(raw_file_path(config, "workbook")),
            "config": config,
        }
    )
    return settings
