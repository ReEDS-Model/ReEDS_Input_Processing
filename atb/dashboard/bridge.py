"""Thin layer between the dashboard and the existing ATB pipeline.

Everything that formats, validates, or labels data is imported from
``scripts/`` and ``comparison/``; this module only wires those calls together
and reads option lists from ``config.schema.json`` so no rule is typed twice.

Two kinds of run exist:

* a **preview** formats one technology (or all of them) with the draft config
  into a scratch folder under the system temp directory, for the plots;
* an **export** saves the draft to ``config.yaml`` and runs the pipeline
  stages exactly as ``scripts/run_pipeline.py`` does, so the real outputs in
  ``output/`` (and ReEDS, when ``copy_to_reeds`` is on) always match the
  saved config.
"""

from __future__ import annotations

import contextlib
import io
import json
import re
import subprocess
import sys
import tempfile
import types
from pathlib import Path
from unittest import mock

import pandas as pd
from ruamel.yaml import YAML

DASHBOARD_DIR = Path(__file__).resolve().parent
ATB_DIR = DASHBOARD_DIR.parent
CONFIG_PATH = ATB_DIR / "config.yaml"
SCHEMA_PATH = ATB_DIR / "config.schema.json"
# Scratch files nobody needs to keep: the draft config a preview runs with,
# the preview CSVs, and the pre-smoothing copies the provenance labels read.
WORK_DIR = Path(tempfile.gettempdir()) / "atb_dashboard"
PREVIEW_DIR = WORK_DIR / "preview"
UNSMOOTHED_DIR = WORK_DIR / "unsmoothed"
DRAFT_CONFIG = WORK_DIR / "config_dashboard.yaml"

for folder in (ATB_DIR / "scripts", ATB_DIR / "scripts" / "helpers", ATB_DIR / "comparison"):
    if str(folder) not in sys.path:
        sys.path.insert(0, str(folder))

import ccs_retrofit_inputs  # noqa: E402
from atb_config import ATB_FUTURE_SOURCES, load_processing_settings, resolve_atb_path  # noqa: E402
from compare_atb_outputs import (  # noqa: E402
    BASELINE_CATEGORY,
    METRIC_LABELS,
    PROJECTION_CATEGORIES,
    SCENARIO_COLORS,
    SCENARIO_ORDER,
    grouped_series,
    normalize_frame,
    provenance_categories,
    resolve_historical_mode,
    series_columns,
    smoothing_provenance,
)
from generate_atb_files import (  # noqa: E402
    load_atb_data,
    process_tech_file,
    read_atb_flat_file,
)
from historical_data import load_history  # noqa: E402
from run_pipeline import SCRIPT_DIR, STAGES  # noqa: E402

TECHNOLOGIES_PATH = ("processing", "smooth_cost_curves", "technologies")
# Not a run_pipeline.py stage: the dashboard runs scripts/ccs_retrofit_inputs.py after format.
CCS_STAGE = "ccs retrofit"


# --------------------------------------------------------------------------- config
def yaml_handler() -> YAML:
    """Round-trip YAML so comments in config.yaml survive a save."""
    handler = YAML()
    handler.preserve_quotes = True
    handler.width = 4096
    # Match config.yaml's layout: list items indented under their key, and an
    # explicit ``null`` instead of an empty value.
    handler.indent(mapping=2, sequence=4, offset=2)
    handler.representer.add_representer(
        type(None), lambda representer, _: representer.represent_scalar("tag:yaml.org,2002:null", "null")
    )
    return handler


def load_config(path: Path = CONFIG_PATH):
    """Read config.yaml, keeping its comments."""
    with Path(path).open(encoding="utf-8") as stream:
        return yaml_handler().load(stream)


def dump_config(config) -> str:
    """Return the config as YAML text."""
    buffer = io.StringIO()
    yaml_handler().dump(config, buffer)
    return buffer.getvalue()


def write_config(config, path: Path) -> Path:
    """Write the config to a file and return its path."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dump_config(config), encoding="utf-8", newline="\n")
    return path


def load_default_config():
    """Return the default config: config.yaml as committed in git, or the file on disk without git."""
    try:
        shown = subprocess.run(["git", "show", "HEAD:./config.yaml"], cwd=str(ATB_DIR),
                               capture_output=True, text=True, encoding="utf-8")
    except OSError:
        return load_config()
    return yaml_handler().load(shown.stdout) if shown.returncode == 0 else load_config()


def _flatten(node, path: tuple = ()):
    """Yield (path, value) for every setting of a config."""
    if isinstance(node, dict):
        for key, value in node.items():
            yield from _flatten(value, path + (str(key),))
    else:
        yield path, node


def config_changes(default, config) -> pd.DataFrame:
    """One row per setting whose value differs from the default config."""
    before, after = dict(_flatten(default)), dict(_flatten(config))
    depth = len(TECHNOLOGIES_PATH)
    rows = [
        {"setting": " > ".join(path[depth:] if path[:depth] == TECHNOLOGIES_PATH else path),
         "default": str(before.get(path, "(not set)")), "now": str(after.get(path, "(not set)"))}
        for path in {**before, **after} if before.get(path) != after.get(path)
    ]
    return pd.DataFrame(rows, columns=["setting", "default", "now"])


def technologies(config) -> list[str]:
    """Return the technology names configured under smooth_cost_curves."""
    node = config
    for key in TECHNOLOGIES_PATH:
        node = node[key]
    return list(node)


def technology_config(config, tech: str):
    """Return one technology's config node."""
    node = config
    for key in TECHNOLOGIES_PATH:
        node = node[key]
    return node[tech]


# --------------------------------------------------------------------------- schema
def load_schema(path: Path = SCHEMA_PATH) -> dict:
    """Read config.schema.json."""
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _deref(schema: dict, node):
    """Follow $ref links until reaching a schema definition."""
    while isinstance(node, dict) and "$ref" in node:
        node = schema["definitions"][node["$ref"].rsplit("/", 1)[1]]
    return node


def _enum(schema: dict, node) -> list | None:
    """Return the values a schema node allows (enum or const), or None."""
    node = _deref(schema, node)
    if not isinstance(node, dict):
        return None
    if "enum" in node:
        return list(node["enum"])
    if "const" in node:
        return [node["const"]]
    return None


def _technology_properties(schema: dict, tech: str) -> dict:
    """Return the schema properties of one technology."""
    technologies_schema = schema["definitions"]["smoothing"]["properties"]["technologies"]["properties"]
    node = _deref(schema, technologies_schema[tech])
    properties = {}
    for part in node.get("allOf", [node]):
        properties.update(_deref(schema, part).get("properties", {}))
    return properties


def _options(schema: dict, node, cls: str | None) -> list | None:
    """Return the allowed values of a schema node, for one sub-technology class or for the whole entry."""
    node = _deref(schema, node)
    candidates = node.get("oneOf", [node]) if isinstance(node, dict) else []
    for candidate in candidates:
        candidate = _deref(schema, candidate)
        if cls is None:
            values = _enum(schema, candidate)
        else:
            values = _enum(schema, candidate.get("properties", {}).get(cls))
        if values:
            return values
    return None


def history_options(schema: dict, tech: str, metric: str, cls: str | None) -> list | None:
    """Allowed historical_data modes for one metric of one sub-technology."""
    history = _deref(schema, _technology_properties(schema, tech)["historical_data"])
    return _options(schema, history.get("properties", {}).get(metric), cls)


def future_options(schema: dict, tech: str, cls: str | None) -> list | None:
    """Allowed future_data sources for one sub-technology."""
    return _options(schema, _technology_properties(schema, tech)["future_data"], cls)


# --------------------------------------------------------------------------- settings table
def _cell(config, tech: str, column: str, cls: str | None):
    """Return the mapping and key holding one setting, or (None, None) if the technology has no such setting.

    A setting is stored per series (a mapping) or once for the whole technology.
    """
    node = technology_config(config, tech)
    parent = node if column == "future_data" else node["historical_data"]
    if column not in parent:
        return None, None
    return (parent[column], cls) if isinstance(parent[column], dict) else (parent, column)


def _cell_options(schema: dict, tech: str, column: str, cls: str | None) -> list:
    """Return the values the schema allows for one setting of one series."""
    if column == "future_data":
        return future_options(schema, tech, cls) or []
    return history_options(schema, tech, column, cls) or []


def series_settings(config, tech: str) -> pd.DataFrame:
    """One row per series of a technology: its projection source and each metric's history mode."""
    node = technology_config(config, tech)
    future = node["future_data"]
    rows = []
    for cls in (list(future) if isinstance(future, dict) else [None]):
        row = {"series": cls or "*"}
        for column in ("future_data", *node["historical_data"]):
            holder, key = _cell(config, tech, column, cls)
            value = holder.get(key)
            row[column] = None if value is None else str(value)
        rows.append(row)
    return pd.DataFrame(rows)


def series_options(schema: dict, tech: str, table: pd.DataFrame) -> dict[str, list]:
    """Return the values the schema allows in each settings column, across the technology's series."""
    options = {}
    for column in table.columns[1:]:
        values = []
        for series, current in zip(table["series"], table[column]):
            for value in _cell_options(schema, tech, column, None if series == "*" else series) or [current]:
                if value is not None and value not in values:
                    values.append(value)
        options[column] = values
    return options


def apply_series_settings(config, schema: dict, tech: str, table: pd.DataFrame) -> list[str]:
    """Write an edited settings table into the config; return a message per value the schema rejects."""
    rejected = []
    for row in table.to_dict("records"):
        cls = None if row["series"] == "*" else row["series"]
        for column in table.columns[1:]:
            new = None if pd.isna(row[column]) else row[column]
            holder, key = _cell(config, tech, column, cls)
            current = holder.get(key) if holder is not None else None
            if new == current:
                continue
            options = _cell_options(schema, tech, column, cls) if current is not None else []
            if new in options:
                holder[key] = new
            else:
                rejected.append(f"{tech} {row['series']} {column}: {new} is not allowed here"
                                + (f" (options: {', '.join(options)})." if options else "."))
    return rejected


def set_optional(parent, name: str, tech: str, value) -> None:
    """Set one technology's entry of an optional mapping such as unavailable_before; None removes it."""
    if value is None:
        if tech in parent.get(name, {}):
            del parent[name][tech]
    elif parent.get(name, {}).get(tech) != value:
        parent.setdefault(name, {})[tech] = value


def preview_key(config, tech: str) -> str:
    """Return text that changes when a setting that affects this technology's outputs changes."""
    data = json.loads(json.dumps(config, default=str))
    processing = data["processing"]
    smoothing = processing["smooth_cost_curves"]
    smoothing["technologies"] = smoothing["technologies"][tech]
    for mapping in (smoothing.get("unavailable_before", {}), processing.get("atb_case_overrides", {})):
        for other in [name for name in mapping if name != tech]:
            del mapping[other]
    for name in ("workflow", "plotting"):
        data.pop(name, None)
    for name in ("copy_to_reeds", "technologies", "reeds_atb_year"):
        processing.pop(name, None)
    return json.dumps(data, sort_keys=True)


# --------------------------------------------------------------------------- preview
def flat_file_path(config_path: Path = CONFIG_PATH) -> Path:
    """Return the configured ATB flat file path."""
    return Path(load_processing_settings(config_path)["flat_file_path"])


def read_flat_file(config_path: Path = CONFIG_PATH) -> pd.DataFrame:
    """Read the raw flat file once; the app caches the result."""
    return read_atb_flat_file(load_processing_settings(config_path))


def baseline_year_range(config_path: Path = CONFIG_PATH) -> tuple[int, int]:
    """First and last year of the prepared ReEDS baseline (inputs/historical/reeds_baseline.csv).

    Outputs cannot start before the baseline begins, so the dashboard uses the
    first year as the lower bound of ``reeds_start_year``.
    """
    years = load_history(load_processing_settings(config_path), "reeds_baseline")["year"]
    return int(years.min()), int(years.max())


def _clear_technology_files(folder: Path, prefix: str) -> None:
    """Remove this technology's earlier preview files, one by one."""
    if folder.is_dir():
        for path in folder.glob(f"{prefix}_ATB_*_*.csv"):
            path.unlink()


def preview_settings(config) -> dict:
    """Pipeline settings for the draft, pointed at the scratch folders."""
    write_config(config, DRAFT_CONFIG)
    settings = load_processing_settings(DRAFT_CONFIG)
    settings["output_dir"] = str(PREVIEW_DIR)
    settings["unsmoothed_output_dir"] = str(UNSMOOTHED_DIR)
    PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
    UNSMOOTHED_DIR.mkdir(parents=True, exist_ok=True)
    return settings


def run_technology(config, tech: str, flat_file: pd.DataFrame, settings: dict | None = None) -> dict:
    """Format one technology with the draft config into the scratch folder.

    The draft goes through the pipeline's own ``load_processing_settings``, so
    every validation the command line applies also applies here.
    """
    settings = settings or preview_settings(config)
    prefix = settings["techs"][tech].get("reeds_name", tech)
    _clear_technology_files(PREVIEW_DIR, prefix)
    _clear_technology_files(UNSMOOTHED_DIR, prefix)

    reeds = Path(settings["reedspath"])
    dollaryear = pd.read_csv(reeds / "inputs" / "plant_characteristics" / "dollaryear.csv",
                             index_col="Scenario").squeeze()
    deflator = pd.read_csv(reeds / "inputs" / "financials" / "deflator.csv",
                           index_col="*Dollar.Year").squeeze()
    for kind in ("real", "reeds_baseline"):
        load_history(settings, kind)

    args = types.SimpleNamespace(debug=False)
    filenames: list[str] = []
    log = io.StringIO()

    def refuse(prompt=""):
        """The pipeline asks on the terminal when the ATB case is missing; a page cannot answer."""
        raise ValueError(" ".join(log.getvalue().splitlines()[-2:]))

    with contextlib.redirect_stdout(log), mock.patch("builtins.input", refuse):
        atb_data = load_atb_data(settings, args, [tech], atb_data_in=flat_file, report_changes=False)
        process_tech_file(atb_data, tech, settings, filenames, dollaryear, deflator,
                          settings["config"]["processing"].get("sensitivity_name"), str(PREVIEW_DIR), args)
    result = {"settings": settings, "files": [PREVIEW_DIR / name for name in filenames]}
    result["points"] = series_table(result)
    return result


# --------------------------------------------------------------------------- CCS retrofit
def ccs_retrofit_key(config) -> str:
    """Return text that changes when a setting that affects the CCS retrofit files changes."""
    processing = config["processing"]
    names = ("atb_case", "reeds_repo", "reeds_start_year", "reeds_end_year", "decimals")
    return json.dumps([config["atb"], config["raw_data"], {name: processing.get(name) for name in names}],
                      default=str, sort_keys=True)


def read_retrofit_costs(config) -> pd.DataFrame:
    """Read the ATB retrofit costs the CCS retrofit files are built from (slow; the app caches it)."""
    with contextlib.redirect_stdout(io.StringIO()):
        return ccs_retrofit_inputs.read_retrofit_costs(preview_settings(config))


def ccs_retrofit_preview(config, costs: pd.DataFrame) -> dict:
    """Build the CCS retrofit files with the draft config, without writing them.

    Returns the file names, the ATB series behind each fuel, and a table of
    every value with columns output, fuel, scenario, t, value.
    """
    settings = preview_settings(config)
    rules = settings["ccs_retrofits"]
    costs = ccs_retrofit_inputs.extend_years(
        costs, settings["reeds_start_year"], settings["reeds_end_year"] or int(costs.columns.max()))
    multipliers = ccs_retrofit_inputs.build_multipliers(costs, settings)
    cost_tables = ccs_retrofit_inputs.build_costs(costs, settings)
    deltas = pd.read_csv(Path(settings["reedspath"]) / "inputs" / "upgrades" / "upgrade_link.csv",
                         index_col="*TO")["DELTA"]
    frames = []
    for scenario, table in multipliers.items():
        for fuel, mapping in rules["fuels"].items():
            # Every upgrade row of a fuel carries the same multipliers; take the first.
            row = next(name for name, delta in deltas.items() if re.search(mapping["delta_pattern"], delta))
            frames.append(pd.DataFrame({"output": "multiplier", "fuel": fuel, "scenario": scenario.lower(),
                                        "t": table.columns, "value": table.loc[row].to_numpy()}))
    for fuel, table in cost_tables.items():
        for column in table.columns:
            frames.append(pd.DataFrame({"output": "cost", "fuel": fuel, "scenario": column.split("_", 1)[1],
                                        "t": table.index, "value": table[column].to_numpy()}))
    yy = str(settings["atbyear"])[-2:]
    return {
        "points": pd.concat(frames, ignore_index=True),
        "files": [f"upgrade_mult_atb{yy}_ccs_{suffix}.csv" for suffix in rules["scenario_suffix"].values()]
        + [f"upgrade_costs_ccs_{fuel}.csv" for fuel in rules["fuels"]],
        "series": {fuel: mapping["DisplayName"] for fuel, mapping in rules["fuels"].items()},
        "normalization": f"{rules['normalization_scenario']} in {rules['normalization_year']}",
    }


# --------------------------------------------------------------------------- export
def stage_command(stage: str, config_path: Path = CONFIG_PATH, unsmoothed_dir: Path | None = None) -> list[str]:
    """The command ``run_pipeline.py`` runs for one stage, built from its table."""
    if stage == CCS_STAGE:
        return [sys.executable, str(SCRIPT_DIR / "ccs_retrofit_inputs.py"), "--config", str(config_path)]
    _, script = STAGES[stage]
    command = [sys.executable, str(SCRIPT_DIR / script), "--config", str(config_path)]
    if unsmoothed_dir is not None and stage == "format":
        command += ["--unsmoothed-output-dir", str(unsmoothed_dir)]
    if unsmoothed_dir is not None and stage == "compare":
        command += ["--unsmoothed-dir", str(unsmoothed_dir)]
    return command


def run_stage(stage: str, config_path: Path = CONFIG_PATH, unsmoothed_dir: Path | None = None) -> tuple[int, str]:
    """Run one pipeline stage as a subprocess and return (exit code, log)."""
    completed = subprocess.run(
        stage_command(stage, config_path, unsmoothed_dir),
        cwd=str(ATB_DIR), capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    log = completed.stdout + ("\n" + completed.stderr if completed.stderr else "")
    return completed.returncode, log


def stage_switches() -> dict[str, str]:
    """Stage name -> the ``workflow`` switch that enables it in config.yaml."""
    return {stage: switch for stage, (switch, _) in STAGES.items()}


# --------------------------------------------------------------------------- results
def series_table(result: dict) -> pd.DataFrame:
    """Long table of every output point with its provenance category.

    Columns: scenario, series, metric, t, value, category, history_mode, file.
    The categories come from the comparison stage's own labeler. ATB
    projection points name the release file they were read from; history
    points name the source file recorded in inputs/historical/real.csv or reeds_baseline.csv.
    """
    settings = result["settings"]
    files = result["files"]
    columns = ["scenario", "series", "metric", "t", "value", "category", "history_mode", "file"]
    if not files:
        return pd.DataFrame(columns=columns)
    provenance = smoothing_provenance(settings, files[0])
    class_column = provenance["history_class_column"]
    future_data = provenance["future_data"]
    raw_files = {kind: Path(settings[f"{kind}_path"]).name for kind in ATB_FUTURE_SOURCES.values()}
    unsmoothed = Path(settings.get("unsmoothed_output_dir") or UNSMOOTHED_DIR)
    history_file = _history_file_lookup(settings, provenance["technology"])

    def projection_file(group):
        """Name the ATB release file this series' projection was read from."""
        value = future_data.get(group[class_column].iloc[0]) if isinstance(future_data, dict) else future_data
        kind = ATB_FUTURE_SOURCES.get(value)
        return (kind.replace("_", " "), raw_files[kind]) if kind else ("", "")

    rows = []
    for path in sorted(files, key=lambda p: SCENARIO_ORDER.index(p.stem.rpartition("_")[2])
                       if p.stem.rpartition("_")[2] in SCENARIO_ORDER else 99):
        scenario = path.stem.rpartition("_")[2]
        final = normalize_frame(pd.read_csv(path))
        baseline_path = unsmoothed / path.name
        baseline = normalize_frame(pd.read_csv(baseline_path)) if baseline_path.is_file() else None
        identifiers = series_columns(final)
        baseline_groups = dict(grouped_series(baseline, identifiers)) if baseline is not None else {}
        metrics = [c for c in final.columns if c != "t" and pd.api.types.is_numeric_dtype(final[c])]
        for label, group in grouped_series(final, identifiers):
            kind, raw_file = projection_file(group)
            series = str(group[class_column].iloc[0]) if class_column in group else "*"
            for metric in metrics:
                categories = provenance_categories(group, baseline_groups.get(label), metric, provenance)
                try:
                    mode = resolve_historical_mode(metric, group, provenance)
                except KeyError:
                    mode = ""
                for t, value, category in zip(group["t"], group[metric], categories):
                    projected = category in PROJECTION_CATEGORIES and kind
                    rows.append({"scenario": scenario, "series": label, "metric": metric,
                                 "t": int(t), "value": float(value),
                                 "category": f"{category} · {kind}" if projected else category,
                                 "history_mode": mode,
                                 "file": raw_file if projected
                                 else history_file(category, series, scenario, metric, int(t))})
    return pd.DataFrame(rows, columns=columns)


def _history_file_lookup(settings: dict, tech: str):
    """Return a function naming the prepared-history source file of one point.

    ReEDS baseline points are looked up in reeds_baseline.csv by series, scenario, metric
    and year; every other history category in real.csv (mapped rows). A
    series without its own rows falls back to the technology-wide "*" rows,
    and indexed metrics to their "<metric>_index" rows. Unknown points get "".
    """
    reeds_baseline = load_history(settings, "reeds_baseline")
    reeds_baseline = reeds_baseline.loc[reeds_baseline.technology.eq(tech)]
    baseline_files = {
        (row.series, str(row.scenario).lower(), row.metric, int(row.year)): row.source_file
        for row in reeds_baseline.itertuples()
    }
    real = load_history(settings, "real")
    real = real.loc[real.technology.eq(tech)]
    real_files = {(row.series, row.metric, int(row.year)): row.source_file for row in real.itertuples()}

    def lookup(category: str, series: str, scenario: str, metric: str, year: int) -> str:
        if category == BASELINE_CATEGORY:
            return baseline_files.get((series, scenario.lower(), metric, year), "")
        for name in (series, "*"):
            for column in (metric, f"{metric}_index"):
                if (name, column, year) in real_files:
                    return real_files[(name, column, year)]
        return ""

    return lookup


def metric_label(metric: str, dollar_year) -> str:
    """Return the axis label of a metric, with its dollar year."""
    return METRIC_LABELS.get(metric, metric).format(d=dollar_year)


def copy_target(config_path: Path = CONFIG_PATH) -> Path:
    """Return the ReEDS plant_characteristics folder that exports are copied to."""
    settings = load_processing_settings(config_path)
    return Path(settings["reedspath"]) / "inputs" / "plant_characteristics"


__all__ = [
    "ATB_DIR", "CCS_STAGE", "CONFIG_PATH", "PREVIEW_DIR", "SCENARIO_COLORS", "SCENARIO_ORDER", "WORK_DIR",
    "apply_series_settings", "baseline_year_range", "ccs_retrofit_key", "ccs_retrofit_preview",
    "config_changes", "copy_target", "dump_config",
    "flat_file_path", "future_options", "history_options", "load_config", "load_default_config", "load_schema",
    "metric_label", "preview_key", "preview_settings", "read_flat_file", "read_retrofit_costs",
    "resolve_atb_path", "run_stage",
    "run_technology", "series_options", "series_settings", "series_table", "set_optional", "stage_command",
    "stage_switches", "technologies", "technology_config", "write_config",
]
