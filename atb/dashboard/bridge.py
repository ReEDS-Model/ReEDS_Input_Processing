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

import difflib
import io
import json
import subprocess
import sys
import tempfile
import types
from pathlib import Path

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

for folder in (ATB_DIR / "scripts", ATB_DIR / "comparison"):
    if str(folder) not in sys.path:
        sys.path.insert(0, str(folder))

from atb_config import ATB_FUTURE_SOURCES, load_processing_settings, resolve_atb_path  # noqa: E402
from compare_atb_outputs import (  # noqa: E402
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
    with Path(path).open(encoding="utf-8") as stream:
        return yaml_handler().load(stream)


def dump_config(config) -> str:
    buffer = io.StringIO()
    yaml_handler().dump(config, buffer)
    return buffer.getvalue()


def write_config(config, path: Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dump_config(config), encoding="utf-8", newline="\n")
    return path


def _yaml_path(lines: list[str], index: int) -> list[str]:
    """Return the mapping keys enclosing one line, outermost first."""
    path, indent = [], len(lines[index]) - len(lines[index].lstrip())
    for text in reversed(lines[:index]):
        stripped = text.strip()
        level = len(text) - len(text.lstrip())
        if stripped and not stripped.startswith("#") and level < indent and stripped.endswith(":"):
            path.append(stripped[:-1])
            indent = level
            if level == 0:
                break
    return path[::-1]


def config_diff_hunks(config, path: Path = CONFIG_PATH, context: int = 4) -> list[dict]:
    """Diff the saved config.yaml against the draft, one hunk per change.

    Each hunk carries the YAML path of its first changed line, so a change
    deep inside a technology block still says which technology it belongs to.
    """
    before = Path(path).read_text(encoding="utf-8").splitlines()
    after = dump_config(config).splitlines()
    hunks, old, new = [], 0, 0
    for line in difflib.unified_diff(before, after, n=context, lineterm=""):
        if line.startswith(("---", "+++")):
            continue
        if line.startswith("@@"):
            old, new = (int(part[1:].split(",")[0]) - 1 for part in line.split()[1:3])
            hunks.append({"path": None, "lines": []})
            continue
        kind = {"-": "removed", "+": "added"}.get(line[:1], "context")
        if kind != "context" and hunks[-1]["path"] is None:
            source, index = (after, new) if kind == "added" else (before, old)
            hunks[-1]["path"] = _yaml_path(source, index)
        hunks[-1]["lines"].append((kind, line[1:]))
        old += kind != "added"
        new += kind != "removed"
    return hunks


def technologies(config) -> list[str]:
    node = config
    for key in TECHNOLOGIES_PATH:
        node = node[key]
    return list(node)


def technology_config(config, tech: str):
    node = config
    for key in TECHNOLOGIES_PATH:
        node = node[key]
    return node[tech]


def config_matrix(config) -> pd.DataFrame:
    """One row per technology and sub-technology with every switch as a column."""
    rows = []
    unavailable = config["processing"]["smooth_cost_curves"].get("unavailable_before", {})
    overrides = config["processing"].get("atb_case_overrides", {})
    for tech in technologies(config):
        node = technology_config(config, tech)
        future = node["future_data"]
        classes = list(future) if isinstance(future, dict) else [None]
        for cls in classes:
            row = {
                "technology": tech,
                "series": cls or "*",
                "future_data": future[cls] if cls else future,
            }
            for metric, mode in node["historical_data"].items():
                row[metric] = mode.get(cls, "") if isinstance(mode, dict) else mode
            row["smooth_projection"] = bool(node["future_smoothing_treatments"].get("smooth_projection_curve", False))
            row["atb_case"] = overrides.get(tech, config["processing"].get("atb_case", ""))
            row["unavailable_before"] = unavailable.get(tech, "")
            rows.append(row)
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- schema
def load_schema(path: Path = SCHEMA_PATH) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _deref(schema: dict, node):
    while isinstance(node, dict) and "$ref" in node:
        node = schema["definitions"][node["$ref"].rsplit("/", 1)[1]]
    return node


def _enum(schema: dict, node) -> list | None:
    node = _deref(schema, node)
    if not isinstance(node, dict):
        return None
    if "enum" in node:
        return list(node["enum"])
    if "const" in node:
        return [node["const"]]
    return None


def _technology_properties(schema: dict, tech: str) -> dict:
    technologies_schema = schema["definitions"]["smoothing"]["properties"]["technologies"]["properties"]
    node = _deref(schema, technologies_schema[tech])
    properties = {}
    for part in node.get("allOf", [node]):
        properties.update(_deref(schema, part).get("properties", {}))
    return properties


def _options(schema: dict, node, cls: str | None) -> list | None:
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


# --------------------------------------------------------------------------- preview
def flat_file_path(config_path: Path = CONFIG_PATH) -> Path:
    return Path(load_processing_settings(config_path)["flat_file_path"])


def read_flat_file(config_path: Path = CONFIG_PATH) -> pd.DataFrame:
    """Read the raw flat file once; the app caches the result."""
    return read_atb_flat_file(load_processing_settings(config_path))


def baseline_year_range(config_path: Path = CONFIG_PATH) -> tuple[int, int]:
    """First and last year of the prepared ReEDS baseline (historical/manual.csv).

    Outputs cannot start before the baseline begins, so the dashboard uses the
    first year as the lower bound of ``reeds_start_year``.
    """
    years = load_history(load_processing_settings(config_path), "manual")["year"]
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
    for kind in ("real", "manual"):
        load_history(settings, kind)

    args = types.SimpleNamespace(debug=False)
    atb_data = load_atb_data(settings, args, [tech], atb_data_in=flat_file, report_changes=False)
    filenames: list[str] = []
    process_tech_file(atb_data, tech, settings, filenames, dollaryear, deflator,
                      settings["config"]["processing"].get("sensitivity_name"), str(PREVIEW_DIR), args)
    result = {"settings": settings, "files": [PREVIEW_DIR / name for name in filenames]}
    result["points"] = series_table(result)
    result["sources"] = source_table(settings, tech)
    return result


def configured_technologies(config) -> list[str]:
    """Technologies a full run formats (``processing.technologies``)."""
    selected = config["processing"].get("technologies", "all")
    return technologies(config) if selected in ("all", ["all"]) else list(selected)


# --------------------------------------------------------------------------- export
def stage_command(stage: str, config_path: Path = CONFIG_PATH, unsmoothed_dir: Path | None = None) -> list[str]:
    """The command ``run_pipeline.py`` runs for one stage, built from its table."""
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


def output_listing(config_path: Path = CONFIG_PATH) -> tuple[Path, pd.DataFrame]:
    """The configured output folder and its ATB files, newest first."""
    settings = load_processing_settings(config_path)
    folder = Path(settings["output_dir"])
    year = settings["atbyear"]
    rows = [
        {"file": path.name, "modified": pd.Timestamp(path.stat().st_mtime, unit="s").strftime("%Y-%m-%d %H:%M:%S"),
         "size_kB": round(path.stat().st_size / 1024, 1)}
        for path in sorted(folder.glob(f"*_ATB_{year}_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    ] if folder.is_dir() else []
    return folder, pd.DataFrame(rows)


def exported_result(tech: str, config_path: Path = CONFIG_PATH) -> dict | None:
    """Plot data for one technology from the real output folder after an export."""
    settings = load_processing_settings(config_path)
    settings["unsmoothed_output_dir"] = str(UNSMOOTHED_DIR)
    prefix = settings["techs"][tech].get("reeds_name", tech)
    files = sorted(Path(settings["output_dir"]).glob(f"{prefix}_ATB_{settings['atbyear']}_*.csv"))
    files = [f for f in files if f.stem.rpartition("_")[2] in SCENARIO_ORDER]
    if not files:
        return None
    result = {"settings": settings, "files": files}
    result["points"] = series_table(result)
    result["sources"] = source_table(settings, tech)
    return result


# --------------------------------------------------------------------------- results
def series_table(result: dict) -> pd.DataFrame:
    """Long table of every output point with its provenance category.

    Columns: scenario, series, metric, t, value, category, history_mode, file.
    The categories come from the comparison stage's own labeler; ATB
    projection points also name the release file they were read from.
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
                                 "file": raw_file if projected else ""})
    return pd.DataFrame(rows, columns=columns)


def metric_label(metric: str, dollar_year) -> str:
    return METRIC_LABELS.get(metric, metric).format(d=dollar_year)


def source_table(settings: dict, tech: str) -> pd.DataFrame:
    """Where each metric's history came from, from the prepared history tables."""
    frames = []
    for kind in ("real", "manual"):
        data = load_history(settings, kind)
        data = data.loc[data.technology.eq(tech)]
        if kind == "real":
            data = data.loc[data.scope.eq("reeds")]
        if data.empty:
            continue
        anchors = data.loc[data.source_type.ne("filled")]
        summary = (
            anchors.groupby(["metric", "series", "source_type", "source_file", "source_url", "method"], dropna=False)
            .agg(first_year=("year", "min"), last_year=("year", "max"), points=("year", "nunique"))
            .reset_index()
        )
        summary.insert(0, "table", f"{kind}.csv")
        frames.append(summary)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def copy_target(config_path: Path = CONFIG_PATH) -> Path:
    settings = load_processing_settings(config_path)
    return Path(settings["reedspath"]) / "inputs" / "plant_characteristics"


__all__ = [
    "ATB_DIR", "CONFIG_PATH", "PREVIEW_DIR", "SCENARIO_COLORS", "SCENARIO_ORDER", "WORK_DIR",
    "baseline_year_range", "config_diff_hunks", "config_matrix", "configured_technologies", "copy_target",
    "dump_config", "exported_result", "flat_file_path", "future_options", "history_options",
    "load_config", "load_schema", "metric_label", "output_listing", "preview_settings", "read_flat_file",
    "resolve_atb_path", "run_stage", "run_technology", "series_table", "source_table", "stage_command",
    "stage_switches", "technologies", "technology_config", "write_config",
]
