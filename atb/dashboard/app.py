"""ATB inputs dashboard: every technology on one page.

Run from ``atb/``::

    streamlit run dashboard/app.py

Each technology has its settings next to its chart; changing a setting redraws
the charts it affects. The top of the page lists every setting that differs
from the default, which is ``config.yaml`` as committed in git. Nothing is
written until **Save and run**, which saves the settings to ``config.yaml``
and runs the pipeline stages as ``run_pipeline.py`` does.
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import bridge

st.set_page_config(page_title="ATB inputs dashboard", layout="wide")

# Text size. Streamlit sizes its text in rem, so one root font size scales the
# page, labels, and headings together; charts take PLOT_FONT_SIZE.
BASE_FONT_SIZE_PX = 18
PLOT_FONT_SIZE = 14
CHART_HEIGHT = 380
CCS_SECTION = "CCS retrofit"
st.markdown(
    f"<style>html {{ font-size: {BASE_FONT_SIZE_PX}px; }}</style>",
    unsafe_allow_html=True,
)

CATEGORY_COLORS = {
    "ATB projection (raw) · flat file": "#444444",
    "ATB projection (raw) · workbook": "#2b6cb0",
    "ATB projection (smoothed) · flat file": "#7f7f7f",
    "ATB projection (smoothed) · workbook": "#63b3ed",
    "ATB projection (raw)": "#444444",
    "ATB projection (smoothed)": "#7f7f7f",
    "Observed history (real)": "#d62728",
    "Filled real history": "#ff9896",
    "Split real history": "#e377c2",
    "Scaled real history": "#bcbd22",
    "Calculated project history": "#8c564b",
    "Indexed anchor (observed O&M)": "#9467bd",
    "Indexed history (ATB level, observed trend)": "#c5b0d5",
    "Backfill history": "#17becf",
    "Unavailable placeholder": "#c7c7c7",
    "ReEDS baseline history": "#ff7f0e",
}
DASHES = ["solid", "dash", "dot", "dashdot", "longdash", "longdashdot"]
# Line color of a series that is identical in every scenario.
ALL_SCENARIOS_COLOR = "#555555"


# --------------------------------------------------------------------------- state
@st.cache_resource(show_spinner=False)
def cached_flat_file(path: str, mtime: float) -> pd.DataFrame:
    """Read the ATB flat file once per file version (cached on its path and modified time)."""
    return bridge.read_flat_file()


def flat_file() -> pd.DataFrame:
    """Return the cached ATB flat file."""
    path = bridge.flat_file_path()
    return cached_flat_file(str(path), path.stat().st_mtime)


@st.cache_data(show_spinner=False)
def cached_retrofit_costs(path: str, mtime: float, case: str, _config) -> pd.DataFrame:
    """Read the ATB retrofit costs once per flat file version and ATB case."""
    return bridge.read_retrofit_costs(_config)


@st.cache_data(show_spinner=False)
def baseline_years() -> tuple[int, int]:
    """Return the first and last year of the ReEDS baseline."""
    return bridge.baseline_year_range()


def restart_widgets() -> None:
    """Rebuild the settings tables from the config and give every widget a new key, so all show its values."""
    config = st.session_state.config
    st.session_state.tables = {tech: bridge.series_settings(config, tech) for tech in bridge.technologies(config)}
    st.session_state.generation = st.session_state.get("generation", 0) + 1


if "config" not in st.session_state:
    st.session_state.default = bridge.load_default_config()
    st.session_state.config = bridge.load_config()
    st.session_state.saved = bridge.load_config()
    st.session_state.schema = bridge.load_schema()
    st.session_state.previews = {}
    st.session_state.rejected = {}
    st.session_state.run_log = None
    st.session_state.run_result = None
    restart_widgets()

config = st.session_state.config
default = st.session_state.default
schema = st.session_state.schema
previews = st.session_state.previews
generation = st.session_state.generation
technologies = sorted(bridge.technologies(config), key=str.lower)


def widget_key(name: str) -> str:
    """Return a widget key that is new after a reset."""
    return f"{name}_{generation}"


# --------------------------------------------------------------------------- settings
def edit_global_settings() -> None:
    """Inputs for the settings shared by all technologies."""
    atb = config["atb"]
    processing = config["processing"]
    smoothing = processing["smooth_cost_curves"]
    workflow = config["workflow"]
    first_baseline_year, _ = baseline_years()
    columns = st.columns(4)
    atb["dollar_year"] = columns[0].number_input(
        "Output dollar year", 1990, 2100, int(atb["dollar_year"]), key=widget_key("dollar_year"))
    processing["atb_case"] = columns[1].text_input(
        "ATB case", processing["atb_case"], key=widget_key("case"),
        help="A technology's own case override, if it has one, replaces this.")
    processing["reeds_start_year"] = columns[2].number_input(
        "First output year", first_baseline_year, 2100,
        max(int(processing["reeds_start_year"]), first_baseline_year), key=widget_key("start"),
        help=f"The ReEDS baseline (inputs/historical/reeds_baseline.csv) starts in {first_baseline_year}.")
    if "reeds_end_year" in processing:
        processing["reeds_end_year"] = columns[3].number_input(
            "Last output year", 2000, 2200, int(processing["reeds_end_year"]), key=widget_key("end"))
    smoothing["index_reference_year"] = columns[0].number_input(
        "Index reference year", 1990, 2100, int(smoothing["index_reference_year"]), key=widget_key("index"))
    smoothing["enabled"] = columns[1].checkbox(
        "Use the history modes", bool(smoothing["enabled"]), key=widget_key("enabled"),
        help="When off, all history comes from the ReEDS baseline.")
    smoothing["fill_atbstartyear2atbyear_with_real"] = columns[2].checkbox(
        "Fill ATB start year to ATB year with real data",
        bool(smoothing.get("fill_atbstartyear2atbyear_with_real", False)), key=widget_key("fill"))
    processing["copy_to_reeds"] = columns[3].checkbox(
        "Copy outputs into ReEDS", bool(processing.get("copy_to_reeds", False)), key=widget_key("copy"),
        help=f"Save and run then also writes into {bridge.copy_target()}")
    st.caption("Stages that Save and run runs:")
    columns = st.columns(4)
    for column, (stage, switch) in zip(columns, bridge.stage_switches().items()):
        workflow[switch] = column.checkbox(
            stage, bool(workflow[switch]), key=widget_key(f"stage_{stage}"),
            help="Also writes the CCS retrofit files." if stage == "format" else None)
    processing["reeds_atb_year"] = columns[3].number_input(
        "ReEDS ATB year to compare with", 2015, 2100, int(processing.get("reeds_atb_year", atb["year"])),
        key=widget_key("reeds_atb_year"),
        help=f"The compare stage plots the outputs against this ATB vintage in {bridge.copy_target()}.")


def edit_technology(tech: str) -> list[str]:
    """Inputs for one technology's settings; return a message per rejected value."""
    base = st.session_state.tables[tech]
    edited = st.data_editor(
        base, key=widget_key(f"{tech}_series"), hide_index=True, use_container_width=True, disabled=["series"],
        column_config={column: st.column_config.SelectboxColumn(options=options, required=True)
                       for column, options in bridge.series_options(schema, tech, base).items()})
    rejected = bridge.apply_series_settings(config, schema, tech, edited)

    processing = config["processing"]
    smoothing = processing["smooth_cost_curves"]
    treatments = bridge.technology_config(config, tech)["future_smoothing_treatments"]
    columns = st.columns(3)
    treatments["smooth_projection_curve"] = columns[0].checkbox(
        "Smooth projection curve", bool(treatments["smooth_projection_curve"]), key=widget_key(f"{tech}_smooth"))
    case = columns[1].text_input(
        "ATB case override", processing.get("atb_case_overrides", {}).get(tech, ""),
        placeholder=processing["atb_case"], key=widget_key(f"{tech}_case"))
    bridge.set_optional(processing, "atb_case_overrides", tech, case.strip() or None)
    uses_unavailable = "unavailable" in edited.drop(columns="series").to_numpy()
    if uses_unavailable or tech in smoothing.get("unavailable_before", {}):
        year = columns[2].number_input(
            "First buildable year", 1900, 2100, smoothing.get("unavailable_before", {}).get(tech), step=1,
            key=widget_key(f"{tech}_unavailable"), help="Years before it get the unavailable placeholder.")
        bridge.set_optional(smoothing, "unavailable_before", tech, None if year is None else int(year))
    return rejected


# --------------------------------------------------------------------------- charts
def plot_metric(points: pd.DataFrame, metric: str, dollar_year, hide_placeholder) -> go.Figure:
    """Return a chart of one metric: a line per scenario and series, with points colored by source."""
    data = points.loc[points.metric.eq(metric)].copy()
    if hide_placeholder is not None:
        data = data.loc[~data.value.eq(float(hide_placeholder))]
    all_scenarios = set(data.scenario)
    figure = go.Figure()
    series = sorted(data.series.unique())
    scenarios = [s for s in bridge.SCENARIO_ORDER if s in all_scenarios] + \
        sorted(all_scenarios - set(bridge.SCENARIO_ORDER))
    for index, label in enumerate(series):
        groups = {scenario: data.loc[data.scenario.eq(scenario) & data.series.eq(label)].sort_values("t")
                  for scenario in scenarios}
        groups = {scenario: group for scenario, group in groups.items() if not group.empty}
        # A series whose values are the same in every scenario (for example a
        # ReEDS-only legacy series) is drawn once instead of as stacked copies.
        first = next(iter(groups.values()))
        if len(groups) > 1 and all(
            group[["t", "value"]].to_numpy().tolist() == first[["t", "value"]].to_numpy().tolist()
            for group in groups.values()
        ):
            merged = "all scenarios" if set(groups) == all_scenarios else " + ".join(groups)
            groups = {merged: first}
        for scenario, group in groups.items():
            name = scenario if label == "all" else f"{scenario} · {label}"
            figure.add_trace(go.Scatter(
                x=group.t, y=group.value, mode="lines", name=name,
                line=dict(color=bridge.SCENARIO_COLORS.get(scenario, ALL_SCENARIOS_COLOR),
                          dash=DASHES[index % len(DASHES)]),
                customdata=group[["category", "history_mode", "file"]].to_numpy(),
                hovertemplate=(f"<b>{name}</b><br>%{{x}}: %{{y:,.3f}}<br>source: %{{customdata[0]}}"
                               "<br>file: %{customdata[2]}<br>history mode: %{customdata[1]}<extra></extra>"),
            ))
    for category, group in data.drop_duplicates(["t", "value", "category"]).groupby("category"):
        figure.add_trace(go.Scatter(
            x=group.t, y=group.value, mode="markers", name=category, legendgroup="source",
            legendgrouptitle_text="Source of each point",
            marker=dict(size=6, color=CATEGORY_COLORS.get(category, "#000000")),
            hoverinfo="skip",
        ))
    figure.update_layout(
        title=bridge.metric_label(metric, dollar_year), height=CHART_HEIGHT,
        margin=dict(l=40, r=20, t=50, b=40),
        font=dict(size=PLOT_FONT_SIZE), legend=dict(font=dict(size=PLOT_FONT_SIZE - 2)),
        xaxis_title="year", hovermode="closest",
    )
    return figure


def refresh_previews() -> None:
    """Format again every technology whose settings changed since its last preview."""
    refresh_ccs_retrofit()
    stale = [tech for tech in technologies
             if previews.get(tech, (None, None))[0] != bridge.preview_key(config, tech)]
    if not stale:
        return
    progress = st.progress(0.0, text="Reading the ATB flat file...")
    try:
        data = flat_file()
        settings = bridge.preview_settings(config)
    except Exception as error:  # the pipeline's own validation message is the point
        for tech in stale:
            previews[tech] = (bridge.preview_key(config, tech), error)
        progress.empty()
        return
    for index, tech in enumerate(stale):
        progress.progress(index / len(stale), text=f"Formatting {tech} ({index + 1}/{len(stale)})")
        try:
            result = bridge.run_technology(config, tech, data, settings)
        except (Exception, SystemExit) as error:
            result = error
        previews[tech] = (bridge.preview_key(config, tech), result)
    progress.empty()


def refresh_ccs_retrofit() -> None:
    """Build the CCS retrofit files again if a setting they depend on changed."""
    key = bridge.ccs_retrofit_key(config)
    if previews.get(bridge.CCS_STAGE, (None, None))[0] == key:
        return
    with st.spinner("Building the CCS retrofit files..."):
        try:
            path = bridge.flat_file_path()
            costs = cached_retrofit_costs(str(path), path.stat().st_mtime, config["processing"]["atb_case"], config)
            result = bridge.ccs_retrofit_preview(config, costs)
        except Exception as error:
            result = error
    previews[bridge.CCS_STAGE] = (key, result)


def show_ccs_retrofit() -> None:
    """Show the CCS retrofit multipliers and costs, which have no settings on this page."""
    changed = bridge.ccs_retrofit_key(config) != bridge.ccs_retrofit_key(default)
    st.subheader(CCS_SECTION + (" :orange[· changed from default]" if changed else ""))
    result = previews[bridge.CCS_STAGE][1]
    if isinstance(result, Exception):
        st.error(f"{type(result).__name__}: {result}")
        return
    left, right = st.columns([5, 6])
    with left:
        st.dataframe(result["series"], use_container_width=True, hide_index=True)
    titles = {"multiplier": f"Retrofit cost multiplier (1 = {result['normalization']})",
              "cost": "Retrofit capital cost (2004$/kW)"}
    with right:
        output = st.radio("Output", list(titles), horizontal=True, key="ccs_output", label_visibility="collapsed")
        data = result["points"].loc[result["points"].output.eq(output)]
        figure = go.Figure()
        for index, fuel in enumerate(result["series"]["series"]):
            for scenario in bridge.SCENARIO_ORDER:
                group = data.loc[data.fuel.eq(fuel) & data.scenario.eq(scenario)]
                figure.add_trace(go.Scatter(
                    x=group.t, y=group.value, mode="lines", name=f"{scenario} · {fuel}",
                    line=dict(color=bridge.SCENARIO_COLORS[scenario], dash=DASHES[index % len(DASHES)]),
                    hovertemplate=f"<b>{scenario} · {fuel}</b><br>%{{x}}: %{{y:,.3f}}<extra></extra>",
                ))
        figure.update_layout(
            title=titles[output], height=CHART_HEIGHT, margin=dict(l=40, r=20, t=50, b=40),
            font=dict(size=PLOT_FONT_SIZE), legend=dict(font=dict(size=PLOT_FONT_SIZE - 2)),
            xaxis_title="year", hovermode="closest",
        )
        st.plotly_chart(figure, use_container_width=True, key="ccs_chart")


def show_chart(tech: str, hide_placeholder: bool) -> None:
    """Show one metric of a technology's preview, or the error that stopped it."""
    result = previews[tech][1]
    if isinstance(result, BaseException):
        st.error(f"{type(result).__name__}: {result}")
        return
    points = result["points"]
    settings = result["settings"]
    # Metrics in the order of the settings table, then any the table does not list.
    metrics = list(dict.fromkeys([*st.session_state.tables[tech].columns[2:], *points.metric]))
    metric = st.radio("Metric", [m for m in metrics if m in set(points.metric)], horizontal=True,
                      key=f"{tech}_metric", label_visibility="collapsed")
    placeholder = settings["config"]["processing"]["smooth_cost_curves"]["unavailable_value"] \
        if hide_placeholder else None
    st.plotly_chart(plot_metric(points, metric, settings["dollaryear"], placeholder),
                    use_container_width=True, key=f"{tech}_chart")


# --------------------------------------------------------------------------- run
def save_and_run() -> None:
    """Save the settings to config.yaml, run the enabled pipeline stages, and say where the outputs are."""
    bridge.write_config(config, bridge.CONFIG_PATH)
    st.session_state.saved = bridge.load_config()
    logs, ran, failed = [], [], None
    stages = [stage for stage, switch in bridge.stage_switches().items() if config["workflow"][switch]]
    if "format" in stages:
        stages.insert(stages.index("format") + 1, bridge.CCS_STAGE)
    for stage in stages:
        with st.spinner(f"Running {stage}..."):
            code, log = bridge.run_stage(stage, bridge.CONFIG_PATH, bridge.WORK_DIR / "unsmoothed_export")
        logs.append(f"=== {stage} (exit {code}) ===\n{log}")
        if code != 0:
            failed = stage
            break
        ran.append(stage)
    st.session_state.run_log = "\n".join(logs)
    processing = config["processing"]
    if failed:
        reason = (log.strip().splitlines() or [""])[-1]
        result = ("error", f"The {failed} stage failed: {reason} See the pipeline log. config.yaml was saved.")
    elif "format" in ran:
        message = f"Finished. The updated outputs are in `{bridge.resolve_atb_path(processing['output_directory'])}`"
        if processing.get("copy_to_reeds"):
            message += f" and were copied into `{bridge.copy_target().parent}` (plant_characteristics and upgrades)"
        result = ("success", message + ".")
    else:
        result = ("warning", "Saved config.yaml. The format stage is off, so the output files were not updated.")
    st.session_state.run_result = result


# --------------------------------------------------------------------------- page
st.title("ATB inputs dashboard")
summary = st.container()
with st.expander("Global settings"):
    edit_global_settings()
hide_placeholder = st.checkbox("Hide unavailable placeholders (99999) in charts", True, key="hide")

rejected_before, st.session_state.rejected = st.session_state.rejected, {}
rejected = {}
charts = {}
ccs_section = None
for tech in sorted([*technologies, CCS_SECTION], key=str.lower):
    if tech == CCS_SECTION:
        ccs_section = st.container()
        st.divider()
        continue
    heading = st.empty()
    left, charts[tech] = st.columns([5, 6])
    with left:
        rejected[tech] = edit_technology(tech)
        for message in rejected_before.get(tech, []):
            st.warning(message)
    changed = bridge.preview_key(config, tech) != bridge.preview_key(default, tech)
    heading.subheader(f"{tech} :orange[· changed from default]" if changed else tech)
    st.divider()
if any(rejected.values()):
    # Redraw the tables from the config, which holds only the accepted values.
    st.session_state.rejected = rejected
    restart_widgets()
    st.rerun()

with summary:
    changes = bridge.config_changes(default, config)
    saved = bridge.config_changes(st.session_state.saved, config).empty
    status, reset_column, run_column = st.columns([6, 2, 2])
    status.markdown(f"**{len(changes)} setting(s) changed from the default.**" if len(changes)
                    else "**All settings are at their default.**")
    status.caption("The default is config.yaml as committed in git. "
                   + ("config.yaml holds these settings." if saved else "These settings are not saved yet."))
    if reset_column.button("Reset to default", use_container_width=True, disabled=changes.empty):
        st.session_state.config = bridge.load_default_config()
        restart_widgets()
        st.rerun()
    if run_column.button("Save and run", type="primary", use_container_width=True,
                         help="Save the settings to config.yaml, then run the pipeline."):
        save_and_run()
    if not changes.empty:
        st.dataframe(changes, use_container_width=True, hide_index=True)
    if st.session_state.run_result:
        kind, message = st.session_state.run_result
        getattr(st, kind)(message)
    if st.session_state.run_log:
        with st.expander("Pipeline log"):
            st.code(st.session_state.run_log)
    refresh_previews()

for tech in technologies:
    with charts[tech]:
        show_chart(tech, hide_placeholder)
with ccs_section:
    show_ccs_retrofit()
