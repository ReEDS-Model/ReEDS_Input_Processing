"""ATB inputs dashboard.

Run from ``atb/``::

    streamlit run dashboard/app.py

Three pages share one draft of ``config.yaml``:

* **Technology**: edit one technology's switches, preview it in seconds, see
  every scenario's trajectory with the source of each point.
* **All technologies**: the whole configuration as one table, and one chart
  per technology for a chosen metric.
* **Export**: save the draft to ``config.yaml`` and run the pipeline stages
  exactly as ``run_pipeline.py`` does, so ``output/`` (and ReEDS when
  ``copy_to_reeds`` is on) matches the saved config.
"""

from __future__ import annotations

import html
import traceback

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import bridge

st.set_page_config(page_title="ATB inputs dashboard", layout="wide")

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
    "Broadcast history": "#17becf",
    "Unavailable placeholder": "#c7c7c7",
    "Manual history (ReEDS ATB baseline)": "#ff7f0e",
}
DASHES = ["solid", "dash", "dot", "dashdot", "longdash", "longdashdot"]



DIFF_ROW_STYLE = {
    "removed": "background:rgba(220,60,60,.28)",
    "added": "background:rgba(60,170,90,.28)",
    "context": "opacity:.55",
}


def diff_hunk_html(hunk: dict) -> str:
    """Render one config diff hunk: a YAML-path breadcrumb, then colored lines."""
    crumbs = [html.escape(part) for part in hunk["path"] or []]
    if "technologies" in crumbs and crumbs.index("technologies") + 1 < len(crumbs):
        tech = crumbs.index("technologies") + 1
        crumbs[tech] = f"<b>{crumbs[tech]}</b>"
    signs = {"removed": "- ", "added": "+ "}
    rows = "".join(
        f'<div style="{DIFF_ROW_STYLE[kind]};white-space:pre;padding:0 .4em">'
        f'{signs.get(kind, "  ")}{html.escape(text)}</div>'
        for kind, text in hunk["lines"]
    )
    return (
        '<div style="font-family:monospace;font-size:.85em;margin-bottom:.9em">'
        f'<div style="margin-bottom:.25em">{" › ".join(crumbs) or "(top level)"}</div>{rows}</div>'
    )

# --------------------------------------------------------------------------- state
@st.cache_resource(show_spinner=False)
def cached_flat_file(path: str, mtime: float) -> pd.DataFrame:
    return bridge.read_flat_file()


def flat_file() -> pd.DataFrame:
    path = bridge.flat_file_path()
    return cached_flat_file(str(path), path.stat().st_mtime)


@st.cache_data(show_spinner=False)
def baseline_years() -> tuple[int, int]:
    return bridge.baseline_year_range()


def reset_config() -> None:
    st.session_state.config = bridge.load_config()
    st.session_state.results = {}
    st.session_state.export_log = None


if "config" not in st.session_state:
    reset_config()
if "schema" not in st.session_state:
    st.session_state.schema = bridge.load_schema()

config = st.session_state.config
schema = st.session_state.schema
technologies = bridge.technologies(config)


def show_error(error: Exception) -> None:
    """The pipeline's own validation messages are the point; show them as-is."""
    st.error(f"{type(error).__name__}: {error}")
    with st.expander("Traceback"):
        st.code(traceback.format_exc())


def preview(tech: str, settings: dict | None = None) -> bool:
    try:
        st.session_state.results[tech] = bridge.run_technology(config, tech, flat_file(), settings)
        return True
    except Exception as error:
        show_error(error)
        return False


# --------------------------------------------------------------------------- widgets
def choose(label: str, current, options: list | None, key: str):
    """Selectbox over the schema's options; a value outside them is still shown."""
    if not options:
        st.text_input(label, value=str(current), key=key, disabled=True,
                      help="config.schema.json gives this entry no option list.")
        return current
    options = list(options)
    if current not in options:
        options.append(current)
    if len(options) == 1:
        st.text_input(label, value=str(current), key=key, disabled=True,
                      help="The only value the schema allows here.")
        return current
    return st.selectbox(label, options, index=options.index(current), key=key)


def edit_global_settings() -> None:
    atb = config["atb"]
    processing = config["processing"]
    smoothing = processing["smooth_cost_curves"]
    st.caption(f"ATB {atb['year']} release {atb.get('release_version', '')}")
    atb["dollar_year"] = st.number_input("Output dollar year (atb.dollar_year)", 1990, 2100,
                                         int(atb["dollar_year"]), key="g_dollar_year")
    processing["atb_case"] = st.text_input("ATB case (processing.atb_case)", processing.get("atb_case", ""),
                                           key="g_case", help="Overrides per technology stay in atb_case_overrides.")
    first_baseline_year, _ = baseline_years()
    processing["reeds_start_year"] = st.number_input(
        "First output year", first_baseline_year, 2100,
        max(int(processing["reeds_start_year"]), first_baseline_year), key="g_start",
        help=f"The prepared ReEDS baseline (historical/manual.csv) starts in {first_baseline_year}.")
    if "reeds_end_year" in processing:
        processing["reeds_end_year"] = st.number_input("Last output year", 2000, 2200,
                                                       int(processing["reeds_end_year"]), key="g_end")
    smoothing["enabled"] = st.checkbox("History and smoothing enabled", bool(smoothing["enabled"]), key="g_enabled")
    smoothing["fill_atbstartyear2atbyear_with_real"] = st.checkbox(
        "Fill ATB start year to ATB year with real data",
        bool(smoothing.get("fill_atbstartyear2atbyear_with_real", False)), key="g_fill")
    smoothing["index_reference_year"] = st.number_input(
        "Index reference year", 1990, 2100, int(smoothing["index_reference_year"]), key="g_index")
    processing["copy_to_reeds"] = st.checkbox(
        "Copy exported files into ReEDS (processing.copy_to_reeds)", bool(processing.get("copy_to_reeds", False)),
        key="g_copy", help=f"Export then also writes into {bridge.copy_target()}")
    st.caption("Global changes apply to every technology; preview the ones you want to see.")


def edit_technology(tech: str) -> None:
    node = bridge.technology_config(config, tech)
    history = node["historical_data"]
    future = node["future_data"]

    st.markdown("**Projection source** (`future_data`)")
    if isinstance(future, dict):
        columns = st.columns(min(len(future), 4))
        for index, cls in enumerate(list(future)):
            with columns[index % len(columns)]:
                future[cls] = choose(cls, future[cls], bridge.future_options(schema, tech, cls), f"{tech}_future_{cls}")
    else:
        node["future_data"] = choose(tech, future, bridge.future_options(schema, tech, None), f"{tech}_future")

    st.markdown("**History mode per metric** (`historical_data`)")
    for metric in list(history):
        value = history[metric]
        if isinstance(value, dict):
            st.caption(metric)
            columns = st.columns(min(len(value), 4))
            for index, cls in enumerate(list(value)):
                with columns[index % len(columns)]:
                    value[cls] = choose(cls, value[cls], bridge.history_options(schema, tech, metric, cls),
                                        f"{tech}_hist_{metric}_{cls}")
        else:
            history[metric] = choose(metric, value, bridge.history_options(schema, tech, metric, None),
                                     f"{tech}_hist_{metric}")

    treatments = node["future_smoothing_treatments"]
    treatments["smooth_projection_curve"] = st.checkbox(
        "Smooth the projection curve", bool(treatments.get("smooth_projection_curve", False)), key=f"{tech}_smooth")
    if "fill_atbstartyear2atbyear_with_real" in node:
        node["fill_atbstartyear2atbyear_with_real"] = st.checkbox(
            "Fill ATB start year to ATB year with real data (this technology)",
            bool(node["fill_atbstartyear2atbyear_with_real"]), key=f"{tech}_fill")
    unavailable = config["processing"]["smooth_cost_curves"].get("unavailable_before", {})
    if tech in unavailable:
        unavailable[tech] = st.number_input("First buildable year (unavailable_before)", 1900, 2100,
                                            int(unavailable[tech]), key=f"{tech}_unavailable")
    overrides = config["processing"].get("atb_case_overrides", {})
    if tech in overrides:
        overrides[tech] = st.text_input("ATB case override", overrides[tech], key=f"{tech}_case")


# --------------------------------------------------------------------------- plots
def plot_metric(points: pd.DataFrame, metric: str, dollar_year, hide_placeholder, title=None,
                height=380, show_legend=True) -> go.Figure:
    data = points.loc[points.metric.eq(metric)].copy()
    if hide_placeholder is not None:
        data = data.loc[~data.value.eq(float(hide_placeholder))]
    figure = go.Figure()
    series = sorted(data.series.unique())
    scenarios = [s for s in bridge.SCENARIO_ORDER if s in set(data.scenario)] + \
        sorted(set(data.scenario) - set(bridge.SCENARIO_ORDER))
    for scenario in scenarios:
        for index, label in enumerate(series):
            group = data.loc[data.scenario.eq(scenario) & data.series.eq(label)].sort_values("t")
            if group.empty:
                continue
            name = scenario if label == "all" else f"{scenario} · {label}"
            figure.add_trace(go.Scatter(
                x=group.t, y=group.value, mode="lines", name=name,
                line=dict(color=bridge.SCENARIO_COLORS.get(scenario, "#333"), dash=DASHES[index % len(DASHES)]),
                customdata=group[["category", "history_mode", "file"]].to_numpy(),
                hovertemplate=(f"<b>{name}</b><br>%{{x}}: %{{y:,.3f}}<br>source: %{{customdata[0]}}"
                               "<br>file: %{customdata[2]}<br>history mode: %{customdata[1]}<extra></extra>"),
            ))
    for category, group in data.groupby("category"):
        figure.add_trace(go.Scatter(
            x=group.t, y=group.value, mode="markers", name=category, legendgroup="source",
            legendgrouptitle_text="Source of each point",
            marker=dict(size=6, color=CATEGORY_COLORS.get(category, "#000000")),
            hoverinfo="skip",
        ))
    figure.update_layout(
        title=title or bridge.metric_label(metric, dollar_year), height=height,
        margin=dict(l=40, r=20, t=50, b=40), legend=dict(font=dict(size=10)), showlegend=show_legend,
        xaxis_title="year", hovermode="closest",
    )
    return figure


def show_result(result: dict, hide_placeholder: bool) -> None:
    settings = result["settings"]
    points = result["points"]
    placeholder = settings["config"]["processing"]["smooth_cost_curves"].get("unavailable_value") \
        if hide_placeholder else None
    st.caption("Files: " + ", ".join(str(path) for path in result["files"]))
    metrics = list(dict.fromkeys(points.metric))
    columns = st.columns(2)
    for index, metric in enumerate(metrics):
        with columns[index % 2]:
            st.plotly_chart(plot_metric(points, metric, settings["dollaryear"], placeholder), use_container_width=True)

    st.subheader("Where each stretch of the curve comes from")
    spans = (
        points.groupby(["metric", "scenario", "series", "category", "history_mode", "file"], sort=False)
        .agg(first_year=("t", "min"), last_year=("t", "max"), points=("t", "size"))
        .reset_index()
        .sort_values(["metric", "scenario", "series", "first_year"])
    )
    st.dataframe(spans, use_container_width=True, hide_index=True)

    st.subheader("Prepared history sources (historical/real.csv and manual.csv)")
    sources = result["sources"]
    if sources.empty:
        st.caption("No prepared history rows for this technology.")
    else:
        st.dataframe(sources, use_container_width=True, hide_index=True,
                     column_config={"source_url": st.column_config.LinkColumn("source_url")})


# --------------------------------------------------------------------------- sidebar
with st.sidebar:
    st.title("ATB inputs")
    tech = st.selectbox("Technology", technologies, key="tech")
    with st.expander("Global settings", expanded=False):
        edit_global_settings()
    hide_placeholder = st.checkbox("Hide 99999 placeholders in plots", True, key="hide")
    st.divider()
    do_preview = st.button("Preview this technology", type="primary", use_container_width=True,
                           help="Formats only this technology with the draft, into a scratch folder.")
    do_preview_all = st.button("Preview all technologies", use_container_width=True)
    col_save, col_reset = st.columns(2)
    do_save = col_save.button("Save config.yaml", use_container_width=True)
    do_reset = col_reset.button("Reset draft", use_container_width=True)
    draft_differs = bool(bridge.config_diff_hunks(config))
    st.caption("Draft differs from config.yaml" if draft_differs else "Draft matches config.yaml")

if do_reset:
    reset_config()
    st.rerun()
if do_save:
    bridge.write_config(config, bridge.CONFIG_PATH)
    st.sidebar.success("Saved config.yaml")
if do_preview:
    with st.spinner(f"Formatting {tech}..."):
        preview(tech)
if do_preview_all:
    progress = st.progress(0.0, text="Formatting every technology with the draft...")
    try:
        settings = bridge.preview_settings(config)
        selected = bridge.configured_technologies(config)
        for index, name in enumerate(selected):
            progress.progress((index + 1) / len(selected), text=f"Formatting {name} ({index + 1}/{len(selected)})")
            if not preview(name, settings):
                break
    except Exception as error:
        show_error(error)
    progress.empty()

tab_tech, tab_all, tab_export = st.tabs(["Technology", "All technologies", "Export"])

# --------------------------------------------------------------------------- technology page
with tab_tech:
    st.header(tech)
    with st.expander("Technology switches (config.yaml)", expanded=True):
        edit_technology(tech)
    result = st.session_state.results.get(tech)
    if result is None:
        st.info("Press **Preview this technology** to format it with the current draft and plot the result.")
    else:
        show_result(result, hide_placeholder)
    with st.expander("Draft vs. saved config.yaml"):
        hunks = bridge.config_diff_hunks(config)
        if not hunks:
            st.caption("No differences.")
        for hunk in hunks:
            st.markdown(diff_hunk_html(hunk), unsafe_allow_html=True)

# --------------------------------------------------------------------------- overview page
with tab_all:
    st.header("All technologies")
    st.markdown("**Configuration** (one row per series; edit on the Technology page)")
    st.dataframe(bridge.config_matrix(config), use_container_width=True, hide_index=True)
    previewed = [name for name in technologies if name in st.session_state.results]
    if not previewed:
        st.info("Press **Preview all technologies** to see every trajectory here.")
    else:
        all_metrics = list(dict.fromkeys(
            metric for name in previewed for metric in st.session_state.results[name]["points"].metric))
        metric = st.selectbox("Metric", all_metrics, key="overview_metric")
        st.caption(f"{len(previewed)} of {len(technologies)} technologies previewed with the current draft.")
        grid = st.columns(3)
        for index, name in enumerate(previewed):
            result = st.session_state.results[name]
            points = result["points"]
            if metric not in set(points.metric):
                continue
            placeholder = result["settings"]["config"]["processing"]["smooth_cost_curves"].get("unavailable_value") \
                if hide_placeholder else None
            with grid[index % 3]:
                st.plotly_chart(
                    plot_metric(points, metric, result["settings"]["dollaryear"], placeholder,
                                title=f"{name}: {bridge.metric_label(metric, result['settings']['dollaryear'])}",
                                height=300, show_legend=False),
                    use_container_width=True)

# --------------------------------------------------------------------------- export page
with tab_export:
    st.header("Export: save the draft and run the pipeline")
    st.markdown(
        "This is `python scripts/run_pipeline.py`. The draft is saved to `config.yaml` first, "
        "so the files written below always match the saved configuration."
    )
    switches = bridge.stage_switches()
    workflow = config["workflow"]
    chosen = []
    columns = st.columns(len(switches))
    for column, (stage, switch) in zip(columns, switches.items()):
        with column:
            if st.checkbox(f"{stage} (`workflow.{switch}`)", bool(workflow.get(switch, False)), key=f"stage_{stage}"):
                chosen.append(stage)
    output_dir, listing = bridge.output_listing()
    st.caption(f"Format writes to `{output_dir}`"
               + (f" and copies into `{bridge.copy_target()}`" if config["processing"].get("copy_to_reeds") else "")
               + ".")
    if st.button("Save config.yaml and run", type="primary", disabled=not chosen):
        bridge.write_config(config, bridge.CONFIG_PATH)
        logs = []
        failed = False
        for stage in chosen:
            with st.spinner(f"Running {stage}..."):
                code, log = bridge.run_stage(stage, bridge.CONFIG_PATH, bridge.WORK_DIR / "unsmoothed_export")
            logs.append(f"=== {stage} (exit {code}) ===\n{log}")
            if code != 0:
                failed = True
                break
        st.session_state.export_log = "\n".join(logs)
        if failed:
            st.error("A stage failed; see the log below.")
        else:
            st.success("Pipeline finished and config.yaml saved.")
            # Plots on the other pages now reflect the exported files.
            unsmoothed = bridge.WORK_DIR / "unsmoothed_export"
            for name in bridge.configured_technologies(config):
                exported = bridge.exported_result(name)
                if exported is not None:
                    exported["settings"]["unsmoothed_output_dir"] = str(unsmoothed)
                    exported["points"] = bridge.series_table(exported)
                    st.session_state.results[name] = exported
        output_dir, listing = bridge.output_listing()
    if st.session_state.get("export_log"):
        with st.expander("Pipeline log", expanded=False):
            st.code(st.session_state.export_log)
    st.subheader(f"Files in {output_dir}")
    if listing.empty:
        st.caption("No ATB files for this release yet.")
    else:
        st.dataframe(listing, use_container_width=True, hide_index=True)
