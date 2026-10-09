# ATB inputs dashboard

A local web page over the ATB pipeline. It changes nothing about how the
pipeline works: it edits the same `config.yaml` switches, previews
technologies with the same code, and exports by running the same stages as
`scripts/run_pipeline.py`.

Double-click `dashboard/open_dashboard.bat`, or from a terminal:

```bash
cd atb
python -m streamlit run dashboard/app.py
```

Streamlit and plotly are installed in the Anaconda **base** environment, not
in `reeds2`; the launcher uses base. To run it from `reeds2` instead, first
`pip install streamlit plotly ruamel.yaml` there.

## Pages

| Page | What it does |
| --- | --- |
| Sidebar | Pick a technology; global settings (dollar year, ATB case, output years, smoothing, copy to ReEDS); Preview, Preview all, Save, Reset. |
| Technology | Every `future_data` and `historical_data` entry of one technology as a drop-down. Option lists come from `config.schema.json`, so they always match what the formatter accepts. Preview plots one chart per metric: one line per scenario and sub-technology, hover shows value, source category, release file, and history mode; dots color each point by source. Tables list year ranges per source and the prepared history rows (`real.csv`, `manual.csv`) with file, URL, and method. |
| All technologies | The whole configuration as one table (one row per series), and one small chart per previewed technology for a chosen metric. |
| Export | Save the draft to `config.yaml`, then run the chosen stages (format, plot, compare) exactly as `run_pipeline.py` does. Shows the log and the files in the configured output folder. |

## Preview vs. export

A **preview** formats one technology (or all) with the draft config into a
scratch folder under the system temp directory (`%TEMP%\atb_dashboard`).
Nothing is kept inside the repository; the first preview reads the ATB flat
file once and caches it for the session, so previews take seconds.

An **export** first writes the draft to `config.yaml` (comments preserved),
then runs the pipeline stages as subprocesses. The real outputs in `output/`,
and the ReEDS copies when `processing.copy_to_reeds` is on, therefore always
match the saved configuration, exactly as a command-line run would.

The pipeline's own validation errors are shown as they are.

## Files

`app.py` (page), `bridge.py` (calls into `scripts/` and `comparison/`; no
pipeline rule is re-implemented here).
