# ATB inputs dashboard

One local web page over the ATB pipeline. It edits the same `config.yaml`
settings and runs the same code as `scripts/run_pipeline.py`.

Double-click `dashboard/open_dashboard.bat` (Windows) or
`dashboard/open_dashboard.command` (macOS), or from a terminal:

```bash
cd atb
python -m streamlit run dashboard/app.py --theme.base light
```

The Windows launcher uses the Anaconda **base** Python and the macOS one uses
`python3` from your PATH. Whichever Python runs it needs
`pip install streamlit plotly ruamel.yaml`.

## The page

- **Each technology**, in alphabetical order, has its settings next to its
  chart: a table with the
  projection source and history mode of each series, and the
  technology-wide switches below it. Change a setting and the chart is
  redrawn. Hover a point for its value, source, and history mode.
- **CCS retrofit** charts the `inputs/upgrades/` multipliers and costs. It
  has no settings of its own on the page.
- **Global settings** hold the settings shared by all technologies and the
  stages to run.
- **Changes from the default** are listed at the top, and the changed
  technologies are marked. The default is `config.yaml` as committed in git.
- **Reset to default** returns every setting to the default.
- **Save and run** saves the settings to `config.yaml` and runs the chosen
  stages; the format stage also runs `scripts/ccs_retrofit_inputs.py`. The
  page then says where the updated outputs are. Nothing is written before
  that; charts are previews formatted into the system temp folder.

Option lists come from `config.schema.json`, and the pipeline's own validation
errors are shown as they are. `app.py` is the page; `bridge.py` calls into
`scripts/` and `comparison/`, so no pipeline rule is re-implemented here.
