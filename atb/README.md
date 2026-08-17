# ATB input pipeline

This directory turns raw NLR Annual Technology Baseline (ATB) data into ReEDS
input files and plots. The workflow has three explicit stages:

1. download and inspect raw data;
2. format the local raw data for ReEDS;
3. plot metrics from the same local raw data.

## Configure the run

[`config.yaml`](config.yaml) is the user-facing control file. It shows which
stages will run, the ATB release and source URLs, local filenames, ReEDS path,
technologies, processing choices, and plotting choices.

Review that file first, especially:

- `atb.year`, `atb.release_version`, and `atb.dollar_year`;
- the two `raw_data` URLs and filenames;
- `processing.reeds_repo` and selected technologies;
- `historical_data.directory` and its fixed dollar year;
- the metrics and technologies under `plotting`.

## Run one step at a time

Run the following commands from the `atb/` directory.

### Step 1: scrape raw inputs

```bash
python scripts/scrape_atb_inputs.py
```

This downloads or reuses both independent raw inputs:

- `scraped_input/atb_<year>_flat_file.csv`;
- `scraped_input/atb_<year>_workbook.xlsx`.

It then displays a summary of the flat file and the workbook sheets. Existing
files are reused unless `--force` is supplied:

```bash
python scripts/scrape_atb_inputs.py --force
```

Neither raw file is committed, so this step is required after a fresh clone.
See [`scraped_input/README.md`](scraped_input/README.md) for why, and how the
pinned URLs keep a run reproducible without storing the data in Git.

Downloads normally verify HTTPS certificates. If certificate verification
fails because the active conda environment does not trust an NLR network
inspection certificate, `raw_data.allow_insecure_ssl_fallback: true` permits a
clearly labeled `verify=False` retry. Set it to `false` to prohibit that retry.

### Step 2: format ReEDS inputs

```bash
python scripts/generate_atb_files.py
```

This reads the local raw files plus the versioned history under
`manual_input/historical/`, then writes ReEDS-formatted CSVs to `output/`. It
does **not** download raw data. If a required raw file is missing, run Step 1
first.

A missing history file can be initialized from the matching file in
`processing.reeds_repo` by temporarily setting
`historical_data.seed_missing_from_reeds: true`. Current scraped ATB rows
replace history from the first available projection year onward. That boundary
year is then appended to the fixed-dollar history file, ready for the next
annual ATB release.

Whether this step generates cost files and financial files, which technologies
it processes, and whether it copies results into ReEDS are all controlled under
`processing:` in `config.yaml`.

### Selective smoothing logic

The optional `processing.smooth_cost_curves` block removes short-lived dips,
bumps, and rounded stair steps without replacing each ATB trajectory with one
fully smoothed curve. The `selective` method follows these rules:

1. **Use 2022 as the projection anchor.** Moving backward from 2022, a
   contiguous historical tail below the anchor value is raised to the anchor.
   The scan stops at the first earlier value that already meets or exceeds it.
2. **Remove movement in the wrong direction.** For a series that declines from
   the anchor to its endpoint, temporary future increases are removed. For an
   improving series, such as a capacity-factor multiplier, temporary decreases
   are removed instead.
3. **Bridge near-equal plateaus.** Consecutive future values within the relative
   or absolute tolerance are treated as one group. The last year of each group
   is retained as a change point, and the years between change points are
   linearly interpolated. This removes small rounded stair steps.
4. **Bridge compact dips and bumps.** A cluster of nearby abrupt slope changes
   is interpolated between the years immediately outside the disturbance. This
   removes patterns such as the coal-CCS dip around 2033--2035.
5. **Preserve major transitions.** A year-to-year change at or above the major
   step threshold is not bridged. For example, the large fuel-cell capital-cost
   transition in 2035 remains explicit. A single isolated slope change is also
   retained as a normal ATB milestone.
6. **Limit the affected metrics.** With the current configuration, smoothing
   covers every `capcost*`, `fom*`, and `vom` column, including battery energy
   costs, plus `cf_improvement`/`CF_mult`. Heat rate and efficiency are not
   changed.

The current defaults in `config.yaml` are:

| Setting | Default | Meaning |
| --- | ---: | --- |
| `projection_start_year` | `2022` | Historical/projection anchor |
| `similar_value_relative_tolerance` | `0.001` | Values within 0.1% can form one plateau |
| `similar_value_absolute_tolerance` | `1e-9` | Numerical tolerance near zero |
| `slope_change_threshold` | `0.4` | Detect a normalized slope change of 40% or more |
| `max_kink_years` | `4` | Maximum span of a compact slope-change cluster |
| `major_step_relative_threshold` | `0.1` | Preserve year-to-year changes of 10% or more |
| `include_capacity_factor_multiplier` | `true` | Apply the same rules to capacity-factor multipliers |

The older flat-history/anchor-to-target behavior remains available as
`method: linear_bridge`.

### File names and column schemas expected by ReEDS

Generated files must match what ReEDS expects exactly, because ReEDS resolves
these inputs by file name and, for some technologies, reads their columns by
position rather than by name. Two settings in
[`scripts/settings.yaml`](scripts/settings.yaml) handle the cases where the
internal representation and the ReEDS representation differ:

- `reeds_name` — the file prefix ReEDS uses when it differs from the internal
  technology key. Onshore and offshore wind are `wind-ons`/`wind-ofs`
  internally but `ons-wind`/`ofs-wind` in ReEDS, so their outputs are written
  as `ons-wind_ATB_<year>_<scenario>.csv` and `ofs-wind_ATB_<year>_<scenario>.csv`.
  The output file name doubles as the `Scenario` key in the ReEDS
  `dollaryear.csv`, so a mismatch here silently leaves ReEDS reading its
  previous inputs.
- `output_cols` — an ordered mapping of internal column name to ReEDS header,
  applied as the last step before writing. ReEDS reads the two wind files
  positionally in `reeds/input_processing/plantcostprep.py`, and detects the
  ATB 2024 offshore format by the presence of a `Turbine` column, so those
  files must keep the legacy headers and this exact column order:
  `Turbine, Year, CF_mult, Overnight Cap Cost $/kW, Fixed O&M $/(kW-yr),
  Var O&M $/MWh` (plus `rsc_mult` for offshore). Writing the internal names or
  order instead makes ReEDS assign capital cost to the capacity-factor
  multiplier without raising an error.

Technologies without these settings are written using the internal column names
listed under `cols`, which already match their ReEDS files. Everything upstream
of the write step — history files in `manual_input/historical/`, scenario
comparisons, transformations — uses the internal names throughout.

### Step 3: plot raw ATB data

```bash
python scripts/atb_plotting.py
```

This reads the same local flat file used in Step 2 and saves the configured
figures to `figures/`. It does not scrape data or plot the formatted files from
`output/`.

## Run the configured pipeline

To run the enabled stages in order, set the switches under `workflow:` in
`config.yaml`, then run:

```bash
python scripts/run_pipeline.py
```

The runner prints which stages will run before doing any work. A stage can also
be selected explicitly:

```bash
python scripts/run_pipeline.py --only scrape
python scripts/run_pipeline.py --only format
python scripts/run_pipeline.py --only plot
```

## Data flow

```text
config.yaml
    |
    +--> scrape_atb_inputs.py
    |        +--> scraped_input/atb_<year>_flat_file.csv
    |        +--> scraped_input/atb_<year>_workbook.xlsx
    |        +--> terminal summaries/previews of both raw files
    |
    +--> generate_atb_files.py
    |        +--> raw flat file (primary ATB data)
    |        +--> raw workbook (battery power/energy cost split)
    |        +--> manual_input/historical/ (versioned ReEDS history)
    |        +--> manual_input/ (CSP ratios and pre-release fallbacks)
    |        +--> ReEDS deflator and dollaryear tables
    |        +--> output/*_ATB_<year>_<scenario>.csv
    |
    +--> atb_plotting.py
             +--> the same raw flat file
             +--> figures/
```

The flat file and workbook are independent upstream downloads. Neither is
generated from the other. The formatter and plotter do not download data; they
only consume the raw files created by the scraper.

The normal pipeline validates the formatted pre-smoothing data against the
configured ReEDS repository when `workflow.make_comparison_plots` is enabled.
It holds that data in a temporary directory, prints the validation summary to
the terminal, writes local validation plots under `comparison/plots/`, and
writes versioned before/after plots under `figures/smoothing_comparison/`. The
temporary CSVs are deleted when the pipeline exits; no row-level or summary CSV
reports are created.

For ATB 2024, the URLs are intentionally pinned to corrected release v3.
Changing the release can change technology trajectories; update both source
URLs together when adopting a newer release.

## Directory layout

| Path | Purpose |
| --- | --- |
| `config.yaml` | User-facing workflow configuration |
| `scraped_input/` | Visible, unmodified raw ATB downloads (local only; not committed) |
| `manual_input/` | Versioned history and inputs unavailable in ATB downloads |
| `scripts/settings.yaml` | Internal per-technology ReEDS formatting rules |
| `scripts/scrape_atb_inputs.py` | Raw-data download and inspection |
| `scripts/generate_atb_files.py` | ReEDS input formatter |
| `scripts/atb_plotting.py` | Raw ATB plotting |
| `output/` | Generated ReEDS-formatted CSVs |
| `figures/` | Generated ATB plots |

See [`scripts/README.md`](scripts/README.md) for the scripts folder structure.
