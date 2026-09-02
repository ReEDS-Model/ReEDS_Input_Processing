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
- the optional observed-cost sources under `historical_cost_sources`;
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

### Optional: scrape observed historical capital costs

```bash
python scripts/scrape_historical_costs.py
```

This separate utility downloads the configured LBNL land-wind and utility-PV,
NLR offshore-wind, and annual EIA generator-cost workbooks. It preserves the
original files under `scraped_input/historical_costs/` and creates
`historical_capital_costs.csv` plus a URL/checksum manifest there. These data
enter ReEDS inputs only through an explicit mapping in `config.yaml`. The
current mappings replace pre-projection UPV and land-based-wind `capcost` with
LBNL national series; their FOM, VOM, and capacity-factor history remain manual. See
[`scraped_input/README.md`](scraped_input/README.md) for the retained unit,
capacity-basis, geography, and dollar-year distinctions.

### Step 2: format ReEDS inputs

```bash
python scripts/generate_atb_files.py
```

This reads the local raw files plus the versioned history under
`manual_input/historical/`, then writes ReEDS-formatted CSVs to `output/`. It
does **not** download raw data. If a required raw file is missing, run Step 1
first.

Before processing any cost files, the formatter checks every selected
technology metric configured as `real`. It stops with a single actionable error
if the normalized historical-cost CSV, a reviewed metric mapping, or matching
source rows are missing.

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
fully smoothed curve. Each technology has metric-level historical choices and
independent future switches under `smooth_cost_curves.technologies`:

The default `columns: all` applies future monetary smoothing to every monetary
metric (`capcost*`, `fom*`, `vom`), since a rounding artifact is no more real in
O&M than in capital cost. `capital_costs` narrows it to `capcost*`. Heat rate is
not monetary and is never included by `all`. Historical source choices apply
independently to every metric. By default, metrics with reviewed observations
use `real`, while all others use `broadcast`. Capacity-factor multipliers remain
included automatically for technologies that use them.

| Historical mode | Effect before `projection_start_year` |
| --- | --- |
| `real` | Use only the reviewed observed-series mapping for that metric. Preserve reported values, linearly interpolate internal missing years, and use the nearest observation for years outside the reported range. An unmapped metric or row variant raises an error. |
| `manual` | Preserve the versioned rows in `manual_input/historical/` exactly. |
| `broadcast` | Use the first ATB projection value for every historical year. This generated history does not retain any manual historical values. |

The switches under `future_smoothing_treatments` apply from
`projection_start_year` onward. ATB's published direction is never overridden:
a cost the source projects to rise, such as the 2023 solar and wind increases,
is passed through unchanged.

| Future smoothing treatment | Effect |
| --- | --- |
| `smooth_projection_curve` | Bridge compact clusters of slope changes. Major transitions, single ATB milestones, and flat stretches remain explicit. |

Every technology listed under `smooth_cost_curves.technologies` is processed.
Every modeled metric has an explicit `historical_data` entry. For example,
biopower uses `capcost: real`, while fixed O&M, variable O&M, and heat rate each
explicitly select `broadcast`.

A metric may instead give one mode per sub-technology, for a technology whose
observed series describes only some of its rows. Two do. Offshore wind CapEx is
a fixed-bottom series, so only that class reads it; EIA reports gas plants only
as "combined cycle" or "combustion turbine", so the H-Frame and aeroderivative
variants have no observation at all.

```yaml
wind-ofs:
  historical_data:
    capcost:
      fixed: real
      floating: broadcast

gas:
  historical_data:
    capcost:
      Gas-CC: real
      Gas-CC_H_1x1: broadcast
      Gas-CC_H_2x1: broadcast
      Gas-CT: real
      Gas-CT_aero: broadcast
```

The split form requires the technology to name its sub-technology column
through `history_class_column` in `scripts/settings.yaml` (`turbine` for
offshore wind, `i` for gas), and the entries selecting `real` must match the
mapping's targets exactly; a disagreement raises rather than leaving one
sub-technology on unintended history. The schema offers `real` only where an
observation exists, so a typo is caught in the editor.
Capacity-factor multipliers are included automatically when the technology
output contains `cf_improvement` (utility PV and the two wind technologies).
`real` applies to every technology with an observed series that measures the
same quantity as its ReEDS column: UPV, land-based wind, offshore wind, gas,
and biopower. Metrics without a reviewed observed mapping use broadcast history,
including heat rate and efficiency.

### Per-technology observed-history appliers

A `real` mapping supplies one national value per year, but technologies do not
all carry one row per year, so the two halves of the work are separated in
`scripts/generate_atb_files.py`:

- `_observed_values_by_year` is shared. It filters the normalized observed
  series, requires a stated `dollar_year`, and deflates to the ReEDS dollar
  year.
- an entry in `REAL_HISTORY_APPLIERS`, keyed by technology, decides which rows
  that annual value is allowed to address. A technology selecting a `real`
  metric without an entry raises `NameError`.

Frames at this stage stack all ATB scenarios together, and history is identical
across them, so repetition across `Scenario` is expected. Repetition on any
other dimension is not, and `_assert_one_row_per_year` raises rather than
letting one value silently overwrite several distinct series.

| Applier | Used by | Behavior |
| --- | --- | --- |
| `apply_real_history_single_series` | `upv`, `wind-ons`, `biopower` | One row per scenario-year; assigns directly. |
| `apply_real_history_by_class` | `wind-ofs`, `gas` | One row per sub-technology, named by `history_class_column`. Each series describes exactly one sub-technology; any other must select its own mode in `historical_data`, otherwise the run raises instead of mixing manual history. |

Why a given technology targets the rows it does is recorded beside its mapping
in `config.yaml`, where that choice is made.

Add a technology by writing an applier that owns its row-shape assumption and
registering it, rather than generalizing an existing one.

### Mapping options

Mappings are nested as `technology -> metric`. Each metric entry accepts:

| Key | Default | Effect |
| --- | --- | --- |
| `filters` | required without `series` | Column/value pairs selecting exactly one row per year from the normalized CSV. |
| `turbine_classes` | required for offshore wind | Offshore only: the one turbine class receiving the observed series. Any other class must select its own mode under `historical_data`. |
| `series` | — | A list of sub-series, each with its own `filters` and `technologies`, for technologies whose rows need different observed series. Entries inherit the mapping's other keys. |

A series measures one thing, so it names exactly one sub-technology. Sharing a
series across several made unrelated rows carry identical history and hid the
fact that no observation existed for the others.

Rows that all take the same series use `filters` directly; rows needing
different series use `series`, as `gas` does:

```yaml
gas:
  capcost:
    series:
      - technologies: [Gas-CC, Gas-CC_H_1x1, Gas-CC_H_2x1]
        filters: {technology_detail: Natural gas combined cycle, ...}
      - technologies: [Gas-CT, Gas-CT_aero]
        filters: {technology_detail: Natural gas combustion turbine, ...}
```

Missing years never fall back to the manual file when `real` is selected.
Internal gaps are linearly interpolated between the surrounding observations.
For years outside the observed range, where interpolation is impossible, the
nearest observed endpoint is used. The smoothing-comparison plot colors these
derived years separately from directly reported observations.

The current defaults in `config.yaml` are:

| Setting | Default | Meaning |
| --- | ---: | --- |
| `projection_start_year` | `2022` | Years before this are historical; this year and later are the current projection |
| `slope_change_threshold` | `0.4` | Detect a normalized slope change of 40% or more |
| `max_kink_years` | `4` | Maximum span of a compact slope-change cluster |
| `major_step_relative_threshold` | `0.1` | Preserve year-to-year changes of 10% or more |
| `minimum_adjustment_relative_threshold` | `0.005` | Keep the original ATB value when a proposed future smoothing adjustment is smaller than 0.5% |

For example, this preserves manually supplied UPV history and passes the ATB
projection through untouched:

```yaml
technologies:
  upv:
    historical_data:
      capcost: manual
      fom: manual
      vom: manual
      cf_improvement: manual
    future_smoothing_treatments:
      smooth_projection_curve: false
```

The older flat-history/anchor-to-target behavior remains available as
`method: linear_bridge`.

### Configuration editor support

`config.yaml` declares `config.schema.json` on its first line. Editors with YAML
language-server support use that schema for completion lists, hover descriptions,
required-field checks, and invalid-option warnings. Use the editor's completion
command (typically `Ctrl+Space`) to choose valid history modes and other options.

Historical choices are technology-by-metric. The schema offers `real` only for
metrics with a reviewed observed mapping; other metrics offer `manual` and
`broadcast`. Offshore wind CapEx additionally accepts a per-turbine-class
mapping of modes. Runtime validation remains authoritative when the workflow
runs.

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
    +--> scrape_historical_costs.py (optional, separate)
    |        +--> scraped_input/historical_costs/*.xlsx
    |        +--> historical_capital_costs.csv + source_manifest.csv
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
writes versioned before/after plots under
`comparison/smoothing_comparison/`. The
temporary CSVs are deleted when the pipeline exits; no row-level or summary CSV
reports are created.

For ATB 2024, the URLs are intentionally pinned to corrected release v3.
Changing the release can change technology trajectories; update both source
URLs together when adopting a newer release.

## Directory layout

| Path | Purpose |
| --- | --- |
| `config.yaml` | User-facing workflow configuration |
| `config.schema.json` | Editor completion lists and validation for `config.yaml` |
| `scraped_input/` | Visible raw ATB and optional observed-cost downloads (local only; not committed) |
| `manual_input/` | Versioned history and inputs unavailable in ATB downloads |
| `scripts/settings.yaml` | Internal per-technology ReEDS formatting rules |
| `scripts/scrape_atb_inputs.py` | Raw-data download and inspection |
| `scripts/scrape_historical_costs.py` | Observed capital-cost download, normalization, and manifest |
| `scripts/generate_atb_files.py` | ReEDS input formatter |
| `scripts/atb_plotting.py` | Raw ATB plotting |
| `output/` | Generated ReEDS-formatted CSVs |
| `figures/` | Generated ATB plots |
| `comparison/smoothing_comparison/` | Versioned before/after smoothing plots |

See [`scripts/README.md`](scripts/README.md) for the scripts folder structure.
