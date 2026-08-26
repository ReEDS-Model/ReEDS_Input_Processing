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
fully smoothed curve. Each technology has independent switches under
`smooth_cost_curves.technologies`:

| Historical mode | Effect before `projection_start_year` |
| --- | --- |
| `real` | Use the reviewed observed-series mapping under `historical_cost_sources.reeds_mappings` and preserve those values exactly. An unmapped technology raises an error. |
| `manual` | Preserve the versioned rows in `manual_input/historical/` exactly. |
| `broadcast` | Moving backward from the projection boundary, raise the contiguous historical tail below the boundary value. Stop at the first earlier value that already meets or exceeds it. |

The independent switches under `future_smoothing_treatments` apply from
`projection_start_year` onward:

| Future smoothing treatment | Effect |
| --- | --- |
| `enforce_monotonic_projection` | Remove future movement opposite the anchor-to-endpoint direction: costs cannot temporarily increase, while improving multipliers cannot temporarily decrease. |
| `smooth_projection_curve` | Bridge near-equal plateaus and compact clusters of slope changes. Major transitions and single ATB milestones remain explicit. |

Each technology can also independently set `enabled`, `columns`, and
`include_capacity_factor_multiplier`. Inline comments in `config.yaml` list
only the valid options for each technology. `real` applies to every technology
with an observed series that measures the same quantity as its ReEDS column:
UPV, land-based wind, offshore wind, gas, and biopower. Capacity-factor
multiplier selection is available only for
utility PV and the two wind technologies; it is fixed to `false` elsewhere.
Heat rate and efficiency are not changed.

### Per-technology observed-history appliers

A `real` mapping supplies one national value per year, but technologies do not
all carry one row per year, so the two halves of the work are separated in
`scripts/generate_atb_files.py`:

- `_observed_values_by_year` is shared. It filters the normalized observed
  series, requires a stated `dollar_year`, and deflates to the ReEDS dollar
  year.
- an entry in `REAL_HISTORY_APPLIERS`, keyed by technology, decides which rows
  that annual value is allowed to address. A technology selecting
  `historical_data: real` without an entry raises `NameError`.

Frames at this stage stack all ATB scenarios together, and history is identical
across them, so repetition across `Scenario` is expected. Repetition on any
other dimension is not, and `_assert_one_row_per_year` raises rather than
letting one value silently overwrite several distinct series.

| Applier | Used by | Behavior |
| --- | --- | --- |
| `apply_real_history_single_series` | `upv`, `wind-ons`, `biopower` | One row per scenario-year; assigns directly. |
| `apply_real_history_wind_ofs` | `wind-ofs` | One row per turbine class. Assigns to the classes named by `turbine_classes`; the rest keep manual history. |
| `apply_real_history_gas` | `gas` | One row per plant configuration. Each `series` entry assigns to the configurations it names; unclaimed ones keep manual history. |

Why a given technology targets the rows it does is recorded beside its mapping
in `config.yaml`, where that choice is made.

Add a technology by writing an applier that owns its row-shape assumption and
registering it, rather than generalizing an existing one.

### Mapping options

Each entry under `historical_cost_sources.reeds_mappings` accepts:

| Key | Default | Effect |
| --- | --- | --- |
| `output_column` | required | Which ReEDS column the observed series replaces. |
| `filters` | required | Column/value pairs selecting exactly one row per year from the normalized CSV. |
| `require_complete_history` | `true` | Raise unless every year from `reeds_start_year` to the projection boundary is covered. |
| `backfill_to_first_observed_year` | `false` | Hold the earliest observed value flat across required years that precede it. |
| `turbine_classes` | `[fixed]` | Offshore only: which turbine classes the series applies to. |
| `series` | — | A list of sub-series, each with its own `filters` and `technologies`, for technologies whose rows need different observed series. Entries inherit the mapping's other keys. |

Rows that all take the same series use `filters` directly; rows needing
different series use `series`, as `gas` does:

```yaml
gas:
  output_column: capcost
  backfill_to_first_observed_year: true
  series:
    - technologies: [Gas-CC]
      filters: {technology_detail: Natural gas combined cycle, ...}
    - technologies: [Gas-CT]
      filters: {technology_detail: Natural gas combustion turbine, ...}
```

`backfill_to_first_observed_year` exists because some sources start after
`reeds_start_year` — every EIA generator-cost series begins in 2013, the first
edition EIA published. Holding the first observed value flat across the earlier
years keeps the series usable without inventing a trend for years the source
never measured.

It fills **leading** years only. A hole inside the observed range means the
source reported no installations that year, which is a different situation, and
`require_complete_history` still catches it. EIA biomass, missing 2018 in the
middle of an otherwise continuous run, raises even with backfill enabled.

The current defaults in `config.yaml` are:

| Setting | Default | Meaning |
| --- | ---: | --- |
| `projection_start_year` | `2022` | Years before this are historical; this year and later are the current projection |
| `similar_value_relative_tolerance` | `0.001` | Values within 0.1% can form one plateau |
| `similar_value_absolute_tolerance` | `1e-9` | Numerical tolerance near zero |
| `slope_change_threshold` | `0.4` | Detect a normalized slope change of 40% or more |
| `max_kink_years` | `4` | Maximum span of a compact slope-change cluster |
| `major_step_relative_threshold` | `0.1` | Preserve year-to-year changes of 10% or more |

For example, this preserves manually supplied UPV history while retaining only
the future monotonic treatment:

```yaml
technologies:
  upv:
    enabled: true
    include_capacity_factor_multiplier: true
    historical_data: manual
    future_smoothing_treatments:
      enforce_monotonic_projection: true
      smooth_projection_curve: false
```

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
| `scraped_input/` | Visible raw ATB and optional observed-cost downloads (local only; not committed) |
| `manual_input/` | Versioned history and inputs unavailable in ATB downloads |
| `scripts/settings.yaml` | Internal per-technology ReEDS formatting rules |
| `scripts/scrape_atb_inputs.py` | Raw-data download and inspection |
| `scripts/scrape_historical_costs.py` | Observed capital-cost download, normalization, and manifest |
| `scripts/generate_atb_files.py` | ReEDS input formatter |
| `scripts/atb_plotting.py` | Raw ATB plotting |
| `output/` | Generated ReEDS-formatted CSVs |
| `figures/` | Generated ATB plots |

See [`scripts/README.md`](scripts/README.md) for the scripts folder structure.
