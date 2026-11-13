# ATB inputs for ReEDS

Run from `atb/` after checking paths and the ATB release in `config.yaml`:

```bash
python scripts/future_atb_scraper.py
python scripts/run_pipeline.py
```

The downloader caches the configured ATB flat file and workbook. The pipeline
formats projections, joins prepared history, applies configured smoothing, and
creates plots. It never downloads data or rewrites historical inputs. Use
`--only format` to generate CSVs without plots, and `processing.technologies`
to format only some technologies. Outputs go to `output/`;
`processing.copy_to_reeds` controls copying them into ReEDS.
`streamlit run dashboard/app.py` opens a local page that edits the same
switches, previews technologies with the source of each point, and exports by
running the same stages (see `dashboard/README.md`).

## Layout

| Path | Contents |
| --- | --- |
| `config.yaml` | Settings of a pipeline run. The comment at its top lists what to review for a new ATB release. |
| `config_history.yaml` | Source editions and pins used to rebuild the history tables. |
| `scripts/` | The scripts you run. `scripts/helpers/` holds the modules they call. |
| `inputs/historical/` | Committed history tables the pipeline reads. |
| `inputs/manual/` | Hand-maintained inputs: CSP and offshore ratios, plant-level project costs. |
| `inputs/plot_style/` | Trace, label, and color settings of the ATB plots. |
| `inputs/scraped/` | Raw download cache (git-ignored). |
| `comparison/`, `dashboard/` | Output checks against ReEDS, and the local dashboard. |
| `output/`, `figures/` | Generated ReEDS inputs and plots (git-ignored). |

## Updating to a new ATB release

1. Edit `config.yaml`. The comment at the top of the file lists the settings
   to review for a new release; the rest rarely changes.
2. Download the new ATB files: `python scripts/future_atb_scraper.py`.
3. Add the new year of observed history. Under
   `historical_cost_sources.sources` in `config_history.yaml`, point each
   source at its new edition (report year or last year, URL, filename), and
   add any new plant to the project files in `inputs/manual/`. Then rebuild
   the history tables with `python scripts/historical_data_scraper.py` and
   review their diff before committing. The pipeline alone never adds
   historical data.
4. Run `python scripts/run_pipeline.py` and review the comparison plots.

A new release can rename technologies or drop a financial case. The formatter
prints the technologies and display names that changed since the previous
release; check them against the mappings in `scripts/helpers/settings.yaml`.

`config.schema.json` and `config_history.schema.json` list the keys and values
the two config files accept. Editors that support the YAML language server use
them to flag typos and offer the options while you type, and the dashboard
takes its drop-down lists from `config.schema.json`. The pipeline does not
read them and does its own checks, so a new key or option must be added to
both the config file and its schema.

Each series' `future_data` entry in `config.yaml` names the release file it is
read from: `atb_flatfile` (the ATBe csv), `atb_workbook` (the technology
sheet(s) `settings.yaml` names as `workbook_sheet`), or `reeds_baseline` for
series ATB does not publish. A technology is read from one file only, so all its ATB
series must agree, and a file missing any of its display names or metrics
stops the run. Every technology but battery can read either file; the two
agree for every series in ATB 2025. Battery is flat file only and overlays its
power/energy split from the workbook, because the flat file never publishes
it and the battery sheet has no per-series metric blocks. ATB 2025 publishes
Biopower - 95% CCS (`beccs`) only in the workbook; its `beccs_max` series is
not in ATB and is retained from ReEDS `beccs_reference.csv` through
`inputs/historical/reeds_baseline.csv`.

## Historical inputs

The tables in [`inputs/historical/`](inputs/historical/README.md) are
sufficient for historical processing; users do not need the historical raw
downloads.
The defaults use observed data wherever a reviewed source exists (`real`, or
`indexed` for O&M), hold the first ATB projection value flat otherwise
(`backfill`), and write the ReEDS placeholder before a technology's first
buildable year (`unavailable`). `reeds_baseline` instead copies the curve
ReEDS ships today: its `<tech>_ATB_2024_<scenario>.csv` file for the same scenario, stored
in `inputs/historical/reeds_baseline.csv`. That year is pinned by
`historical_data.reference_atb_year` and does not follow `atb.year`.

To rebuild history from original sources:

```bash
python scripts/historical_data_scraper.py
```

Use `--no-download` to rebuild from cached raw files, or `--force` to replace
cached downloads. Preparation downloads observed sources and the pinned
reference release. It writes the tables only after extraction and preparation
succeed. Review their changes before committing them.

Each metric's history mode is set per technology in `config.yaml`:

- `real`: observed anchors from `inputs/historical/real.csv`, interpolated between
  anchors and carried back before the first.
- `indexed`: ATB's value at `index_reference_year` scaled by how an observed
  series changed over time. Used for wind-ons and upv FOM: the LBNL O&M
  surveys leave out part of the cost (the PV survey excludes property taxes,
  insurance, and land costs), so their dollar level is not a full FOM, but
  their change over time is usable.
- `unavailable`: a placeholder cost (`unavailable_value`, 99999) for years
  before the technology's first buildable year, declared per technology in
  `unavailable_before`; used for fuel cells, nuclear-SMR, floating offshore,
  and CCS. The placeholder is written last, after every dollar-year step, and
  never lives in `reeds_baseline.csv`. It is a marker, not the barrier: ReEDS blocks
  investment before its own `firstyear`, so the value only needs to be far
  above any real cost (ReEDS's own files use 9999, which is below Vogtle).
- `reeds_baseline`: the ReEDS ATB 2024 file for the same scenario, as
  described above.
- `backfill`: the first ATB projection value held flat back to
  `reeds_start_year`.

Technologies with only a few builds use plant-level project files under
`inputs/manual/` (nuclear, fixed offshore). The scraper writes a coverage
table into `inputs/historical/README.md` showing every metric's mode, source,
and anchors.

Formatting interpolates missing years, carries the first anchor backward before
coverage, and interpolates from the last anchor to the first projection year.
That last rule matters where ATB omits a technology until 2030 (nuclear): the
curve runs to meet ATB instead of holding flat and then stepping. These fills
are estimates, not additional observations.

`processing.smooth_cost_curves.fill_atbstartyear2atbyear_with_real: true`
also replaces ATB points through the release year for metrics selected as
`real` (2022-2024 for ATB 2024): every year from the series start through the
last observed or calculated anchor, with years between anchors on the straight
line between them. ATB resumes only after the last anchor. This runs after
smoothing and can be overridden per technology. Plots label the replacement
sources.

The pinned reference release in `historical_data.reference_release` moves only
when you decide to move it.

The default projection start follows `atb.year - 2`; individual series retain
their actual start year. Financial cases are set in config without editing
`settings.yaml`: ATB 2025 has no Market case, so its configuration uses
`atb_case: R&D` with `{upv, wind-ons, battery: Exp}`.

With ATB 2025 configured, the real-data overlap is 2023-2025 automatically;
years without source observations stay ATB. `processing.reeds_atb_year: 2024`
compares outputs against ReEDS's ATB 2024 files until ReEDS carries 2025, and
`reeds_end_year` caps the written years. The pinned history references
(`reference_atb_year`, the two `reference_year` settings) stay at the ATB 2024
release on purpose; `index_reference_year` and the CCS `unavailable_before`
years move with the ATB base year (2023 for ATB 2025).

## Data treatment

- Monetary rows of the history tables are stored in the dollar year set by
  `historical_data.dollar_year` in `config_history.yaml` (2022) and converted to the
  output dollar year (`atb.dollar_year`) with the ReEDS deflator. Nonmonetary
  rows have no dollar year.
- Observed capital costs are reported on wider boundaries than ReEDS `capcost`,
  which is the ATB overnight capital cost. Each scraped cost declares a
  `cost_scope`, and preparation removes the grid connection and, where the source
  is an as-spent project cost, the construction financing, using ATB components
  from `historical_data.reference_atb_year`. Configure this under
  `historical_cost_sources.cost_scope_adjustment`; see
  [`inputs/historical/README.md`](inputs/historical/README.md) for the per-source boundaries
  and the offshore-wind caveat.
- Real capacity factors remain fractions in the prepared files;
  formatting divides them by the current ATB reference capacity factor.
- Battery power and energy costs are estimated from observed total cost and
  cohort duration while preserving ATB reference component proportions.
- CSP configuration costs use the same ratio method as projections. Historical
  reference ratios and the battery split workbook are pinned by
  `historical_data.reference_atb_year` (2024), independently of future updates.
- The ReEDS baseline table retains complete ReEDS curves for retired designs
  and backfill reference values. These are labeled `reeds_baseline`, including future
  reference rows; they are not claimed as measured historical costs.

The scraper pulls ReEDS baseline curves, `dollaryear.csv`, and the financial
deflator from GitHub at the commit pinned in `reeds_source.ref`, so rebuilding
history needs no local ReEDS checkout. A local repository is still needed for
comparison baselines and optional copying. Technology formatting rules are in
`scripts/helpers/settings.yaml`; review year-specific files under
`inputs/manual/` when changing the ATB release.

## CCS retrofit upgrade inputs

`scripts/ccs_retrofit_inputs.py` builds the ReEDS `inputs/upgrades/` CCS
retrofit files from the cached flat file (`--copy-to-reeds` also copies them):
`upgrade_mult_atb<yy>_ccs_{mid,adv,con}.csv`, unitless multipliers on the EIA
unit-level retrofit cost of existing plants (`GSw_UpgradeCost_Mult`), and
`upgrade_costs_ccs_{coal,gas}.csv`, retrofit capital cost in 2004$/kW
(`ccs_upgrade_cost_case`). Both use the ATB retrofit "Additional OCC" series
of one 90%-CCS technology per fuel (the ReEDS retrofit capture rate,
`GSw_CCS_Rate_Upgrade_mod`), mapped under `ccs_retrofits` in
`scripts/helpers/settings.yaml`. A multiplier is each scenario's cost divided by the
Moderate cost in 2028, which reproduces the ATB 2023 files in ReEDS exactly.
Costs are converted with the ReEDS deflator.
Years before the ATB base year hold the base-year value; rows follow ReEDS
`upgrade_link.csv`, and upgrades that are not CCS retrofits get 1.
