# ATB inputs for ReEDS

Run from `atb/` after checking paths and the ATB release in `config.yaml`:

```bash
python scripts/future_atb_scraper.py   # download the ATB flat file and workbook
python scripts/run_pipeline.py         # format, plot, compare
```

Outputs go to `output/`; `processing.copy_to_reeds` also copies them into
ReEDS. `--only format` skips the plots, and `processing.technologies` limits
the technologies. The pipeline never downloads data or rewrites history.
`streamlit run dashboard/app.py` opens a local page over the same settings
(see `dashboard/README.md`).

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

1. Edit `config.yaml`; the comment at its top lists the settings to review.
2. Download the new ATB files: `python scripts/future_atb_scraper.py`.
3. Add the new year of observed history: point each source under
   `historical_cost_sources.sources` in `config_history.yaml` at its new
   edition, add any new plant to the project files in `inputs/manual/`, run
   `python scripts/historical_data_scraper.py`, and review the diff of the
   history tables before committing.
4. Run `python scripts/run_pipeline.py` and review the comparison plots.

A new release can rename technologies or drop a financial case. The formatter
prints the names that changed; check them against
`scripts/helpers/settings.yaml`.

The two `*.schema.json` files list the keys and values the config files
accept, for editor hints and the dashboard drop-downs. The pipeline does not
read them, so add a new key or option to both the config file and its schema.

## Projection data

Each series' `future_data` in `config.yaml` names its source:

- `atb_flatfile`: the ATB flat file.
- `atb_workbook`: the workbook sheet `settings.yaml` names as `workbook_sheet`.
- `reeds_baseline`: a series ATB does not publish, kept from ReEDS.

All ATB series of one technology read the same file, and a missing display
name or metric stops the run. Battery reads the flat file and always takes its
power/energy split from the workbook. ATB 2025 publishes Biopower - 95% CCS
(`beccs_mod`) only in the workbook.

The financial case is `atb_case`, with `atb_case_overrides` per technology.
ATB 2025 has no Market case, so the default is `R&D`, with `Exp` for upv,
wind-ons, and battery.

## Historical data

The pipeline reads history from the committed tables in
[`inputs/historical/`](inputs/historical/README.md); that README documents
the sources and methods. Each metric's mode is set per technology in
`config.yaml`:

| Mode | Years before the first ATB year |
| --- | --- |
| `real` | Observed values from `real.csv`, interpolated between anchors, carried back before the first, and interpolated from the last anchor to the first projection year. |
| `indexed` | ATB's value at `index_reference_year`, scaled by the change of an observed series. Used for wind-ons and upv FOM, where the LBNL O&M surveys cover only part of the cost. |
| `backfill` | The first ATB projection value held flat. |
| `reeds_baseline` | The curve ReEDS ships today (its ATB 2024 files), from `reeds_baseline.csv`. |
| `unavailable` | `unavailable_value` (99999) before the technology's first buildable year in `unavailable_before`. |

With `fill_atbstartyear2atbyear_with_real: true`, `real` metrics also replace
the ATB points up to their last observed year (within 2023-2025 for ATB 2025).

History tables are in `historical_data.dollar_year` (2022) dollars and are
converted to `atb.dollar_year` with the ReEDS deflator.

To rebuild the tables from the original sources:

```bash
python scripts/historical_data_scraper.py   # --no-download uses the cache, --force refreshes it
```

The scraper fetches the ReEDS files it needs from GitHub at the commit in
`reeds_source.ref`, so it needs no local ReEDS checkout.

These settings stay at ATB 2024 on purpose and do not follow `atb.year`:
`historical_data.reference_atb_year` and `reference_release` (ReEDS baseline,
battery split, cost-scope components), and `processing.reeds_atb_year` (the
ReEDS files the outputs are compared against).

## CCS retrofit upgrade inputs

`python scripts/ccs_retrofit_inputs.py` builds the ReEDS `inputs/upgrades/`
files from the flat file (`--copy-to-reeds` also copies them):

- `upgrade_mult_atb<yy>_ccs_{mid,adv,con}.csv`: unitless multipliers on the
  EIA unit-level retrofit cost of existing plants (`GSw_UpgradeCost_Mult`).
  Each is the scenario's cost divided by the Moderate cost in 2028, the method
  behind the ATB 2023 files in ReEDS.
- `upgrade_costs_ccs_{coal,gas}.csv`: retrofit capital cost in 2004$/kW
  (`ccs_upgrade_cost_case`), converted with the ReEDS deflator.

Both use the ATB retrofit "Additional OCC" of one 90%-CCS technology per fuel
(the ReEDS retrofit capture rate, `GSw_CCS_Rate_Upgrade_mod`), mapped under
`ccs_retrofits` in `scripts/helpers/settings.yaml`. Years before the ATB base
year hold the base-year value. Rows follow ReEDS `upgrade_link.csv`; upgrades
that are not CCS retrofits get 1.
