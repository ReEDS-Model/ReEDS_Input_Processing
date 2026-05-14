# Inputs for Alpha Regression

This directory contains input files used by the alpha regression step.

## Historical CSVs (manual inputs)

The following files are copied from the main [ReEDS repository](https://github.com/ReEDS-Model/ReEDS) and provide historical data to backfill years (2010 – most recent year) that are not covered by AEO projections:

- `ng_AEO_historical.csv` — Historical NG prices
- `ng_demand_AEO_historical.csv` — Historical electric sector NG demand
- `ng_tot_demand_AEO_historical.csv` — Historical total sector NG demand
- `st_cendiv.csv` — State to Census Division mapping

Each pipeline run automatically appends the current AEO's calibration year (e.g., AEO 2025 appends 2024) to these historical CSVs, so the next AEO version has seamless year coverage with no manual update needed.

## Auto-generated files

During the pipeline run, `sync_beta_to_alpha_inputs.py` copies beta regression results (`cd_beta0.csv`, `national_beta.csv`) into this directory. These are then read by `aeo_alpha_regression.py`.
