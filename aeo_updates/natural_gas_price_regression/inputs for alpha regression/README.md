# Inputs for Alpha Regression

This directory contains input files used by the alpha regression step.

## Historical CSVs

These files provide historical data to backfill years (2010 – most recent year) that are not covered by AEO projections. The source data comes from the ReEDS repo (`ReEDS-2.0/inputs/fuelprices/`).

- `ng_AEO_historical.csv` — Historical NG prices by census division (USD/MMBtu). These input values are always in 2024$ (from AEO 2025) and should not be modified. The pipeline automatically converts them to the current AEO's dollar year (AEO year − 1) in the output when `price_deflator_to_2004` is updated in the config.
- `ng_demand_AEO_historical.csv` — Historical electric-sector NG demand by census division (Quads). 
- `ng_tot_demand_AEO_historical.csv` — Historical total (all-sector) NG demand by census division (Quads).
- `st_cendiv.csv` — State to Census Division mapping.

Each pipeline run automatically appends the current AEO's calibration year (e.g., AEO 2026 appends 2025) to these CSVs. For prices, the appended value is deflated back to 2024$ so the file remains in a consistent dollar year.

## Auto-generated files

During the pipeline run, `sync_beta_to_alpha_inputs.py` copies beta regression results (`cd_beta0.csv`, `national_beta.csv`) into this directory. These are then read by `aeo_alpha_regression.py`.
