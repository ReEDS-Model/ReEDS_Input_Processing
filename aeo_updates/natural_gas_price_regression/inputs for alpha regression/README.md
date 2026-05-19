# Inputs for Alpha Regression

This directory contains input files used by the alpha regression step.

## Manual inputs

- `st_cendiv.csv` — State to Census Division mapping.

## Historical NG price and demand data

Historical years are no longer maintained in this directory. `aeo_alpha_regression.py` pulls the full year range (`start_year` – `end_year` from `aeo_pipeline_config.json`, e.g., 2010 – 2050) directly from the EIA AEO API in a single request. Because history and projections come from the same AEO release, every year is in the same dollar year (e.g., AEO 2025 → 2024 USD/MMBtu), and the `price_deflator_to_2004` value in the config correctly converts the entire series to 2004 USD/MMBtu.

If the AEO API returns fewer historical years than `start_year` requests, the pipeline logs a warning and validates from the first available year onward.

## Auto-generated files

During the pipeline run, `sync_beta_to_alpha_inputs.py` copies beta regression results (`cd_beta0.csv`, `national_beta.csv`) into this directory. These are then read by `aeo_alpha_regression.py`.
