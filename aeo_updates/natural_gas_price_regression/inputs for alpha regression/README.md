# Inputs for Alpha Regression

This directory contains input files used by the alpha regression step.

## Manual inputs

- `st_cendiv.csv` — State to Census Division mapping.

## Auto-generated files

During the pipeline run, `sync_beta_to_alpha_inputs.py` copies beta regression results (`cd_beta0.csv`, `national_beta.csv`) into this directory. These are then read by `aeo_alpha_regression.py`.
