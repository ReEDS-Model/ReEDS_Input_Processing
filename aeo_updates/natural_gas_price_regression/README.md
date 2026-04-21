# Natural Gas Price Regression

This folder has the natural gas update workflow for ReEDS.

## Model Note

`alpha` differs by step:

- Beta step (`alpha1`) is `alpha1(region, year)` and is shared across scenarios.
- Alpha step (`alpha2`) is `alpha2(region, year, scenario)`, so each output
  scenario has its own alpha path.

## Prerequisites

### EIA API Key

The pipeline fetches data from the EIA API and requires an API key. Set it as an environment variable before running:

**Windows (persistent — open a new terminal after running):**
```cmd
setx EIA_API_KEY your_api_key_here
```

**Windows (current session only):**
```cmd
set EIA_API_KEY=your_api_key_here
```

**macOS/Linux:**
```bash
export EIA_API_KEY=your_api_key_here
```

> The key is read from the `EIA_API_KEY` environment variable. Do **not** hardcode it in `aeo_pipeline_config.json`.

## Run order

1. Edit `aeo_pipeline_config.json` with your settings.

2. Run **beta regression first**.

```bash
python aeo_beta_regression.py --config aeo_pipeline_config.json
```

3. Sync beta outputs into alpha inputs.

```bash
python sync_beta_to_alpha_inputs.py --config aeo_pipeline_config.json
```

4. Run **alpha regression**.

```bash
python aeo_alpha_regression.py --config aeo_pipeline_config.json
```

5. Generate all diagnostic plots and validation.

```bash
python visualization.py --config aeo_pipeline_config.json
```

Skip individual parts with flags:
```bash
python visualization.py --skip-raw-scatter --skip-validation
```

Default output folder: `results validation` (all plots and validation CSVs).

## Scenario Configuration

Set explicit scenarios in `aeo_pipeline_config.json` under `scenarios`:

- `scenarios.beta_regression.include`: scenarios used to estimate beta.
- `scenarios.alpha_regression.fetch`: scenarios fetched for alpha preprocessing.
- `scenarios.alpha_regression.outputs`: mapping from output suffix (`reference`, `HOG`, `LOG`, etc.) to scenario aliases.

Use canonical IDs (for example `ref{aeo_year}`, `highogs`, `lowogs`) for a clear setup.

## One-command batch (Windows)

```bat
run_ng_pipeline.bat
```

Optional custom config:

```bat
run_ng_pipeline.bat my_config.json
```
