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

For ATB 2024, the URLs are intentionally pinned to corrected release v3.
Changing the release can change technology trajectories; update both source
URLs together when adopting a newer release.

## Directory layout

| Path | Purpose |
| --- | --- |
| `config.yaml` | User-facing workflow configuration |
| `scraped_input/` | Visible, unmodified raw ATB downloads |
| `manual_input/` | Versioned history and inputs unavailable in ATB downloads |
| `scripts/settings.yaml` | Internal per-technology ReEDS formatting rules |
| `scripts/scrape_atb_inputs.py` | Raw-data download and inspection |
| `scripts/generate_atb_files.py` | ReEDS input formatter |
| `scripts/atb_plotting.py` | Raw ATB plotting |
| `output/` | Generated ReEDS-formatted CSVs |
| `figures/` | Generated ATB plots |

See [`scripts/README.md`](scripts/README.md) for the scripts folder structure.
