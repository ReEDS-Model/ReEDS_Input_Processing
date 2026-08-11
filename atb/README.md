# ATB input pipeline

This directory turns raw NLR Annual Technology Baseline (ATB) data into ReEDS
input files and plots. The workflow has three explicit stages:

1. download and inspect raw data;
2. format the local raw data for ReEDS;
3. plot metrics from the same local raw data.

## Start here

[`config.yaml`](config.yaml) is the user-facing control file. It shows which
stages will run, the ATB release and source URLs, local filenames, ReEDS path,
technologies, processing choices, and plotting choices.

From this `atb/` directory, run the configured workflow with:

```bash
python scripts/run_pipeline.py
```

The runner prints the selected plan before doing any work. Individual stages
can also be run directly:

```bash
python scripts/scrape_atb_inputs.py
python scripts/generate_atb_files.py
python scripts/atb_plotting.py
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
    |        +--> manual_input/ (CSP ratios and historic/fallback data)
    |        +--> existing ReEDS inputs (history and dollar-year conversion)
    |        +--> output/*_ATB_<year>_<scenario>.csv
    |
    +--> atb_plotting.py
             +--> the same raw flat file
             +--> figures/
```

The flat file and workbook are independent upstream downloads. Neither is
generated from the other. The formatter and plotter do not download data; they
only consume the raw files created by the scraper.

## Directory layout

| Path | Purpose |
| --- | --- |
| `config.yaml` | User-facing workflow configuration |
| `scraped_input/` | Visible, unmodified raw ATB downloads |
| `manual_input/` | Hand-maintained inputs not available in the ATB downloads |
| `scripts/settings.yaml` | Internal per-technology ReEDS formatting rules |
| `scripts/scrape_atb_inputs.py` | Raw-data download and inspection |
| `scripts/generate_atb_files.py` | ReEDS input formatter |
| `scripts/atb_plotting.py` | Raw ATB plotting |
| `output/` | Generated ReEDS-formatted CSVs |
| `figures/` | Generated ATB plots |

See [`scripts/README_atb-processing.md`](scripts/README_atb-processing.md) and
[`scripts/README_atb-plotting.md`](scripts/README_atb-plotting.md) for stage
details.
