# ATB input processing

Tools for turning NREL Annual Technology Baseline (ATB) data into the
per-technology cost/performance files that ReEDS consumes.

## Folder structure

| Folder                            | Contents                                                                                                      |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| [`scripts/`](scripts/)           | All Python scripts and`settings.yaml`. Start here to run the pipeline.                                      |
| [`input/`](input/)               | Intermediate files produced by the scrapers (e.g.`battery_costs_*.csv`) and the cached ATB workbook.        |
| [`manual_input/`](manual_input/) | Hand-maintained inputs that are **not** scraped from ATB (CSP cost ratios, historic capacity factors). |
| `output/`                       | Generated per-technology CSVs written by`generate_atb_files.py` (git-ignored).                              |

## Quick start

```bash
conda activate reeds2
cd atb/scripts
python generate_atb_files.py -f
```

See [`scripts/README_atb-processing.md`](scripts/README_atb-processing.md) for
full processing details and [`scripts/README_atb-plotting.md`](scripts/README_atb-plotting.md)
for plotting.
