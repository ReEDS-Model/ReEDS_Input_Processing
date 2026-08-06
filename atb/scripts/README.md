# scripts/

All ATB processing code and configuration.

- `settings.yaml` — user settings (ATB year, source, tech definitions).
- `generate_atb_files.py` — main pipeline: reads ATB data plus `../scraped_input/`
  and `../manual_input/`, writes formatted files to `../output/`. See
  [`README_atb-processing.md`](README_atb-processing.md).
- `scrape_battery_inputs.py` — downloads the ATB workbook and writes
  `../scraped_input/battery_costs_<year>.csv`.
- `atb_plotting.py` — plotting utilities. See
  [`README_atb-plotting.md`](README_atb-plotting.md).

Run from this folder in the `reeds2` environment, e.g.
`python generate_atb_files.py -f`.
