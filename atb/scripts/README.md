# scripts/

All ATB processing code and configuration.

- `settings.yaml` — user settings (ATB year, source, tech definitions).
- `generate_atb_files.py` — main pipeline: reads ATB data plus `../scraped_input/`
  and `../manual_input/`, writes formatted files to `../output/`. See
  [`README_atb-processing.md`](README_atb-processing.md).
- `scrape_battery_inputs.py` — downloads the raw ATB workbook into
  `../scraped_input/`; its `download_workbook` / `extract_battery_costs`
  functions are imported by `generate_atb_files.py` to read battery costs from
  the workbook at runtime.
- `atb_plotting.py` — plotting utilities. See
  [`README_atb-plotting.md`](README_atb-plotting.md).

Run from this folder in the `reeds2` environment, e.g.
`python generate_atb_files.py -f`.
