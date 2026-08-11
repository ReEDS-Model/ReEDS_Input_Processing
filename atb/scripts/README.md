# scripts/

The scripts use the shared user configuration in [`../config.yaml`](../config.yaml).

- `run_pipeline.py` — runs the enabled stages in order and prints the plan.
- `scrape_atb_inputs.py` — downloads both configured raw inputs and displays
  their paths, sizes, and contents/structure.
- `generate_atb_files.py` — reads local raw and manual inputs and writes
  ReEDS-formatted CSVs. It does not access the network.
- `atb_plotting.py` — reads the same local flat file and writes configured plots.
- `atb_config.py` — shared config and path loader.
- `settings.yaml` — internal technology mappings and transformations; normal
  workflow choices belong in `../config.yaml`.
- `scrape_battery_inputs.py` — battery-workbook extraction helper retained for
  the formatter and specialized preview use.

Run commands from either `atb/` or `atb/scripts/`; paths are resolved relative
to the `atb/` directory rather than the current working directory.
