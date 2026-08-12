# scripts/

All scripts share the user configuration in [`../config.yaml`](../config.yaml).
User-facing run instructions are in the main [`../README.md`](../README.md).

| File | Role |
| --- | --- |
| `run_pipeline.py` | Runs the config-enabled scrape, format, and plot stages in order |
| `scrape_atb_inputs.py` | Downloads and summarizes both raw ATB inputs |
| `generate_atb_files.py` | Converts local raw and manual inputs into ReEDS-formatted CSVs |
| `atb_plotting.py` | Plots configured metrics from the local raw flat file |
| `atb_config.py` | Shared config and path-loading functions |
| `settings.yaml` | Internal per-technology mappings and transformations |
| `battery_workbook.py` | Extracts battery cost components from the downloaded workbook; imported by the formatter |

Normal workflow choices belong in `../config.yaml`; `settings.yaml` is only for
technology-specific formatting rules.
