# scripts/

All scripts share the user configuration in [`../config.yaml`](../config.yaml).
User-facing run instructions are in the main [`../README.md`](../README.md).

| File | Role |
| --- | --- |
| `run_pipeline.py` | Runs the config-enabled scrape, format, and plot stages in order |
| `scrape_atb_inputs.py` | Downloads and summarizes both raw ATB inputs |
| `scrape_historical_costs.py` | Separately downloads and normalizes observed capital-cost workbooks |
| `generate_atb_files.py` | Converts local raw and manual inputs into ReEDS-formatted CSVs |
| `atb_plotting.py` | Plots configured metrics from the local raw flat file |
| `atb_config.py` | Shared config and path-loading functions |
| `settings.yaml` | Internal per-technology mappings and transformations |
| `battery_workbook.py` | Extracts battery cost components from the downloaded workbook; imported by the formatter |

Normal workflow choices belong in `../config.yaml`; `settings.yaml` is only for
technology-specific formatting rules.

## settings.yaml keys that control the ReEDS output format

Most keys describe how to pull a technology out of the ATB flat file. These two
instead describe how the result must be written so ReEDS can read it, and are
only needed where the ReEDS representation differs from the internal one:

| Key | Effect |
| --- | --- |
| `reeds_name` | File prefix used in ReEDS when it differs from the technology key (`wind-ons` -> `ons-wind`, `wind-ofs` -> `ofs-wind`). Also becomes the `Scenario` key written to the ReEDS `dollaryear.csv`. |
| `output_cols` | Ordered mapping of internal column name -> ReEDS header, applied as the last step before writing. Required for the wind files because ReEDS assigns their columns by position, not by name. |

Both are optional. A technology without them is written as
`<tech>_ATB_<year>_<scenario>.csv` using the internal column names listed under
`cols`. `output_cols` must cover every column in `cols`; the formatter raises an
error rather than silently dropping or reordering one.

See "File names and column schemas expected by ReEDS" in the main
[`../README.md`](../README.md) for why the wind files need this.
