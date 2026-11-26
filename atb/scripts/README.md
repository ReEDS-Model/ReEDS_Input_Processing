# ATB scripts

Scripts you run:

| File | Role |
| --- | --- |
| `future_atb_scraper.py` | Download the configured future ATB flat file and workbook. |
| `historical_data_scraper.py` | Download historical sources and prepare the history tables in `../inputs/historical/`. |
| `run_pipeline.py` | Run formatting, plotting, and comparisons from local inputs. |
| `ccs_retrofit_inputs.py` | Build the ReEDS `inputs/upgrades` CCS retrofit multipliers and costs from the ATB flat file. |

Modules those scripts call, in `helpers/`:

| File | Role |
| --- | --- |
| `generate_atb_files.py` | Format stage: format projections and merge selected history. |
| `historical_data.py` | Validate, select, and apply prepared history. |
| `observed_sources.py`, `real_history_sources.py` | Extract observations and calculate mapped real series. |
| `atb_workbook.py` | Extract battery power/energy capital components and technology-sheet metrics from the ATB workbook. |
| `downloads.py`, `atb_config.py` | Shared download and configuration helpers. |
| `atb_plotting.py` | Plot stage: plot the current ATB flat file. |
| `settings.yaml` | Internal technology mappings and ReEDS output schemas. |

User choices belong in `../config.yaml`. In `settings.yaml`, `reeds_name`
controls output file prefixes and `output_cols` controls final column names
and order; both must agree with the ReEDS input format.
