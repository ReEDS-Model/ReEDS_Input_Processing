# ATB scripts

| File | Role |
| --- | --- |
| `future_atb_scraper.py` | Download the configured future ATB flat file and workbook. |
| `historical_data_scraper.py` | Download historical sources and prepare the three versioned historical tables. |
| `run_pipeline.py` | Run formatting, plotting, and comparisons from local inputs. |
| `generate_atb_files.py` | Format projections and merge selected history. |
| `historical_data.py` | Validate, select, and apply prepared history. |
| `observed_sources.py`, `real_history_sources.py` | Extract observations and calculate mapped real series. |
| `archived_sources.py`, `historical_atb.py` | Extract and map archived ATB estimates. |
| `battery_workbook.py` | Extract battery power/energy capital components. |
| `downloads.py`, `atb_config.py` | Shared download and configuration helpers. |
| `atb_plotting.py` | Plot the current ATB flat file. |
| `settings.yaml` | Internal technology mappings and ReEDS output schemas. |

User choices belong in `../config.yaml`. In `settings.yaml`, `reeds_name`
controls output file prefixes and `output_cols` controls final column names
and order; both must agree with the ReEDS input format.
