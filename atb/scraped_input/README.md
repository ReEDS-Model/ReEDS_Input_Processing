# scraped_input/

Raw files scraped directly from the NREL ATB, consumed by the scripts in
[`../scripts/`](../scripts/). These are downloaded automatically when
`generate_atb_files.py` runs (and can be pre-fetched manually).

- `atb_<year>_flat_file.csv` — the ATB summary flat file (ATBe.csv). Source of
  cost/performance data for most technologies. Downloaded by
  `generate_atb_files.py` (`atb_source: url`).
- `atb_<year>_workbook.xlsx` — the ATB Excel workbook. Source of the battery
  power ($/kW) and energy ($/kWh) capital-cost split, which is not in the flat
  file. Read at runtime by `generate_atb_files.py`.

These are the raw downloads only — no intermediate/derived files live here.
Battery costs are extracted from the workbook at runtime rather than stored.

Pre-fetch manually with, e.g.:

```bash
python ../scripts/scrape_battery_inputs.py --year 2024
```
