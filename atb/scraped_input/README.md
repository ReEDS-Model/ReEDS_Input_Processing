# scraped_input/

Intermediate files produced automatically by the scrapers in
[`../scripts/`](../scripts/) and consumed by `generate_atb_files.py`.

- `battery_costs_<year>.csv` — battery power ($/kW) and energy ($/kWh) capital
  costs, produced by `scrape_battery_inputs.py`.
- `_workbook_cache/` — cached ATB Excel workbook downloads (git-ignored).

Regenerate with, e.g.:

```bash
python ../scripts/scrape_battery_inputs.py --year 2024
```

Do not hand-edit these files; update the source scraper instead.
