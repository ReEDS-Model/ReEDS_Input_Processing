# Raw download cache

- `atb_<year>_flat_file.csv` and `atb_<year>_workbook.xlsx`: current projection
  inputs downloaded by `scripts/future_atb_scraper.py`.
- `historical_costs/`: observed-source downloads, normalized observations,
  and their source manifest.
- `historical_atb/`: archived ATB downloads, release-minus-two estimates,
  and their source manifest.
- `reeds_source/<ref>/`: ReEDS repository files fetched at the commit pinned in
  `reeds_source.ref` (baseline plant characteristics, `dollaryear.csv`, and the
  financial deflator). The ref is part of the path, so changing it fetches fresh
  copies instead of reusing another version's files.

`historical_data_scraper.py` maintains the historical caches and prepares the
versioned CSVs in `../historical/`. These raw caches are ignored by Git and are
not needed to use the prepared history. Keep them to rebuild with
`--no-download`; use `--force` to refresh public downloads.
