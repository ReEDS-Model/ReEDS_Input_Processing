# scraped_input/

This directory exposes the independent raw downloads used by the NLR ATB
workflow. None of the source files is generated from another.

| File | Upstream source | Used for |
| --- | --- | --- |
| `atb_<year>_flat_file.csv` | ATB summary flat file (`ATBe.csv`) on OEDI | Primary cost, performance, and financial data |
| `atb_<year>_workbook.xlsx` | ATB Excel data workbook | Battery power ($/kW) and energy ($/kWh) capital-cost components |

## Observed historical capital costs

The optional `historical_costs/` subdirectory contains original research
workbooks from LBNL, NLR, and EIA together with two generated raw-data aids:

- `historical_capital_costs.csv`: long-form observed capital costs with units,
  capacity basis, statistic, geography, dollar year, and source provenance.
- `source_manifest.csv`: upstream URLs, local filenames, byte counts, and
  SHA-256 checksums for every downloaded workbook.

Download and extract them separately from the ATB inputs:

```bash
python ../scripts/scrape_historical_costs.py
```

Use `--only wind`, `--only solar`, `--only offshore`, or `--only eia` to limit
the sources. Use `--no-download` to rebuild the two CSVs from existing local
workbooks. The formatter uses only explicitly reviewed mappings under
`historical_cost_sources.reeds_mappings` in `config.yaml`. Currently, the LBNL
AC-based UPV and national land-based-wind capital-cost series replace their
respective `capcost` columns before the configured projection boundary; other
series remain review inputs.

Important distinctions retained in the CSV are:

- LBNL utility-PV costs are included on both AC and DC capacity bases.
- NLR offshore costs remain separate by geography, and pipeline years after
  2023 are excluded from the observed series.
- EIA costs are broad, nominal-year generator categories; they are not exact
  matches for detailed ATB technologies.

The URLs and local filenames are declared under `raw_data:` in
[`../config.yaml`](../config.yaml). Download and inspect both files with:

```bash
python ../scripts/scrape_atb_inputs.py
```

Useful variants:

```bash
python ../scripts/scrape_atb_inputs.py --only flat
python ../scripts/scrape_atb_inputs.py --only workbook
python ../scripts/scrape_atb_inputs.py --force
```

Without `--force`, existing files are reused and displayed. These files remain
raw and unmodified: derived ReEDS inputs go in `../output/`, and plots go in
`../figures/`.

HTTPS certificates are checked first. The
`raw_data.allow_insecure_ssl_fallback` setting controls whether a failed check
may be retried with `verify=False`, which can be needed when the active conda
environment does not trust an NLR network inspection certificate.

All raw files and generated raw-data CSVs are local only and are never
committed. The ATB flat file is far
above the 50 MB limit in the top-level [`README.md`](../../README.md) File Size
Guidelines, and both files can be pulled directly from their upstream source, so
this directory is ignored except for this README. Reproducibility comes from the
URLs instead: they are pinned to a specific ATB release in
[`../config.yaml`](../config.yaml) (ATB 2024 is pinned to corrected release v3),
so re-running the scraper restores the exact inputs used for a run.
