# scraped_input/

This directory exposes the two independent raw downloads used by the NLR ATB
workflow. Neither file is generated from the other.

| File | Upstream source | Used for |
| --- | --- | --- |
| `atb_<year>_flat_file.csv` | ATB summary flat file (`ATBe.csv`) on OEDI | Primary cost, performance, and financial data |
| `atb_<year>_workbook.xlsx` | ATB Excel data workbook | Battery power ($/kW) and energy ($/kWh) capital-cost components |

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

Both raw files are local only and are never committed. The flat file is far
above the 50 MB limit in the top-level [`README.md`](../../README.md) File Size
Guidelines, and both files can be pulled directly from their upstream source, so
this directory is ignored except for this README. Reproducibility comes from the
URLs instead: they are pinned to a specific ATB release in
[`../config.yaml`](../config.yaml) (ATB 2024 is pinned to corrected release v3),
so re-running the scraper restores the exact inputs used for a run.
