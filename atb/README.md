# ATB inputs for ReEDS

Run from `atb/` after checking paths and the ATB release in `config.yaml`:

```bash
python scripts/future_atb_scraper.py
python scripts/run_pipeline.py
```

The downloader caches the configured ATB flat file and workbook. The pipeline
formats projections, joins prepared history, applies configured smoothing, and
creates plots. It never downloads data or rewrites historical inputs. Use
`--only format` to generate CSVs without plots. Outputs go to `output/`;
`processing.copy_to_reeds` controls copying them into ReEDS.

## Historical inputs

The three versioned tables in [`historical/`](historical/README.md) are sufficient
for historical processing; users do not need the historical raw downloads.
Defaults select reviewed real data, then archived ATB estimates, then broadcast
the first projection value when no archived series matches. Each metric can
also explicitly select `manual` in `config.yaml`.

To rebuild history from original sources:

```bash
python scripts/historical_data_scraper.py
```

Use `--no-download` to rebuild from cached raw files, or `--force` to replace
cached downloads. Preparation downloads observed sources and historical ATB
releases, including the configured current release. It writes all three CSVs
only after extraction and preparation succeed. Review their changes before
committing them. Manual values always come from the ReEDS **ATB 2024** files.

Archived anchors use exactly `historical year = ATB release year - 2`.
Formatting interpolates missing years, carries the first anchor backward before
coverage, and interpolates from the last anchor to the first projection year.
That last rule matters where ATB omits a technology until 2030 (nuclear,
nuclear-SMR, floating offshore): the curve rises to meet ATB instead of holding
flat and then stepping. These fills are estimates, not additional annual ATB
observations.
ATB history uses Moderate estimates across output scenarios.

`processing.smooth_cost_curves.fill_atbstartyear2atbyear_with_real: true`
also replaces available ATB points through the release year for metrics selected
as `real` (2022-2024 for ATB 2024). Only observed or calculated source anchors
qualify; filled years keep their ATB values. This runs after smoothing and can
be overridden per technology. Plots label the replacement sources.

During an annual update, update the configured release, URLs, dollar year and
technology mappings, download future ATB, then rerun historical preparation to
add the new release's base year. Previous releases are recovered from the
prepared table's provenance or the cached manifest. Pipeline runs alone never
add historical data. ATB schema or technology changes still require review.

The default projection start follows `atb.year - 2`; individual series retain
their actual start year. Financial cases can be changed in config without
editing `settings.yaml`. To match `yc/25ATB`, use:

```yaml
processing:
  atb_case: R&D
  atb_case_overrides: {upv: Exp, wind-ons: Exp, battery: Exp}
```

With ATB 2025 configured, the real-data overlap becomes 2023-2025 automatically;
years without source observations stay ATB. Release URLs, dollar year, and
year-specific future adjustment files still need the normal annual update.

## Data treatment

- Monetary history is stored in `historical_data.dollar_year` and converted to
  the output dollar year with the ReEDS deflator. Nonmonetary rows have no
  dollar year.
- Real and archived capacity factors remain fractions in the prepared files;
  formatting divides them by the current ATB reference capacity factor.
- Battery power and energy costs are estimated from observed total cost and
  cohort duration while preserving ATB reference component proportions.
- CSP configuration costs use the same ratio method as projections. Historical
  reference ratios and the battery split workbook are pinned by
  `historical_data.reference_atb_year` (2024), independently of future updates.
- The manual baseline retains complete ReEDS curves for retired designs and
  broadcast reference values. These are labeled manual, including future
  reference rows; they are not claimed as measured historical costs.

The scraper pulls ReEDS baseline curves, `dollaryear.csv`, and the financial
deflator from GitHub at the commit pinned in `reeds_source.ref`, so rebuilding
history needs no local ReEDS checkout. A local repository is still needed for
comparison baselines and optional copying. Technology formatting rules are in
`scripts/settings.yaml`; review year-specific files under `manual_input/`
when changing the ATB release.
