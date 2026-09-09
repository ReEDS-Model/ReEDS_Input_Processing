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

- `historical_capital_costs.csv`: long-form capital cost, O&M, and CF observations with units,
  capacity basis, statistic, geography, dollar year, and source provenance.
- `source_manifest.csv`: upstream URLs, local filenames, byte counts, and
  SHA-256 checksums for every downloaded workbook.

The LBNL workbooks also supply `metric: capacity_factor`, `unit: fraction` rows:
wind uses `Capacity Factor in 2024 by COD` (generation-weighted CF, 2006–2023
individual COD years); PV uses `CF by Project Vintage` (capacity-weighted
cumulative CF through 2024, 2010–2023 vintages). Dollar year and price basis are
blank for these dimensionless observations. The formatter normalizes them to
its raw ATB CF reference before applying `cf_improvement: real`.

`fixed_om` rows come from PV's `O&M Cost Time Trend` (annual mean, AC basis)
and wind's `O&M Over Time` (mean by COD among projects reporting 2024 O&M).
Both use 2024 USD/kW-year and map to FOM only; wind's dollar year is assumed
because its sheet does not state one. Wind values average available
operating years within each project; PV values describe the operating fleet.
The $/MWh presentation is not a separate variable O&M observation.

CSP extraction selects only the 110-MW tower with a 2015 COD in `CSP CapEx`,
matched to [Crescent Dunes](https://solarpaces.nlr.gov/project/crescent-dunes-solar-energy-project).
Its 10-hour storage is used as the `csp2` proxy. The CSV retains the single
observed cost in 2024 USD/kW-AC; endpoint filling and configuration-ratio scaling
happen in the formatter. Other CSP projects are not pooled into this reference.

`eia860_2016.zip` through `eia860_2024.zip` are raw annual EIA-860 inventories.
The scraper reads `3_4_Energy_Storage_Y<year>.xlsx` directly from each archive,
selecting the `Operable` sheet and battery prime mover `BA`. It emits
`storage_duration` rows in hours for each year's installation cohort using
sum(MWh)/sum(MW). The 2015 cohort comes from the 2016 inventory. Notes record
valid-generator counts and MW coverage; missing or nonpositive energy/power
records are excluded from both sums. These inventory cohorts approximate, but
cannot identify, the cost-reporting sample behind the EIA average capital cost.

Battery total costs remain observed `capital_cost` rows. The formatter derives
power and energy cost components using ATB reference proportions and these
durations; the raw CSV does not claim observed component costs. Run the scraper
once to download the additional archives, or use `--no-download` to reuse them.

Download and extract them separately from the ATB inputs:

```bash
python ../scripts/scrape_historical_costs.py
```

Use `--only wind`, `--only solar`, `--only offshore`, or `--only eia` to limit
the sources. Use `--no-download` to rebuild the two CSVs from existing local
workbooks. The formatter uses only explicitly reviewed mappings under
`historical_cost_sources.reeds_mappings` in `config.yaml`. Currently, the LBNL
AC-based UPV and land-based-wind series and the NLR offshore fixed-bottom series
replace their respective `capcost` columns before the configured projection
boundary; other series remain review inputs.

Important distinctions retained in the CSV are:

- LBNL utility-PV costs are included on both AC and DC capacity bases.
- NLR offshore costs remain separate by geography (`Global`, `Europe and United
  States`, `Asia`). None is U.S.-only. Pipeline years after 2023 are excluded.
  The workbook states no units, but the report's Figure 31 axis reads
  `USD2023/kW` and its section 1.2.2 normalizes every cost to real 2023 USD by
  currency conversion followed by U.S. CPI inflation, so the rows are recorded
  as `dollar_year: 2023`, `price_basis: real`.
- EIA costs are nominal in the year of installation, and its categories follow
  EIA definitions rather than ATB technology definitions.

### Which EIA tables are used

Each EIA workbook holds roughly a dozen tables. `source_table` records which one
a row came from, because the same label means different things in different
tables:

| `source_table` | EIA table | Adds |
| --- | --- | --- |
| `major_energy_source` | Generators installed by major energy source | The broad fuel categories: gas, wind, solar, battery, biomass, hydro, geothermal, petroleum |
| `prime_mover` | Generators installed by prime mover | Equipment-level detail; the only table reporting fuel cells, and it isolates onshore wind and photovoltaics from their broader categories |
| `natural_gas_technology` | Natural gas generators installed by technology (and the 2013-only "by plant type") | Whole-plant combined cycle, rather than the cost split across its turbine halves |

The remaining tables break the same capacity down by Census region, state, plant
size, wind class, or PV panel type. None can become a national ReEDS series, so
they are skipped deliberately rather than by omission.

Some labels are intentionally left unmapped: `Steam turbine` in the prime-mover
table has an ambiguous fuel, `... (as part of combined cycle)` rows are one half
of a plant reported whole elsewhere, and `Internal combustion engine` in that
table spans both gas and oil units.

Coverage gaps are real absences, not extraction failures: EIA reports only
categories with installations in a given year, which is why biomass skips 2018
and geothermal appears only in 2013. The series also cannot start before 2013 —
`generatorcosts/archive/` has no editions before that year.

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
