# Prepared historical inputs

Commit these CSVs with the code. `run_pipeline.py` reads them without historical
raw files or network access. `historical_data_scraper.py` is their only writer.

| File | Contents |
| --- | --- |
| `real.csv` | All scraped observations (`scope=raw`), plus reviewed ReEDS mappings, calculated components, and annual fills (`scope=reeds`). |
| `atb.csv` | Reviewed ATB release-minus-two anchors, including CSP and offshore adjustments. Missing years are filled during formatting. |
| `manual.csv` | ReEDS ATB 2024 baseline curves, including reference/projection rows needed for retired designs and broadcasting. |

Each row identifies `technology`, `series`, `metric`, `year`, `value`, `unit`,
`dollar_year`, and `scenario`. `identifiers` retains ReEDS sub-technology keys
or raw source classifications. A `*` series applies to the family's single
modeled series. Monetary rows explicitly label their dollar year; dimensionless
values and durations leave it blank.

`source_type` is `real`, `calculated`, `filled`, `atb`, or `manual`.
`source_file` and `source_url` identify inputs; `sources` contains their SHA256
checksums, locations, original dollar years, and source notes. `source_years`
identifies the actual anchors behind a fill. `method` records interpolation,
endpoint filling, deflation, component splitting, or configuration scaling.
`atb_year` identifies the release where applicable.

The real table includes observations outside the modeled period and unmapped
technologies for review. Only `scope=reeds` rows enter outputs. Missing real
years are linearly interpolated internally and carried from the nearest
endpoint externally. When formatting applies a prepared table, years between two
anchors are interpolated, years before the first anchor carry it backward, and
years between the last anchor and the first projection year are interpolated
toward that projection value rather than held flat. These operations do not
create observed measurements.

Real battery components prefer the LBNL storage sample (2018-2024), with EIA
for other available years. LBNL's total system $/kWh is multiplied by
`sum(MWh)/sum(MW)` from the same sample before the ATB-ratio split. Costs
retain their original dollar year (2024 for LBNL) and mapped costs use 2022
dollars. Alternate cost statistics and reported energy-weighted durations
remain raw; the latter are not used for this conversion.

All seven CSP project costs and 171 annual plant CF observations are retained.
Crescent Dunes remains the mapped cost proxy; the other designs lack a reviewed
configuration mapping. CSP CF rows describe operating years, not build vintages,
and remain raw because current CSP outputs have no CF column.

ATB anchor rows always satisfy `year = atb_year - 2`; this relationship does not
apply to interpolated years in generated outputs. `manual.csv` is a ReEDS
snapshot, not a claim that every value was observed or published in that year.
Manual CF values retain their ATB 2024 normalization; real and ATB CF values
are fractions normalized to the current projection reference at runtime.

ATB 2022-2023 H-frame combined-cycle entries map to ReEDS `Gas-CC_H_2x1`
and, for 95% capture, `Gas-CC_H_2x1-CCS_mod`. Their workbook assumptions and
[2023 documentation](https://atb.nlr.gov/electricity/2023/fossil_energy_technologies)
specify 2x1 plants (992 MW without capture; 877 MW with 95% capture).
[ATB 2024](https://atb.nlr.gov/electricity/2024/fossil_energy_technologies)
introduced separate 1x1 plants. Those keep the configured fallback for earlier
years; 97% capture and retrofit entries are not substituted for new-build 95% CCS.

Mapping coverage was checked against the configured ATB 2015-2024 archives:

| ReEDS series | Historical ATB mapping |
| --- | --- |
| Coal-new, Coal-IGCC, Gas-CC, Gas-CT, Nuclear, biopower | Named conventional designs; available anchors start in 2013. |
| CofireOld, CofireNew | Same-named entries on the older Biopower sheets, mapped into ReEDS coal. |
| Coal and gas 95% CCS, H-frame 2x1, Nuclear-SMR | Matching designs start with ATB 2022 (2020 anchors). |
| H-frame 1x1 | Introduced in ATB 2024; no earlier matching anchors. |
| wind-ons | Class 4 from ATB 2020 onward, including the ATB 2022 name without a technology suffix. |
| wind-ofs | Fixed Class 1 and floating Class 8 from ATB 2020 onward. |
| upv | Class 4 from ATB 2021 onward. Earlier city/CF categories are not assumed equivalent. |
| csp1-4 | Ten-hour CSP reference from ATB 2016 onward, with the documented configuration ratios. |
| battery_li | Components, VOM, and efficiency from ATB 2020 onward. |
| ng-fuel-cell | Manual by configuration. |

The [2020 workbook](https://atb-archive.nlr.gov/electricity/2020/data.php)
supplies overnight costs and heat rates absent from its flat CSV. It also replaces
the flat CSV's slightly different CSP O&M estimates. Its battery components are
calculated as `E=(C4-C2)/2` and `P=C2-2*E`; FOM uses the ReEDS 2.5% convention.
The 2019 battery sheet has no values in its required 2017 column.

Wind mappings preserve resource classes with each vintage's turbine assumptions;
they do not equate historical turbines with the current 115 m/170 m design.
Earlier TRGs use different definitions ([land-based](https://atb.nlr.gov/electricity/2022/land-based_wind),
[offshore](https://atb-archive.nlr.gov/electricity/2020/index.php?t=ow)).
Old coal designs and aeroderivative gas turbines lack matching archive series.
Missing cells and parameters remain gaps; later projection years are never
relabeled as release-minus-two anchors.
