# manual_input/

Inputs that are not available from the raw ATB downloads. They are consumed by
`generate_atb_files.py`.

- `historical/` — versioned ReEDS rows that precede each scraped ATB projection
  series. These come directly from the ReEDS repository, not from ATB. Their
  upstream sources are mixed and are not recorded per row: they have been
  maintained by hand in ReEDS and carried forward year by year, with each new
  ATB release adding one more year. Treat them as ReEDS's own record rather
  than a derived product, and update them in ReEDS rather than here.

  Two things live in these files. Every technology stores the years before its
  ATB data begins, which is why a technology ATB publishes late (nuclear from
  2030, fuel cell from 2035, floating offshore wind from 2030) stores more
  years than one starting at the release year. Separately, technologies ATB
  never publishes at all — cofired and existing coal, the aeroderivative
  combustion turbine — store their complete series here, so those rows are the
  only source for them.

  The formatter rolls the first current projection year into these files
  automatically after every run, so any run rewrites one row per series.
- `deflator.csv` — GDP deflator used to convert every stored or observed value
  into `atb.dollar_year`. Copied from `<reeds_repo>/inputs/financials/deflator.csv`
  so a run does not depend on a ReEDS checkout; update it by hand when ReEDS
  publishes a new dollar year.
- `csp_cost_ratios_<year>.csv` — CSP configuration cost multipliers (csp1–csp4)
  relative to the base configuration (csp2). They come from the separate ReEDS
  CSP thermal-storage sizing/SAM analysis rather than either ATB download. The
  2024 multipliers reproduce the published ReEDS ATB 2024 configurations.
  The csp1/csp3/csp4 rows under `historical/` are derived from csp2 with these
  ratios, so rebuild them whenever the ratios change or the history and the
  projection will disagree at the boundary.
- `offshore_cost_multipliers_<year>.csv` — ReEDS fixed-bottom and floating
  configuration adjustments applied to the corresponding ATB offshore class
  proxies. These account for ReEDS configuration assumptions outside the raw
  ATB flat file.
- `coal_projection_overrides_<year>.csv` — published ReEDS coal projection
  values retained where a later ATB errata release changed intermediate years.
  For 2024 this pins only 2033–2034; all other coal years come from ATB v3.
- `battery_costs_<year>.csv` — power/energy capital-cost split for an ATB year
  whose Excel workbook has not yet been published. It is a temporary fallback
  for `format_continuous_battery` and can be removed after publication.

When a new ATB year is released, review the year-specific CSP or battery inputs.
The files under `historical/` update automatically.
