# manual_input/

Inputs that are not available from the raw ATB downloads. They are consumed by
`generate_atb_files.py`.

- `historical/` — versioned ReEDS rows that precede each scraped ATB projection
  series. The formatter rolls the first current projection year into these
  files automatically after every run.
- `csp_cost_ratios_<year>.csv` — CSP configuration cost multipliers (csp1–csp4)
  relative to the base configuration (csp2). They come from the separate ReEDS
  CSP thermal-storage sizing/SAM analysis rather than either ATB download. The
  2024 multipliers reproduce the published ReEDS ATB 2024 configurations.
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
