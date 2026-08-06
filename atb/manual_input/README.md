# manual_input/

Hand-maintained inputs that are **not** available from the ATB data and so must
be updated manually. Consumed by `generate_atb_files.py`.

- `csp_cost_ratios_<year>.csv` — CSP configuration cost ratios (csp1–csp4)
  relative to the base config (csp2). Derived from a separate ReEDS CSP
  thermal-storage sizing / SAM analysis, not from the ATB workbook.
- `historic_capacity_factors.csv` — historic capacity factors for upv, wind-ons,
  and wind-ofs used to backfill the early years of `cf_improvement`. Sourced
  from LBL wind reports and colleague-provided data.

When a new ATB year is released, review and update these files by hand.
