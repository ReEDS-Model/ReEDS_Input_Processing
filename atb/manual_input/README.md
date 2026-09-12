# Manual configuration inputs

- `csp_cost_ratios_<year>.csv`: ReEDS CSP configuration multipliers relative to
  the 10-hour csp2 reference.
- `offshore_cost_multipliers_<year>.csv`: ReEDS fixed/floating configuration
  adjustments to ATB offshore costs.
- `coal_projection_overrides_<year>.csv`: retained ReEDS coal values for
  specified projection years (2024: 2033-2034).

Historical values are consolidated in `../historical/manual.csv`, sourced from
ReEDS ATB 2024. Review year-specific projection inputs when updating ATB.
