# Manual configuration inputs

- `csp_cost_ratios_<year>.csv`: ReEDS CSP configuration multipliers relative to
  the 10-hour csp2 reference.
- `offshore_cost_multipliers_<year>.csv`: ReEDS fixed/floating configuration
  adjustments to ATB offshore costs.
- `nuclear_historical_projects.csv`: TVA and MIT capital-cost inputs and source
  notes, prepared in `../historical/real.csv`.
- `offshore_historical_projects.csv`: completed US fixed-bottom projects (Block
  Island 2016, CVOW pilot 2020, South Fork 2024) with reported costs and source
  notes. Vineyard Wind 1 is omitted until it declares commercial operation.
  Both project files share one schema and are registered in
  `config.yaml` under `historical_cost_sources.project_files`.

Historical values are consolidated in `../historical/manual.csv`, sourced from
ReEDS ATB 2024. Review year-specific projection inputs when updating ATB.
