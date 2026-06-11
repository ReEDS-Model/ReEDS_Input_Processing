# Workflow
- `inspect_hub_locations.ipynb` identifies the gasreg (regions used in the regression) that each natural gas hub overlaps with geographically.
- `write_gasreg_regression_data.ipynb` writes the daily heating/cooling degree day and spot price data used in the regression. Creates `inputs/gasreg_regression_data.csv`.
- `calculate_regression_params.ipynb` performs the regression and exports the corresponding parameters (HDD/CDD coefficients, intercept, and monthly fixed effects). Creates the ReEDS input file `ReEDS/inputs/fuelprices/gasreg_degree_day_price_mult_regression_params.csv`.
- `calculate_gasreg_degree_days.ipynb` calculates and exports annual (2010-2050) HDD/CDD projections for each gasreg. Creates the ReEDS input file `ReEDS/inputs/fuelprices/gasreg_degree_days.csv`.

# Sources
- `inputs/kdegday.txt`: https://github.com/EIAgov/NEMS/blob/main/input/bld/kdegday.txt
- `inputs/NationalProjections_ProjectedTotalPopulation_2030-2050.csv`: https://www.coopercenter.org/national-population-projections
- `inputs/nst-est2020.xlsx`: https://www.census.gov/programs-surveys/popest/technical-documentation/research/evaluation-estimates/2020-evaluation-estimates/2010s-state-total.html
- `inputs/NST-EST2025-POP.xlsx`: https://www.census.gov/data/tables/time-series/demo/popest/2020s-state-total.html
