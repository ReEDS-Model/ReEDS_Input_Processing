## Wind cost and capacity factor pre-processing

The `cost_and_performance_script.py` file creates the ReEDS inputs `ons-wind_ATB_{year}_moderate.csv`, `ons-wind_ATB_{year}_advanced.csv`, and `ons-wind_ATB_{year}_conservative.csv` (and similarly for `ofs-wind`) based on the latest ATB data and historical cost and performance from LBNL wind market reports.

### Steps for updating to latest ATB
  - Note that the following steps are for onshore wind. Offshore wind is similar, except cost and performance was provided by Gabe Zuckerman for one "base" fixed and floating plant (rather than taken from ATB 2024). Also, historical cost and performance for offshore wind is currently just set constant to the first year's value from Gabe.
  1. Update `ons-wind_ATB_raw.csv` with the new cost and performance data for the specified technology and class. E.g. for the 2024 update, I used data from https://data.openei.org/files/6006/2024%20v1%20Annual%20Technology%20Baseline%20Workbook%20Original%206-24-2024.xlsx (accessed from https://atb.nlr.gov/electricity/2024/data).
  1. If historical years were removed from `ons-wind_ATB_raw.csv`, add rows to `ons-wind_cost-and-performance_historical.csv` for those years, and fill in Overnight Capital Cost, Fixed O&M, and Var O&M with their prior values in `ons-wind_ATB_raw.csv` (before the update).
  1. Inflate all costs in `ons-wind_cost-and-performance_historical.csv` based on inflation between dollar year of last ATB and this ATB (including the costs in the new rows that were just added).
  1. Update capacity factors in `ons-wind_cost-and-performance_historical.csv` with latest LBNL land-based wind market report. E.g. for the 2024 update, I used data from https://www.energy.gov/sites/default/files/2023-08/land-based-wind-market-report-2023-edition-data_0.xlsx (accessed from https://www.energy.gov/eere/wind/articles/land-based-wind-market-report-2023-edition), "Capacity Factor in 2022 by COD" tab, "Generation-Weighted Average Capacity Factor" field (with 2010 as the weighted average of 2010 and all prior years).
  1. Update `tech` variable at top of `cost_and_performance_script.py` to `ons-wind` (if necessary) 
  1. Update `year` variable to this ATB year
  1. Update `baseyear` in the script if reV assumes a different year for cost and performance.
  1. Copy resulting `ons-wind_ATB...csv` files to ReEDS repo in the `inputs/plant_characteristics/` directory.
  1. Add new ATB dollar year to ReEDS repo in the `inputs/plant_characteristics/dollaryear.csv` file.

### Potential Improvements
  1. LBNL historical CF data is currently single-year based on latest year, rather than multi-year. We may want multi-year and/or CF based on the reV weather year(s) that we're using.
  1. We calibrate nationally but may want to calibrate regionally.
  1. We use ATB class 4 for all CF multiplier calculations, and may want to update this.
