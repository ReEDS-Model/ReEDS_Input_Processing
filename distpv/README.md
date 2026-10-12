# Updating dGen estimates in ReEDS

1. Rerun bokehpivot reporting on desired scenarios to get BA-level prices:
- Copy `ba_wholesale_prices.py` from this folder to `ReEDS/postprocessing/bokehpivot/reports/templates`.
- Populate `ReEDS/postprocessing/bokehpivot/reeds_scenarios.csv` with desired scenarios.
- Update and run `ReEDS/postprocessing/bokehpivot/run_report.py`
 up  (use `Bulk System Electricity Price by BA ($/MWh)` preset).
2. Run `reeds_to_dgen.py` to format prices for dGen and pass to dGen team.
- Review input assumptions for price smoothing.
3. On receiving dGen results, process for ReEds using `dgen_to_reeds.py`:
- Save raw dGen outputs to `raw_dgen_data` folder.
- Update `reedspath`, `dgen_scenarios`, and `hist_dgen_scen` in the script. 
- When run, the script will copy processed files to ReEDS.
4. Review inputs in ReEDS and conduct test runs before commiting results.