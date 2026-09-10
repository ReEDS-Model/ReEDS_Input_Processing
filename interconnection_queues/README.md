# Overview
This repo includes scripts and inputs to preprocess interconnection queues that are used to run ReEDS 2.0.

# Scripts
- Script is run in `process_interconnection_queues.py`
- This script takes original interconnection queue data file from LBNL:
    - Determine cumulative queues between 2 years at FIPS level by technology
    - First year (`t_1`) cumulative queues: `q_status = ‘active’` and `IA_status_clean = ‘IA Executed’`
    - Final year (`t_2`) cumulative queues: `q_status = ‘active’` regardless of `IA_status_clean` status
    - Cumulative values for all the years in between `t_1` and `t_2` are interpolated from these two years' values
    - To run the script, a filename of the most recent data version, version release year and `t_1` and `t_2` are required
![interconnection queue inputs](interconnection_queue_inputs.png)

# Input files and params to run process_interconnection_queues.py
All the input files to run the scripts are located in `inputs` folder, including original queue data from LBNL (most recently `LBNL_Ix_Queue_Data_File_thru2025.xlsx`), the supplemental resource-type file from LBNL (`queues_other_forNLR_2025.xlsx`, see below) and the county-to-state file (read from the ReEDS repo at `inputs/zones/county_state.csv`) to match ReEDS counties to appropriate bas. Point the script at your ReEDS checkout with the `REEDS_PATH` environment variable.

Note: starting with the 2025 data vintage, LBNL renamed several columns (`IA_status_*` &rarr; `IA_phase_*`, `type1`/`mw1` &rarr; `type_1`/`mw_1`) and folded the less-common resource types into the aggregated `Other`/`Other Storage` categories.

## Supplemental resource types (`queues_other_forNLR_2025.xlsx`)
`Pumped Storage` is folded into `Other Storage` and `Biofuel`/`Biomass` into `Other` in the public 2025 file, so the `pumped-hydro` and `biomass` tech groups would otherwise disappear from the output. LBNL sent us a supplement naming the detailed resource type for every *active* request in those categories, which `process_interconnection_queues.py` merges back in before anything else happens:
- Requests are matched on `q_id` + `entity`, since a `q_id` is only unique within an interconnecting entity
- Capacities always come from the public file; the supplement only relabels the resource type
- `compressed air`, `waste heat` and `wave` are in the supplement but have no ReEDS tech group, so they are still dropped
- The script prints how many requests it relabeled and lists any supplement record it could not match
- See the `add_detailed_types` docstring for how hybrid / co-located requests are matched

The script processes the 2025 vintage and requires both workbooks. The comparison plot also requires the previous vintage's saved CSV; no historical workbook processing is needed.

# Output
- Located in the `outputs` folder
- Final file that will be used to run ReEDS: `interconnection_queues.csv`
- The same data is saved as `interconnection_queues_2025.csv`, covering 2027-2031

# Figures
- The script generates `outputs/figures/queue_versions_2025.html` from the current output and `outputs/figures/compare_queue_versions.html` for 2025 minus 2024.
- Comparison vintages follow `version` (the release year): current data is `version-1`, and the previous CSV is `outputs/interconnection_queues_<version-2>.csv`. Year columns are read from that CSV.
- For a 2026 data update, update the workbook filenames, set `version=2027`, and update `t_1`/`t_2`. The comparison automatically becomes 2026 minus 2025, using the saved 2025 CSV.
- Years present in only one vintage are plotted against zero for the difference calculation; this does not mean the missing vintage imposed a zero-capacity limit.
