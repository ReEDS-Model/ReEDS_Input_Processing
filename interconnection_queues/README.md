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

The script processes the 2025 vintage and requires both workbooks; no historical queue inputs or outputs are needed.

# Output
- Located in the `outputs` folder
- Final file that will be used to run ReEDS: `interconnection_queues.csv`
- The same data is saved as `interconnection_queues_2025.csv`, covering 2027-2031

# Figure
- The script generates `outputs/figures/queue_versions_2025.html` from the current output. Historical and difference figures are no longer generated.
