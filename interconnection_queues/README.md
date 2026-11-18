# Overview
This repo includes scripts and inputs to preprocess interconnection queues that are used to run ReEDS 2.0.

# Scripts
- Script is run in `process_interconnection_queues.py`
- This script takes original interconnection queue data file from LBNL:
    - Determine cumulative queues between 2 years at FIPS level by technology
    - First year (`t_1`) cumulative queues: `q_status = ‘active’` and `IA_status_clean = ‘IA Executed’`
    - Final year (`t_2`) cumulative queues: `q_status = ‘active’` regardless of `IA_status_clean` status
    - Cumulative values for all the years in between `t_1` and `t_2` are interpolated from these two years' values
    - To run the script, the input filenames, the data year (`version`) and `t_1` and `t_2` are set at the top of the script:

| Parameter | Current value | Description |
|---|---|---|
| `filename` | `LBNL_Ix_Queue_Data_File_thru2025.xlsx` | Most recent LBNL queue data file |
| `filename_other` | `queues_other_forNLR_2025.xlsx` | LBNL supplement with the detailed types behind `Other`/`Other Storage` |
| `version` | 2025 | Data year (matches the year in the LBNL filenames) |
| `t_1` | 2028 | First year to calculate queue |
| `t_2` | 2031 | Last year to calculate queue |

# Input files and params to run process_interconnection_queues.py
All the input files to run the scripts are located in `inputs` folder, including original queue data from LBNL (most recently `LBNL_Ix_Queue_Data_File_thru2025.xlsx`), the supplement `queues_other_forNLR_2025.xlsx` and `county_state.csv` (read from the `inputs/zones` folder of the ReEDS repo) to match ReEDS counties to appropriate bas. Set `REEDS_PATH` to your ReEDS checkout.

The 2025 supplement restores pumped-storage and biofuel/biomass types grouped under `Other Storage`/`Other` in the public file. Requests are matched on `q_id` + `entity` and resource category, preserving public-file capacities; see `add_detailed_types` for hybrid matching details.

# Output
- Located in the `outputs` folder
- Final file that will be used to run ReEDS: `interconnection_queues.csv`
- Previous version files are also kept there

# Comparison figures
- Interconnection queue figures are generated from `process_interconnection_queues.py` for the current vintage (`version`) and its difference from the previous vintage, using `outputs/interconnection_queues_<version-1>.csv`. Update the input filenames, data year `version` (the year in the LBNL filename), and `t_1`/`t_2` for a new vintage; comparison years are selected automatically.
- Years present in only one vintage are plotted against zero for the difference calculation; this does not mean the missing vintage imposed a zero-capacity limit.
