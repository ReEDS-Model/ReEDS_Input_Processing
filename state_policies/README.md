# Overview

This folder produces three CSV files that ReEDS consumes as state-level
renewable / clean energy policy inputs:

| Output | Description |
| --- | --- |
| `outputs/rps_fraction.csv` | Required RPS fraction of retail sales by state and year, plus voluntary RPS and Nova Scotia (NS) rows. Columns: `t, st, rps_all, rps_solar, rps_wind`. |
| `outputs/ces_fraction.csv` | Required CES fraction of retail sales by state and year. Columns: `*t, st, Value`. |
| `outputs/hydrofrac_policy.csv` | State-level fraction of existing hydro / non-RE generation that already counts toward each state's RPS_All and CES targets. Columns: `*st, RPS_All, CES`. |

Both `rps_fraction.csv` and `ces_fraction.csv` are produced as piecewise-linear
ramps between policy "change points", so the year-over-year trajectory is smooth
even when the underlying LBNL data jumps (for example when a state has a 2050
target with otherwise flat interim values). The un-interpolated values are
saved in `outputs/intermediate outputs/` for diagnostic purposes and are used
by the comparison plots described below.

# Running the script

From this folder:

```
python data_processing.py
```

The script reads everything from `inputs/` and writes the three CSVs above to
`outputs/`, plus diagnostic intermediates to `outputs/intermediate outputs/`.

# Input files

All located in `inputs/`:

| Input | Description |
| --- | --- |
| `RPS data for NREL_June 2026.xlsx` | Annual LBNL state RPS / CES dataset, provided by Galen Barbose. Source: https://emp.lbl.gov/projects/renewables-portfolio. The script reads three sheets: `Statewide Sales`, `RPS & CES Demand (GWh)`, and (currently from the previous-year file — see note below) `Non-RE Accounting`. |
| `RPS data for NREL_June 2025.xlsx` | Previous-year LBNL file. Retained because the June 2026 LBNL release removed the `Non-RE Accounting` sheet (only national totals remain there now). The script falls back to this 2025 file just for the state-level hydro / MSW / non-RE accounting numbers. The underlying values come from compliance reports and IRPs that change slowly, so this is a reasonable interim. Update this if/when LBNL re-releases a state-level version. |
| `nrel-green-power-data-v2023.xlsx` | NREL Green Power Data, used for the voluntary RPS row. Source: https://www.nrel.gov/analysis/green-power. |
| `RPS_nonUS.csv` | Non-US RPS data provided by the ReEDS team (currently only Nova Scotia, `NS`). |
| `hierarchy.csv` | Region hierarchy from a recent ReEDS Reference run, used to map BAs to states for the hydrofrac calculation. |
| `gen_ann.csv` | Annual generation by tech and BA from a recent ReEDS Reference run, used to compute hydro / non-RE shares for the hydrofrac calculation. |

# Annual update procedure

The LBNL state RPS / CES dataset is refreshed roughly once per year. To pick up
a new release:

1. Download the new LBNL RPS dataset from
   https://emp.lbl.gov/projects/renewables-portfolio and place it in
   `inputs/` (e.g. `RPS data for NREL_June 2026.xlsx`).
2. Open `data_processing.py` and update the parameters at the top of the file:
   - `filename` — point to the new file.
   - `Salessheetname`, `Salessheet_usecols`, `Salessheet_skiprows`, `Salessheet_nrows` — match the new sheet's name and header layout.
   - `RPSsheetname`, `RPSsheet_usecols`, `RPSsheet_skiprows`, `RPSsheet_nrows` — match the new sheet's name and header layout.
   - If LBNL restores a state-level `Non-RE Accounting` sheet, set `filename_hydrofrac = filename` and re-check `Hydrosheet_*` parameters.
   - Confirm `hydro_year` is still appropriate for the latest ReEDS Reference run.
3. (Optional, but recommended for PR review) Stage the previous run's outputs
   for comparison. From `state_policies/`:
   ```
   copy outputs\rps_fraction.csv      "old and new data comparison\old ReEDS input\rps_fraction0.csv"
   copy outputs\ces_fraction.csv      "old and new data comparison\old ReEDS input\ces_fraction0.csv"
   copy outputs\hydrofrac_policy.csv  "old and new data comparison\old ReEDS input\hydrofrac_policy0.csv"
   ```
4. Run the processing script:
   ```
   python data_processing.py
   ```
5. Generate before/after comparison plots:
   ```
   cd "old and new data comparison"
   python generate_comparison_plots.py
   ```
   This writes six PNGs to `old and new data comparison/plots/`:
   `rps_all_comparison.png`, `rps_solar_comparison.png`,
   `rps_wind_comparison.png`, `ces_fraction_comparison.png`,
   `hydrofrac_RPS_All_comparison.png`, and
   `hydrofrac_CES_comparison.png`. Attach these to the PR so reviewers can see
   what changed.

# Output files

Located in `outputs/`. These are the files that get copied into ReEDS:

* `rps_fraction.csv`
* `ces_fraction.csv`
* `hydrofrac_policy.csv`

Diagnostic / intermediate files (not used directly by ReEDS) live in
`outputs/intermediate outputs/`:

* `rps_fraction_intermediate.csv` — RPS fractions before piecewise interpolation.
* `ces_fraction_intermediate.csv` — CES fractions before piecewise interpolation.
