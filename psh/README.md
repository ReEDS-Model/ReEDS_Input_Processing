# PSH Input Processing
This subdirectory contains a collection of scripts for processing various ReEDS input files related to the model representation of pumped-storage hydropower (PSH) technologies. 

## Existing PSH Power/Energy Capacity Processing

file: `calculate_existing_psh_capacities.py`

County-level operational and pump power capacity [MW] as well as energy capacity [MWh] are calculated by aggregating plant-level data sourced from the HydroSource team at Oak Ridge National Laboratory (ORNL). Details of this data are outlined in the [2021 U.S. Hydropower Market Report](https://www.energy.gov/sites/prod/files/2021/01/f82/us-hydropower-market-report-full-2021.pdf). The output file, `cap_existing_psh.csv`, is used in ReEDS to calculate the storage duration and pump efficiency of existing PSH capacity.

**To process existing PSH power/energy capacities:**
1. Validate the data in `psh/data/GESDB_Projects_complete RS_v3_fromORNL.xlsx`
    * Double-check plants listed in the "Summary" tab: ensure each plant is both existing and currently operational, and confirm any potential changes to operational generation/pump power capacity and energy capacity that might have resulted from recent changes to plant equipment or operations.
2. Update the `reeds_path` variable in `calculate_existing_psh_capacities.py` to point to the desired ReEDS repository
3. Run `python calculate_existing_psh_capacities.py`:
    * NOTE: Run `python calculate_existing_psh_capacities.py -c True` to automatically copy outputs to the ReEDS inputs folder
4. Check the output data in `psh/outputs/cap_existing_psh.csv`:
    * The operational generator capacity [MW], pump capacity [MW], and max energy capacity [MWh] of a given county should be equal to the sum of the capacity across of all plants in said county
    * All entries should be assigned the `init-1` vintage and `pumped-hydro` technology name

## PSH Supply Curve Processing

file: `process_raw_supplycurves.py`

PSH supply curves are updated annually, and the process begins by receiving the raw files from the NLR geospatial data science team and pasting them into `psh/data/raw_supplycurves`. Each row in a given file corresponds to an individual PSH site, and each file should contain the following columns:
 - `{up|low}\_reservoir\_{latitude|longitude}`: latitudinal/longitudinal coordinates for the upper and lower reservoir of the PSH site
 - `max_gen_power_mw`: the maximum power capacity potential of the PSH site
 - `{dollar_year}_dollars_per_mw`: capital cost per dollar of the PSH site in a given dollar year

**To process the PSH supply curves:**
1. Confirm dollar year, capacity units, scenarios
2. Paste raw supply curve data into `psh/data/raw_supplycurves`
3. Update scenario names and procedures in `process_raw_supplycurves.py` for any new changes to the inputs or data requirements
4. run `python process_raw_supplycurves.py`
5. Check the plotted supply curves in `/psh/outputs/line_PSH_SC_raw.{png|html}` and the output files for any errors. Common checks to look out for include:
    - Shorter storage durations should have lower costs on a $/kW basis
    - Total capacity available should be in the order `limited` < `reference` < `open`