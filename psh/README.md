# PSH Input Processing
This subdirectory contains a collection of scripts for processing various ReEDS input files related to the model representation of pumped-storage hydropower (PSH) technologies. 

## PSH Supply Curve Processing

file: `process_raw_supplycurves.py`

PSH supply curves are updated annually and begins by receiving the raw files from Evan Rosenleib and pasting them into `psh/data/raw_supplycurves`. Each row in a given file corresponds to an idividual PSH site, and each file should contain the following columns:
 - {up|low}\_reservoir\_{latitude|longitude}: latitudinal/longitudinal coordinates for the upper and lower reservoir of the PSH site
 - max_gen_power_mw: the maximum power capacity potential of the PSH site
 - {dollar_year}_dollars_per_mw: capital cost per dollar of the PSH site in a given dollar year

To process the PSH supply curves:
 1. Confirm dollar year, capacity units, scenarios with Evan
 2. Paste raw supply curve data into `psh/data/raw_supplycurves`
 3. Update scenario names and procedures in `process_raw_supplycurves.py` for any new changes to the inputs or data requirements
 4. run `python process_raw_supplycurves.py`
 5. Check the plotted supply curves in `/psh/outputs/line_PSH_SC_raw.{png|html}` and the output files for any errors