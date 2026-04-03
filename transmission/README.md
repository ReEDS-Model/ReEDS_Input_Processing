## County_Transmission Summary
The 'transmission/county_transmission' directory contains the necessary input files and the code to run a cost-minimizing transmission expansion model on the NARIS 2024 county-county transmission file. 
With the baseline NARIS 2024 data, there are cases of year 1 builds of Gas-CC (or other tech) to make up for a supply demand shortfall at the county scale in locations where there is not a history of shortfalls.
This shortfall could be caused by a few things intereacting including the load downsampling to county scale from BA (relies on population) or transmission capacity/topology sampling. 
County load is particularly dependent on <100 kV transmission/sub-transmission to make up for the difference between demand and the county's generation capacity.

The outcome of this directory's code is a set of increased transmission capacities such as the one shown below(line color corresponding to capacity increase in MW). 
The majority of these lines increase the county-county with in a single BA, but some cross BA bounds. These will have flow constrained by ReEDS PR #1780.  
![transmission capacities](transmission_capacities.png)


## Technical details 

To adjust the transmission, we run a simplified transmission capacity expansion model (Transmission_LP_multiperiod_iter.py) which can a) increase existing transmission at the cost of transmission from the inputs file or b) build new transmission along paths defined in the cost of transmission file at a 10,000x cost penalty. This problem minimizes the total cost of new transmission such that:
- All load is served (with a definable PRM, 12% used) in 2024.
- Generation resources are limited to their availabilities (defined by input file, default is using main ReEDS defaults as of 08/2025)
- The model is run over all stress periods by default, but can be used for rep periods as well.

This example map above, the optimization was performed in 6 regions (for tractability): WECC, ERCOT, SPP, MISO+PJM_west, FRCC+SERTP, PJM_east+NYISO+ISONE. This regional sampling can retain a few of the edge cases in which counties lying on either side of these boundaries may have underrepresented transmission, but if a project requires these to be adjusted, the specific areas can be run through the optimization model again. 

An example output from this model is the following figure produced for WECC (using the parameters of the PR file). On the left is the existing main branch transmission (lines), county demand (county fill color), and generation capacity (nameplate, color of point fill). The color scale is the same for all three categories. On the right is the shortfall capacity (county fill), generation (nameplate in the point), and the transmission changes (lines- solid is an existing line which has been upgraded, dashed is a required new line). NOTE: the color scales on left and right are different.
![WECC_sample_map](wecc_sample_map.png)


## How to use
To run this code you must prepare an environment with:
- pyomo
- geopandas
- shapely

Additionally, Gurobi is necessary for the current formulation of the problem for both the speed and the not-quite-perfectly-linear structure.
Licenses for Gurobi are available on Kestrel using the .sh file included in this directory.

Due to the size limits on this repo, the zipped inputs folder must be uncompressed and those inputs moved to the same directory as the python file.
These inputs files are generated with CONUS scale stress periods defined because all tested counties must have the same hours to function. 
Inputs files are the load and availabilities for rep and stress periods.
For specific subregions, these files and their specific stress periods can be substituted in.
Initial_NARIS2024.csv is the pre-adjustment county transmission dataset prior to any county-county changes and provided just for reference and convenience of first time use.

### shell file
After the standard HPC/slurm details the following is specified: 

_module purge_  
_module load anaconda3_  
_module load gurobi_  
_conda activate [your environment name]_  
_cd [your directory for this repo and this sub-file]_

_python Transmission_LP_multiperiod_iter.py --reeds [your ReEDS directory] --reg CA.WA.OR --type st --hr_sample 4 --exportname NARIS_plus_

All command line arguments for this:
- '--reeds' : Any HPC based ReEDS directory
- '--reg', default = 'MT' : The period-delimited list of areas which are desired for the transmission expansion
- '--type', default = 'state' : What is the hierarchy of the above regions
- '--period_type', default= 'stress' : Use stress periods or rep periods
- '--hr_sample', type = int, default= 1 : sampling frequency ([::hr_sample] of the total list)
- '--yr', type = int, default= 2024 : historical year to use
- '--PRM', type = float, default= 12 : PRM percent to account for
- '--OS', type = float, default= 0 : Oversupply portion- 0% means no capacity over supply is considered, 1.0 would be full capacity must be able to flow out of the county
- '--importname',default = "Initial_NARIS2024" : initial csv name to build from
- '--exportname',default = "transmission_capacity_init_AC_county_NARISplus2024" : output name for the updated csv

