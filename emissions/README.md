## Emissions share by county

The R script in this folder (egrid.R) is used to compute the share of 2022 emissions by county. These shares are used to downscale the emissions constraint when running sub-nationally.

The script takes in 2022 eGrid data and matches plants to counties using their lat/long and the county shapefile (located in the ReEDS repository).

Output from this script is used in ReEDS repo under `'inputs','emission_constraints','county_co2_share_egrid_2022.csv'`
