

# Generate a weights csv file to blur fuel weights across regions


## Main functionality:
`generate_weights.py` takes an input file that can be of varying resolution (e.g. county or default p-regions) and generates a weights file for each balancing area such that the fuel weights assigned to those balancing areas are not so abrupt and are more blurred at the edges.  It uses an exponential decay length of 150 km, which can also be modified by the user.  

### Input files:
`inputs/shapefiles/US_PCA/US_PCA.shp` is default (coarse) p-region resolution of areas to have the fuel prices blurred.

`inputs/shapefiles/US_COUNTY_2022/US_COUNTY_2022.shp` is county level resolution of areas to have the fuel prices blurred.

`inputs/cendivweights_original.csv` the original weights file used in ReEDS before this PR. 

`inputs/census_boundaries_gdf.pkl`  A dataframe containing census boundaries.  Produced from running these lines in ReEDS repo:  import reeds && census_boundaries_gdf = reeds.io.get_dfmap()['cendiv'] && census_boundaries_gdf.to_pickle('census_boundaries_gdf.pkl')

`inputs/country_boundaries_gdf.pkl` A dataframe containing country boundaries.  Produced from running these lines in ReEDS repo:  import reeds && country_boundaries_gdf = reeds.io.get_dfmap()['country'] && country_boundaries_gdf.to_pickle('country_boundaries_gdf.pkl')

`hierarchy.csv` The hierarchy file from the ReEDS repo v2025.3.0 that contains hierarchal information about the p-regions.

### Output files:
Coarser p-region resolution `outputs/coarse_p_region_weights_150kmExpDecay.csv` is the output weights file when using the input file `inputs/US_PCA.shp` 

Higher county resolution `outputs/county_weights_150kmExpDecay.csv` is the output weights file when using the input file`inputs/US_COUNTY_2022.shp` 

`figures/` gives figures of the weights files for each run, 

## Other documentation:
`cendiv_weights.pptx` has figures of the weights file, using different input spatial resolution and varying decay lengths.  

`Supplement_other_method_weighting.xlsx`  is a supplementary file, referenced in the `cendiv_weights.pptx` that explains another method of weighting we tried, but ended up not selecting.





