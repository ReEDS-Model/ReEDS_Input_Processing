April 30th, Lara Bezerra and Kodi Obika
# Regional daily degree day calculations

# Files
- "heating_cooling.csv": Monthly degree days by state from https://ftp.cpc.ncep.noaa.gov/htdocs/products/analysis_monitoring/cdus/degree_days/archives/, aggregated by script "scraping_heating_cooling.py"

- "kdegday_census_divs.csv": Yearly degree days from https://github.com/EIAgov/NEMS/blob/main/input/bld/kdegday.txt

- "NationalProjections_ProjectedTotalPopulation_2030-2050.csv": Population forecast from https://www.coopercenter.org/research/how-accurate-are-our-50-state-population-projections

- "ngreg_cdd.csv" and "ngreg_hdd.csv": Yearly degree days from cdivs in "kdegday_census_divs" with estimated Mountain,California,Northwest,Southwest values (they get replaced by scaled numbers in "making_degree_days.py")

- "state_groups.csv": Mapping between census divisions and our gas regions as well as states

- "cooling_degree_days_noaa.csv" & "heating_degree_days_noaa.csv": Monthly heating and colling degree days on a state level from https://www.ncei.noaa.gov/pub/data/cirs/climdiv/ 

