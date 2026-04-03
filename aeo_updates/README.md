# AEO (Annual Energy Outlook) Data Processing Module

This module processes data from the Annual Energy Outlook (AEO) published by the U.S. Energy Information Administration (EIA) to create demand projections, fuel price data, and other economic inputs for the ReEDS model.

## Key Scripts

### AEO_Load_Projections.py
This script creates the demand projection files for AEO scenarios. It uses historical retail sales and behind-the-meter PV generation data from EIA to calibrate historical state-level demand, and then carries that forward using demand ratios calculated from the AEO scenarios.

### AEO_scraper.ipynb
The scraper will grab the following data from EIA's API: 
* Electricity growth by region for high, low, and reference economic growth. 
* Natural gas prices by region for high, low, and reference oil and gas growth.
* Total natural gas use by region for high, low, and reference oil and gas resource
* Natural gas use for electricity by region for high, low, and reference oil and gas resource.
* Coal prices by region for the reference scenario.
* Uranium prices for the US for the reference scenario.

This can be run as is. You will want to adjust the default AEO year.
Sometimes the scenario names change from one year to the next, so if a scenario is not populated, its name has likely changed.

These results will be written out as csv files. 
Note that the write directory is not specified and hence the files will be output to the directory from where the file is run.
The output directory may be altered as required.

If more data sets are desired, search for the API url here: https://www.eia.gov/opendata/qb.php?category=371


### _eia_api_functions.py
Helper functions to retrieve and process data from the EIA API.

## Setup and Configuration

### EIA API Key Setup
Register for an API key here: [https://www.eia.gov/opendata/register.php](https://www.eia.gov/opendata/register.php)

Once you have obtained your api key, create a new environment variable to store your key: 

1. Add the following line to `~/.bash_profile` (or `/.bashrc` if you're on Windows): `export EIA_API_KEY='your unique api key from EIA'`
2. Run `source ~/.bash_profile` or (`source ~/.bashrc` if you're on Windows)
3. Verify the api key was set correctly with the following command: `echo $EIA_API_KEY`

### Input Data Changes When Updating AEO Data
#### Natural Gas Prices and Demand
Natural gas prices and demand can be pulled using the EIA AEO data grabber. The spreadsheet "NG Prices Preprocessing for AEO Inputs.xlsx" is used to calculate the alphas using the preset betas. You need to paste in the NG prices and NG electricity sector demand into the relevant tabs. Historical data needs to be updated to the current dollar year. The deflator to convert the alphas back to 2004$ also need to be updated. The alphas are then put into a csv file to be added to the inputs/fuelprices folder of the ReEDS model repo.

The prices and demand (both for the electricity sector and for all sectors) are also put into the relevant csv files in the inputs/fuelprices folder of the ReEDS model repo. Here are the NG input files that should be updated: 

* ng_tot_demand_AEO_{year}_HOG.csv
* ng_tot_demand_AEO_{year}_LOG.csv
* ng_tot_demand_AEO_{year}_reference.csv
* ng_demand_AEO_{year}_LOG.csv
* ng_demand_AEO_{year}_HOG.csv
* ng_demand_AEO_{year}_reference.csv
* ng_AEO_{year}_LOG.csv
* ng_AEO_{year}_HOG.csv
* ng_AEO_{year}_reference.csv
* alpha_AEO_{year}_LOG.csv
* alpha_AEO_{year}_HOG.csv
* alpha_AEO_{year}_reference.csv

#### Coal Prices
Pulled using the EIA data grabber. Coal data are input into the coal_AEO_{year}_reference.csv.

#### Uranium Prices
Pulled using the EIA data grabber. Uranium prices are input into uranium_AEO_{year}_reference.csv.

#### Demand Growth
Pulled using the EIA AEO data grabber. The demand growth data reports electricity consumption after rooftop PV has supplied a portion of the load, so we need to add the rooftop PV consumption back into this demand. Total rooftop PV consumption is in Table 17, "Renewable Energy Consumption by Sector and Source." We received regional (census division) numbers from EIA by asking Kevin Jarzomski (Kevin.Jarzomski@eia.gov), who sent us the file "AEO2025_bldgs_pv_gen_ref_high_low_economic_growth_2025-04-17.xlsx."[^1]
Electricity demand and rooftop PV consumption were put into the
"Electricity Demand Preprocessing for AEO Inputs.xlsx" spreadsheet,
"Adding DGPV to Demand" tab, and added together to create the demand
growth projections.

The growth numbers are ratios of the specified year to 2010, i.e.
Demand~year~ / Demand~2010~.

The projections through 2050 are created using AEO_Load_Projections.py,
which calibrates historical years to sales, and then carries the
census-division projection forward through 2050. It is set up to pull
the most recent EIA data using the EIA API.

#### Capital Costs
You can get the Table 123 data from table 55 at
[https://www.eia.gov/outlooks/aeo/tables_ref.php](https://www.eia.gov/outlooks/aeo/tables_ref.php).

Current capital costs, O&M costs, and heat rates are found in table 8.2. 
The table is found at the bottom of the AEO webpage under the "Documentation and Assumptions" section. 
There is a link called "Cost and performance characteristics" that takes you to table 8.2.

The capital costs in table 123 or table 55 are divided by the `AnnCostAdd` (annual cost adder), which is also sent by EIA upon request.


#### Dollar Year
Make sure to update the dollaryear.csv files otherwise the runs will
fail.

#### Syncing Up Historical Years
In order to not have deviations in historical or early years, we always
copy the historical data form the reference case into the high/low cases
of other scenarios. For example, the current year might be slightly
different in the high/low cases, but we'll overwrite those values using
historical values.

[^1]: If these numbers are not available regionally from EIA, then they
    can be estimated using the rooftop PV generation, which is available
    regionally (see Renewable Energy Generation by Fuel). The regional
    PV generation was mapped to the census regions using Mapping of EMM
    Regions to Census Regions.xlsx, and then the generation was used to
    scale the total rooftop PV consumption to the census regions. The
    rooftop PV consumption in quads had to be decremented before being
    added to the total demand. We determined the decremented amount
    using the ratio of the rooftop PV generation to the total
    electricity sector generation. See "Electricity Demand met by
    Rooftop PV - AEO 2016 - Not Used.xlsx" in
    \\\\nrelnas01\\ReEDS\\\_ReEDS Documentation\\AEO 2016 Update.