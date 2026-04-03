'''
Formats units for H2 costs going into ReEDS
Pipeline and compressor cost data comes from SERA model (provided by Paige Jadun)
Storage cost numbers derived from Papadias and Ahluwalia, 2021 (data compiled by Pieter Gagnon)
(https://www.sciencedirect.com/science/article/pii/S0360319921030834?via%3Dihub)

author: Brian Sergi

Notes on inputs:
- The “kg” in the storage capital costs is defined for the working gas due to the need for 'cushion gas'. Accordingly, a nominal 
  1 tonne cavern actually has more than 1 tonne stored in it but can only put out 1 tonne before reaching a minimum level.
- The capital costs provided here are based on the single, large cavern estimate. Additional economies-of-scale may 
  be available for multiple co-located large caverns but assuming one large cavern seemed more appropriate for ReEDS
- Storage costs are provided for a storage that can store multiple days of throughput; we currently assume up to 10 days in ReEDS.
- Pipeline and compressor cost estimates are costed for fairly high throughput (~3,000 tonnes per day),
  so these may not extend very well to low usage levels
- Currently no cost improvement assumed over time for H2 storage.


Inputs from Paige --> Outputs from this script

Storage capital cost:     $/kg              -->   $/tonne
Storage FOM cost:                                 $/(tonne*year)
Pipeline capital cost:    $/(kg/day)-km     -->   $/[(tonne/hour)*mile]
Pipeline FOM cost:        $/(kg/day)-km-yr  -->   $/[(tonne/hour)*mile*year]
Compressor Capital cost:  $/(kg/day)        -->   $/[(tonne/hour)]
Compressor FOM cost:      $/(kg/day)-yr     -->   $/[(tonne/hour)*year]

'''

import os 
import pandas as pd

print("Starting H2 cost processing")

# read in original costs
h2_costs = pd.read_csv(os.path.join('costs', 'h2-cost-raw-inputs-2023-05-05.csv'))
h2_costs_out = h2_costs.copy()

# assign technology type (storage, pipeline, compressor) to simplify conversions
gas_components = {'Gaseous H2 Underground Pipe Storage' : 'storage', 
                  'Gaseous H2 Salt Cavern Storage'      : 'storage', 
                  'Gaseous H2 Hard Rock Storage'        : 'storage',
                  'Gaseous H2 Pipeline Compressor'      : 'compressor',
                  'Gaseous H2 Pipeline'                 : 'pipeline'
                  }
for comp in gas_components:
  if comp not in h2_costs_out['tech'].unique():
      print(f"Caution: {comp} not found in raw data. Check spelling in gas_component mapping.")

h2_costs_out['tech_type'] = h2_costs_out['tech'].map(gas_components)

# for storage only keep $/kg capital costs (drop 'cost_cap_rate' which is cost $/(kg/day)
#h2_costs_out.drop(h2_costs_out[h2_costs_out['metric'] == 'cost_cap_rate'].index, inplace = True)

# convert storage capital costs from $/kg to $/tonne
h2_costs_out.loc[(h2_costs_out.tech_type == 'storage') & (h2_costs_out.metric == 'cost_cap'), 'value'] *= 1000
# update storage capital cost units
h2_costs_out.loc[(h2_costs_out.tech_type == 'storage') & (h2_costs_out.metric == 'cost_cap'), 'units'] = '$/tonne'

## this section converted storage FOM costs numbers from Paige, but is not needed for the cost numbers provided by Pieter
# convert storage FOM costs from $/(kg/day)-yr to $/(tonne*year)
# h2_costs_out.loc[(h2_costs_out.tech_type == 'storage') & (h2_costs_out.metric == 'fom'), 'value'] *= 1000
# SERA assumes storage costs for 10 day duration, so divide by 10 to get storage costs on a per tonne basis
# h2_costs_out.loc[(h2_costs_out.tech_type == 'storage') & (h2_costs_out.metric == 'fom'), 'value'] /= 10 
# update units
# h2_costs_out.loc[(h2_costs_out.tech_type == 'storage') & (h2_costs_out.metric == 'fom'), 'units'] = '$/(tonne*year)'

# convert non-storage capital costs and FOM costs from $/(kg/day) or $/[(kg/day)*km] to $/(tonne/hour) or $/[(tonne/hour)*km]
h2_costs_out.loc[((h2_costs_out.units.isin(['$/(kg/day)-km', '$/(kg/day)-km-yr', '$/(kg/day)', '$/(kg/day)-yr']) 
                  ) & (h2_costs_out.tech_type.isin(['compressor', 'pipeline'])
                  )), 'value'] *= (1000 * 24)

# also convert pipeline costs from $/[(tonne/hour)*km] to $/[(tonne/hour)*mile]
km_per_mile = 1.60934
h2_costs_out.loc[h2_costs_out.units.isin(['$/(kg/day)-km', '$/(kg/day)-km-yr']), 'value'] *= km_per_mile

# deflate to 2004 dollars 
deflator = pd.read_csv(os.path.join('costs', 'deflator.csv'))
deflator.columns = ['dollar_year','deflator']
h2_costs_out = pd.merge(h2_costs_out, deflator, on='dollar_year', how='left')
if h2_costs_out.deflator.isna().sum() > 0:
  raise Exception(f"Missing deflator values for {h2_costs_out.loc[h2_costs_out.deflator.isna(),'dollar_year'].unique()}")

h2_costs_out.loc[h2_costs_out.metric.isin(['cost_cap','fom']), 'value'] *= h2_costs_out.loc[h2_costs_out.metric.isin(['cost_cap','fom']), 'deflator']

# format h2 tech set and column names
h2_costs_out['*h2_stor_trans'] = h2_costs_out['tech'].map({'Gaseous H2 Pipeline'                : 'h2_pipeline', 
                                                           'Gaseous H2 Pipeline Compressor'     : 'h2_compressor',
                                                           'Gaseous H2 Salt Cavern Storage'     : 'h2_storage_saltcavern', 
                                                           'Gaseous H2 Hard Rock Storage'       : 'h2_storage_hardrock',
                                                           'Gaseous H2 Underground Pipe Storage': 'h2_storage_undergroundpipe'})

h2_costs_out.rename(columns={'metric':'parameter', 'year':'t'}, inplace=True)
h2_costs_out['parameter'] = h2_costs_out['parameter'].replace({'elec_efficiency':'electric_load'})

# save inputs for ReEDS
h2_costs_out = h2_costs_out.loc[h2_costs_out.parameter.isin(['cost_cap', 'fom', 'electric_load']), ['*h2_stor_trans', 't', 'parameter', 'value']] 

# round values to 3 decimal places
h2_costs_out['value'] = h2_costs_out['value'].round(3)

h2_costs_out.to_csv(os.path.join('costs', 'h2_transport_and_storage_costs.csv'), index=False)

print("H2 cost processing complete!")