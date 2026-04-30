'''
Script to calculate scalars for dr shed supply curve cost and capacity 
And to populate supply curve 
- supply curve values are based on 2030 values 
- capital costs are taken from LBNL data: 
        \Demand_Response\Residential_shed\drpath_data\dr-shed_capcost_IEF_January_2025.csv
- capacity values are taken from dsgrid data:
         \Demand_Response\Residential_shed\dsgrid_data\table_pivot_shed.parquet

'''
#%%
import pandas as pd 
import numpy as np 
import os
from dsgrid2reeds import read_file

#%%
reedsdir = os.path.join("C:\\Users\\LSERPE\\Documents\\Repo\\main\\ReEDS")
inputs_dir = os.path.join(reedsdir,'inputs')
hierarchy = pd.read_csv(os.path.join(inputs_dir,'hierarchy.csv'))

#%%
### Capitol Cost Multipliers 

dr_shed_cap_cost = pd.read_csv('drpath_data\\dr-shed_capcost_IEF_January_2025.csv')

# Extract unique dr-shed types from the data
dr_shed_types = dr_shed_cap_cost['i'].unique()

# Create a dictionary to hold DataFrames for each dr-shed type
dr_shed_capmult_dict = {}

for dr_shed_type in dr_shed_types:
    dr_shed_capmult_dict[dr_shed_type] = pd.DataFrame()
    dr_shed_capmult_dict[dr_shed_type]['t'] = list(range(2010, 2051))

for st in dr_shed_cap_cost['st'].unique():
    # Set multiplier for 2010-2030 to 1
    mult_2010_2030 = [1] * 20

    for dr_shed_type in dr_shed_types:
        # Linear interpolation for 2030-2040
        val_2030 = dr_shed_cap_cost[(dr_shed_cap_cost['st'] == st) & 
                                    (dr_shed_cap_cost['t'] == 2030) & 
                                    (dr_shed_cap_cost['i'] == dr_shed_type)]['2020$/kW'].item()
        val_2040 = dr_shed_cap_cost[(dr_shed_cap_cost['st'] == st) & 
                                    (dr_shed_cap_cost['t'] == 2040) & 
                                    (dr_shed_cap_cost['i'] == dr_shed_type)]['2020$/kW'].item()
        x_fill = list(range(2030, 2041))
        mult_2031_2040 = (np.interp(x_fill, [2030, 2040], [val_2030, val_2040]) / val_2030).tolist()

        # Linear interpolation for 2040-2050
        val_2050 = dr_shed_cap_cost[(dr_shed_cap_cost['st'] == st) & 
                                    (dr_shed_cap_cost['t'] == 2050) & 
                                    (dr_shed_cap_cost['i'] == dr_shed_type)]['2020$/kW'].item()
        x_fill = list(range(2040, 2051))
        mult_2041_2050 = (np.interp(x_fill, [2040, 2050], [val_2040, val_2050]) / val_2030).tolist()[1:]

        # Combine multipliers
        state_bas = hierarchy[hierarchy['st'] == st]['ba'].tolist()
        for ba in state_bas:
            if ba not in dr_shed_capmult_dict[dr_shed_type]:
                dr_shed_capmult_dict[dr_shed_type][ba] = mult_2010_2030 + mult_2031_2040 + mult_2041_2050
        
        # Transpose so column names are year and regions are rows
        dr_shed_capmult_dict[dr_shed_type] = dr_shed_capmult_dict[dr_shed_type].set_index('t').T.reset_index()
        # Rename the index column to 'region'
        dr_shed_capmult_dict[dr_shed_type].rename(columns={'index': 'region'}, inplace=True)
        # Add the 'dr_shed_type' column as the first column
        dr_shed_capmult_dict[dr_shed_type].insert(0, 'tech', dr_shed_type)


# Combine all dr-shed types into a single DataFrame
dr_shed_capmult = pd.concat(dr_shed_capmult_dict.values())
dr_shed_capmult.to_csv('reeds_inputs\\dr-shed_capcost_IEF_January_2025.csv', index = False)


#%%
### Create cap cost file for inputs/supplycurve
dr_shed_cost_dict = {}

for dr_shed_type in dr_shed_types:
    dr_shed_cost_dict[dr_shed_type] = pd.DataFrame()
    dr_shed_cost_dict[dr_shed_type]['tech'] = [dr_shed_type]

    for st in dr_shed_cap_cost['st'].unique():
        state_bas = hierarchy[hierarchy['st'] == st]['ba'].tolist()
        val_2030 = dr_shed_cap_cost[(dr_shed_cap_cost['st'] == st) & 
                                    (dr_shed_cap_cost['t'] == 2030) & 
                                    (dr_shed_cap_cost['i'] == dr_shed_type)]['2020$/kW'].item()

        for ba in state_bas:
            dr_shed_cost_dict[dr_shed_type][ba] = val_2030

# Combine all dr-shed types into a single DataFrame
dr_shed_cost = pd.concat(dr_shed_cost_dict.values())

# Convert cost data from $/kW to $/MW
for col in dr_shed_cost.columns:
    if col != 'tech':
        dr_shed_cost[col] = dr_shed_cost[col] * 1000

#Write
dr_shed_cost.to_csv('reeds_inputs\\dr_shed_cost.csv', index = False)

#%%
### Create capacity file for inputs/supplycurve

# Use dsgrid data
df_pivot = pd.read_parquet("Z:\\FY25_Decarb_IEF\\end-use_data\\table_pivot_shed.parquet")

# dictionary to define shed types in dsgrid data and resource name in reeds 
dr_types = {
    'dr-shed_1': 'cooling_shed_capacity',
    'dr-shed_2': 'heating_shed_capacity'
}

# Filter for 2030 data. Capacity multipliers will be based on 2030 values
df_pivot_2030 = df_pivot[df_pivot['all_years'] == '2030']
# Create a dictionary to hold DataFrames for each dr-shed type
dr_shed_cap_dict = {}
for dr_shed_type in dr_shed_types:
    dr_shed_cap_dict[dr_shed_type] = pd.DataFrame()
    dr_shed_cap_dict[dr_shed_type]['tech'] = [dr_shed_type]

    for ba in df_pivot_2030['reeds_pca'].unique().tolist():
        val_2030 = df_pivot_2030[df_pivot_2030['reeds_pca'] == ba][dr_types[dr_shed_type]].max()
        dr_shed_cap_dict[dr_shed_type][ba] = val_2030

# Combine all dr-shed types into a single DataFrame
dr_shed_cap = pd.concat(dr_shed_cap_dict.values())

# Write
dr_shed_cap.to_csv('reeds_inputs\\dr_shed_cap.csv',index = False) 

#%%
### Capacity Multipliers 
# create multiplier for dr_shed_cap

# path the input processing repo 
reeds_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Read in dsgrid data that have been re-formatted into h5 files for reeds 
shed_hourly = read_file(os.path.join(reeds_path,'Residential_shed','reeds_inputs','dr_shed_hourly'))
shed_hourly['year'] = shed_hourly['year'].astype(int)

# Define year that scalars will be based on 
base_year = 2030
# Filter to base year 
max_base = shed_hourly.loc[shed_hourly['year'] == base_year].copy().drop('year', axis=1)

# Initialize an empty dictionary to hold multipliers for each dr-shed type
drshed_mult = {}
# Iterate over unique years in the data
for t in shed_hourly['year'].unique():
    # Filter the DataFrame for t
    shed_year = shed_hourly.loc[shed_hourly['year']==t].copy().drop('year', axis=1)
    
    # Iterate over all unique dr-shed types 
    for dr_type in dr_types:
        # Filter to the correct dr-shed type
        shed_filtered = shed_year.filter(like=dr_type)
        
        # Initialize the multiplier DataFrame for the dr-shed type if not already done
        if dr_type not in drshed_mult:
            drshed_mult[dr_type] = pd.DataFrame()
        
        # Calculate the multiplier for the current year in filtered data
        # Need value for each region 
        regions = [col.split('|')[-1] for col in shed_filtered.columns]
        multiplier_list = []
        for ba in regions:
            # Calculate the multiplier - divide by base year max for each ba
            multiplier = shed_filtered[f"{dr_type}|{ba}"].max() / max_base[f"{dr_type}|{ba}"].max()
            multiplier_list.append(multiplier)

        drshed_mult[dr_type]['r'] = regions
        drshed_mult[dr_type]['tech'] = dr_type
        drshed_mult[dr_type][t] = multiplier_list    

# Concatenate the multipliers for each dr-shed type
drshed_cap_mult_df = pd.concat(drshed_mult.values(), ignore_index=True)

# Format 
drshed_cap_mult_df = drshed_cap_mult_df[['tech','r'] + [col for col in drshed_cap_mult_df.columns if col not in ['tech','r']]]
# add columns for missing years
for t in range(2010, 2030):
    if t not in drshed_cap_mult_df.columns:
        drshed_cap_mult_df.loc[:, t] = 1

# Move early years to the front, keeping the rest of the columns in their original order
drshed_cap_mult_df = drshed_cap_mult_df[list(range(2010, 2051))  + [col for col in drshed_cap_mult_df.columns if col not in list(range(2010, 2051))]]
# Reorder the columns to have 'tech' and 'r' first
drshed_cap_mult_df = drshed_cap_mult_df[['tech', 'r'] + [col for col in drshed_cap_mult_df.columns if col not in ['tech', 'r']]]

# Save the multipliers to a CSV file
drshed_cap_mult_df.to_csv(os.path.join(reeds_path,'reeds_inputs','dr_shed_capacity_scalar_IEF_January_2025.csv'), index=False)

