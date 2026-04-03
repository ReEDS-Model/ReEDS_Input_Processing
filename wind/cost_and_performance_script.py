import pandas as pd
import os
from pdb import set_trace as b

tech = 'ons-wind' #'ons-wind' or 'ofs-wind'
year = 2024 #This ATB year. Change this with each update (used for output filenames)
baseyear = 2035 #This is the year from which the CF_mult is calculated. It should correspond to the assumptions used in the reV data.

this_dir_path = os.path.dirname(os.path.realpath(__file__))

#Read in source data
df = pd.read_csv(f'{this_dir_path}/{tech}_ATB_raw.csv')
df_hist = pd.read_csv(f'{this_dir_path}/{tech}_cost-and-performance_historical.csv')

#Drop ATB columns, if they exist
df = df.drop(columns=['ATB sheet', 'ATB table', 'ATB row label'], errors='ignore')

#Melt the years
df = pd.melt(df, id_vars=['Turbine','Parameter','Case'], var_name='Year', value_name='Value')
df['Year'] = df['Year'].astype(int)

#pivot out 'Parameter' column
df = df.pivot_table(index=['Turbine','Case','Year'], columns='Parameter', values='Value').reset_index()

#Duplicate df_hist data for each unique Case in df and then concatenate
df_turb_case = df[['Case']].drop_duplicates()
df_turb_case['temp'] = 1
df_hist['temp'] = 1
df_hist = df_hist.merge(df_turb_case, how='left', on='temp').drop(columns=['temp'])
df = pd.concat([df, df_hist])

#Calculate CF_mult, the ratio of CFc to its value when Year=2030 and Case='moderate'
df_cf_moderate_baseyear = df[(df['Year']==baseyear) & (df['Case']=='moderate')][['Turbine','CFc']]
df_cf_moderate_baseyear = df_cf_moderate_baseyear.rename(columns={'CFc':'CF_base'})
df = df.merge(df_cf_moderate_baseyear, how='left', on='Turbine')
df['CF_mult'] = df['CFc'] / df['CF_base']

#Sort
df = df.sort_values(by=['Case','Turbine','Year'])

#Round
val_cols = ['CF_mult','Overnight Cap Cost $/kW','Fixed O&M $/(kW-yr)','Var O&M $/MWh','rsc_mult']
val_cols = [col for col in val_cols if col in df.columns]
df[val_cols] = df[val_cols].round(5)

#Loop through each Case and output ReEDS inputs
for case in df['Case'].unique():
    df_case = df[df['Case']==case]
    #Reduce to columns of interest
    df_case = df_case[['Turbine','Year'] + val_cols].copy()
    df_case.to_csv(f'{this_dir_path}/{tech}_ATB_{year}_{case}.csv', index=False)
