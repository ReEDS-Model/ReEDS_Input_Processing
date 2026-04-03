import os
import sys
import pandas as pd
from itertools import product
import altair as alt
reeds_path = os.path.expanduser('~/Documents/Github/ReEDS/ReEDS-2.0')
sys.path.append(reeds_path)

'''
This script processes the raw LBNL's interconnection queues data (https://emp.lbl.gov/queues) to
apply capacity deployment limit in ReEDS. Specifically, it determines 2026 and 2030 cumulative queues 
at FIPS level by technology:
- 2027 cumulative queues: q_status = "active" and IA_status_clean = "IA Executed"
- 2030 cumulative queues: q_status = "active" regardless of IA_status_clean status
- 2027-2029 cumulative values are interpolated from 2026 and 2030 values
- 2026 values are interpolated from 0 and 2027 values (half of 2027 values)
'''

dir = os.getcwd()

##################### INPUTS ######################
# Most updated version of interconnection queue
filename = 'lbnl_ix_queue_data_file_thru2024.xlsx'
version = 2025              # release year
t_1 = 2027                  # first year to calculate queue
t_2 = 2030                  # last year to calculate queue
year_range = list(range(t_1-1, t_2+1))
year_range_str = [str(x) for x in year_range]

# To compare two versions of interconnection queue
version_1 = 2024            # version 1 release year
version_2 = 2025            # version 2 release year

version_1_t_1 = 2026        # first year to calculate queue in version 1
version_1_t_2 = 2029        # last year to calculate queue in version 1
year_range_version_1 = list(range(version_1_t_1-1, version_1_t_2+1))
year_range_str_version_1 = [str(x) for x in year_range_version_1]
###################################################

# Number of technology type (as specified in the queue data file)
type_no = 3
if version < 2025:
    queue_data = pd.read_excel(os.path.join(dir,'inputs',filename), sheet_name='data')
else:
    queue_data = pd.read_excel(os.path.join(dir,'inputs',filename), sheet_name='03. Complete Queue Data')
    # In version 2025, the first row is empty, so remove it
    queue_data.columns = queue_data.iloc[0]
    queue_data = queue_data[1:]

county2zone = pd.read_csv(os.path.join(reeds_path,'inputs','county2zone.csv'))
county2zone['FIPS'] = 'p' + county2zone['FIPS'].astype(str).str.zfill(5)

# Assuming zero queue for csp
csp_queue = pd.read_csv(os.path.join(dir,'inputs','csp_queues.csv'))

active_queue = pd.DataFrame()

# Function to add zero value rows for tech with no queues
no_queue_tech = 'csp'
def insert_rows(group):
    first_row = group.iloc[[0]]
    first_row['tg'] = no_queue_tech
    for year in year_range:
        first_row[str(year)] = 0 
    return first_row.append(group)

# Iterate over the tech types (3)
for pt in list(range(type_no)):
    item = pt+1

    # Filter out tech type
    if version < 2025:
        queue_data_temp = queue_data[['q_status', 'county_'+str(item), 'state', 'IA_status_clean', 'type'+str(item),'mw'+str(item)]]
        queue_data_temp = queue_data_temp.rename(columns={'county_'+str(item): 'county_name', 'type'+str(item): 'tech','mw'+str(item):'cap'+str(item)})
    else:
        queue_data_temp = queue_data[['q_status', 'county', 'state', 'IA_status_clean', 'type'+str(item),'mw'+str(item)]]
        queue_data_temp = queue_data_temp.rename(columns={'county': 'county_name', 'type'+str(item): 'tech','mw'+str(item):'cap'+str(item)})

    # Only consider queues that have active status
    queue_data_active_temp = queue_data_temp[queue_data_temp['q_status']=='active']

    # Assign initial queue year (in this case 2027) to queues with IA_status_clean = 'IA Executed' and regardless of
    # IA_status_clean to final queue year (in this case 2030)
    queue_data_active_temp['online_year'] = t_1
    queue_data_active_temp.loc[queue_data_active_temp['IA_status_clean']!='IA Executed','online_year'] = t_2
    
    # Remove the queues with negative capacity and clean up
    queue_data_active_temp.loc[queue_data_active_temp['cap'+str(item)]<0,'cap'+str(item)] = 0
    queue_data_active_temp['cap'+str(item)] = queue_data_active_temp['cap'+str(item)].fillna(0)
    queue_data_active_temp['type'] = 'type'+str(item)
    queue_data_active_temp = queue_data_active_temp.rename(columns={'cap'+str(item):'cap'})
    active_queue = pd.concat([active_queue, queue_data_active_temp], axis=0).reset_index(drop=True)
    
# Sum up the queue capacities by county, state, tech, and online year
active_queue['county_name'] = active_queue['county_name'].str.lower()    
active_queue_agg = active_queue.groupby(['county_name', 'state','tech', 'online_year'])['cap'].sum().reset_index()

# Merge the queue data with county2zone to assign FIPS to each county and state pair and clean up
active_queue_county = county2zone.merge(active_queue_agg, on=['county_name','state'], how='outer')
active_queue_county = active_queue_county[active_queue_county['county_name']!= '0']
active_queue_county = active_queue_county.dropna(subset=['tech'])
active_queue_county = active_queue_county.dropna(subset=['FIPS'])

# Assign 0 queue cap value to county-year pair with no value
unique_year_FIPS = pd.DataFrame(product(active_queue_county['FIPS'].unique(),[t_1,t_2]),columns=['FIPS','online_year'])
unique_year_tech = pd.DataFrame(product(active_queue_county['tech'].unique(),[t_1,t_2]),columns=['tech','online_year'])
unique_year_FIPS_tech =unique_year_FIPS.merge(unique_year_tech, on='online_year', how='outer')

active_queue_county = active_queue_county.merge(unique_year_FIPS_tech, on=['FIPS','tech','online_year'], how='outer')
active_queue_county = active_queue_county[['FIPS','tech','online_year','cap']]
active_queue_county['cap'] = active_queue_county['cap'].fillna(0)

# Sum queue capacity by year to get cumulative queue cap by year
active_queue_county[str(t_2)] = active_queue_county['cap']
active_queue_county[str(t_2)] = active_queue_county.groupby(['FIPS','tech'])[str(t_2)].transform("max")

active_queue_county =active_queue_county.rename(columns={'cap': str(t_1)})
active_queue_county = active_queue_county[active_queue_county['online_year']==t_1]

# Interpolate queue capacity for years in between initial and final queue years
active_queue_county[str(t_2)] = active_queue_county[str(t_2)] + active_queue_county[str(t_1)]
active_queue_county = active_queue_county[['FIPS','tech',str(t_1),str(t_2)]]
active_queue_county['slope'] = (active_queue_county[str(t_2)] - active_queue_county[str(t_1)])/(t_2-t_1)
active_queue_county['intercept'] = active_queue_county[str(t_2)] - active_queue_county['slope']*t_2

for t in list(range(t_2-t_1)):
    if t_1 + t + 1 == t_2:
        continue
    active_queue_county[str(t_1+t+1)] = active_queue_county['slope']*(t_1+t+1) + active_queue_county['intercept']
active_queue_county[str(t_1-1)] = active_queue_county[str(t_1)]/2
active_queue_county = active_queue_county[['FIPS','tech',str(t_1-1),str(t_1),str(t_1+1),str(t_1+2),str(t_2)]]

### Final dataset to use in ReEDS
# Rename tech to match with ReEDS symbols
active_queue_county['tech'] = active_queue_county['tech'].apply(lambda x: x.lower() if isinstance(x, str) else x)
active_queue_county.loc[active_queue_county['tech']=='wind','tech'] = 'wind-ons'
active_queue_county.loc[active_queue_county['tech']=='offshore wind','tech'] = 'wind-ofs'
active_queue_county.loc[active_queue_county['tech']=='solar','tech'] = 'pv'
active_queue_county.loc[active_queue_county['tech']=='hydrogen','tech'] = 'h2'
active_queue_county.loc[active_queue_county['tech']=='pumped storage','tech'] = 'pumped-hydro'
active_queue_county.loc[active_queue_county['tech']=='biofuel','tech'] = 'biomass'
active_queue_county.loc[active_queue_county['tech']=='biogas','tech'] = 'biomass'

active_queue_county = active_queue_county.groupby(['FIPS','tech'])[year_range_str].sum().reset_index()

# Filter out tech to match with tg set in ReEDS
active_queue_county_filtered = active_queue_county[(active_queue_county['tech']=="battery") | (active_queue_county['tech']=="coal") 
                                                    | (active_queue_county['tech']=="gas") | (active_queue_county['tech']=="geothermal")
                                                    | (active_queue_county['tech']=="hydro") | (active_queue_county['tech']=="h2") 
                                                    | (active_queue_county['tech']=="nuclear") | (active_queue_county['tech']=="wind-ofs")
                                                    | (active_queue_county['tech']=="pv") | (active_queue_county['tech']=="wind-ons")
                                                    | (active_queue_county['tech']=="biomass") | (active_queue_county['tech']=="pumped-hydro")]

active_queue_county_filtered = active_queue_county_filtered.rename(columns={'FIPS':'r', 'tech':'tg'})

# Add tech with no queue (cap limit = 0)
#active_queue_county_filtered = (active_queue_county_filtered.groupby('r', as_index=False, group_keys=False).
#                                apply(insert_rows).reset_index(drop=True))

active_queue_county_filtered = active_queue_county_filtered.merge(csp_queue, on=['r','tg'], how='outer').fillna(0)

##################### SAVE OUTPUTS ######################
active_queue_county_filtered.to_csv(os.path.join(dir,'outputs','interconnection_queues_'+str(version-1)+'.csv'),index=False)
active_queue_county_filtered.to_csv(os.path.join(dir,'outputs','interconnection_queues.csv'),index=False)
#########################################################


############### COMPARISON PLOTS #######################
# Comparing two versions of interconnection queues
queue_1 = pd.read_csv(os.path.join(dir,'outputs','interconnection_queues_'+str(version_1-1)+'.csv'))
queue_2 = pd.read_csv(os.path.join(dir,'outputs','interconnection_queues_'+str(version_2-1)+'.csv'))

queue_1_temp = pd.melt(queue_1, id_vars=['r','tg'], value_vars=year_range_str_version_1)
queue_1_temp = queue_1_temp.rename(columns={'variable':'year', 'value':'cap_1'})
queue_1_temp = queue_1_temp.groupby(['tg','year'])['cap_1'].sum().reset_index()

queue_2_temp = pd.melt(queue_2, id_vars=['r','tg'], value_vars=year_range_str)
queue_2_temp = queue_2_temp.rename(columns={'variable':'year', 'value':'cap_2'})
queue_2_temp = queue_2_temp.groupby(['tg','year'])['cap_2'].sum().reset_index()

queue_compare = pd.merge(queue_1_temp,queue_2_temp,on=['tg','year'],how='outer')
queue_compare['cap_1'] = queue_compare['cap_1'].fillna(0)
queue_compare['cap_2'] = queue_compare['cap_2'].fillna(0)
queue_compare['cap_diff'] = queue_compare['cap_2'] - queue_compare['cap_1']

# Graph version 1:
sch_order = year_range
status_cat = ['pv','csp','wind-ons', 'wind-ofs', 'nuclear', 'battery', 'pumped-hydro',
              'biomass', 'gas', 'coal','hydro', 'geothermal', 'h2']

resource_order_idx = {
    resource: idx 
    for idx, resource in enumerate(status_cat[::-1]) # Reverse list to align colors with legend order
}        

# Create "idx" column with integer values indicating order in stacked bar
queue_compare["idx"] = queue_compare["tg"].map(resource_order_idx)

chart = alt.Chart(queue_compare).mark_bar(size=30).encode(
    x=alt.X('year:N', title=None, sort=sch_order),
    y=alt.Y('sum(cap_1):Q', axis=alt.Axis(grid=False, title='Capacity (MW)'), scale=alt.Scale(domain=[0, 2200000]), sort=status_cat),
    color=alt.Color('tg', 
                    scale=alt.Scale(range=['gold','goldenrod','skyblue','aqua','lightpink','darkseagreen','aquamarine',
                                           'saddlebrown','grey','black','lightblue','violet','turquoise']),
                    sort=status_cat),
    order=alt.Order('idx')).configure_axis(titleFontSize=15, labelFontSize=15, grid=False
                ).configure_legend(labelFontSize=15, titleFontSize=15).properties(width=200, height=350).properties(
    width=500,
    height=300,
    title='Interconnection Queue Version ' + str(version_1-1)
)

chart.save(os.path.join(dir,'outputs','figures','queue_versions_'+str(version_1-1)+'.html'))

# Graph version 2:
sch_order = year_range
status_cat = ['pv','csp','wind-ons', 'wind-ofs', 'nuclear', 'battery', 'pumped-hydro',
              'biomass', 'gas', 'coal','hydro', 'geothermal', 'h2']

resource_order_idx = {
    resource: idx 
    for idx, resource in enumerate(status_cat[::-1]) # Reverse list to align colors with legend order
}        

# Create "idx" column with integer values indicating order in stacked bar
queue_compare["idx"] = queue_compare["tg"].map(resource_order_idx)

chart = alt.Chart(queue_compare).mark_bar(size=30).encode(
    x=alt.X('year:N', title=None, sort=sch_order),
    y=alt.Y('sum(cap_2):Q', axis=alt.Axis(grid=False, title='Capacity (MW)'), scale=alt.Scale(domain=[0, 2200000]), 
            sort=status_cat),
    color=alt.Color('tg', 
                    scale=alt.Scale(range=['gold','goldenrod','skyblue','aqua','lightpink','darkseagreen','aquamarine',
                                           'saddlebrown','grey','black','lightblue','violet','turquoise']),
                    sort=status_cat),
    order=alt.Order('idx')).configure_axis(titleFontSize=15, labelFontSize=15, grid=False
                ).configure_legend(labelFontSize=15, titleFontSize=15).properties(width=200, height=350).properties(
    width=500,
    height=300,
    title='Interconnection Queue Version ' + str(version_2-1)
)

chart.save(os.path.join(dir,'outputs','figures','queue_versions_'+str(version_2-1)+'.html'))


# Graph difference in planned online capacity between old and new NEMS
sch_order = year_range
status_cat = ['pv','csp','wind-ons', 'wind-ofs', 'nuclear', 'battery', 'pumped-hydro',
              'biomass', 'gas', 'coal','hydro', 'geothermal', 'h2']

resource_order_idx = {
    resource: idx 
    for idx, resource in enumerate(status_cat[::-1]) # Reverse list to align colors with legend order
}        

# Create "idx" column with integer values indicating order in stacked bar
queue_compare["idx"] = queue_compare["tg"].map(resource_order_idx)

chart = alt.Chart(queue_compare).mark_bar(size=30).encode(
    x=alt.X('year:N', title=None, sort=sch_order),
    y=alt.Y('sum(cap_diff):Q', axis=alt.Axis(grid=False, title='Capacity (MW)'), #scale=alt.Scale(domain=[-40000, 60000]), 
            sort=status_cat),
    color=alt.Color('tg', 
                    scale=alt.Scale(range=['gold','goldenrod','skyblue','aqua','lightpink','darkseagreen','aquamarine',
                                           'saddlebrown','grey','black','lightblue','violet','turquoise']),
                    sort=status_cat),
    order=alt.Order('idx')).configure_axis(titleFontSize=15, labelFontSize=15, grid=False
                ).configure_legend(labelFontSize=15, titleFontSize=15).properties(width=200, height=350).properties(
    width=500,
    height=300,
    title='Interconnection Queue Difference (Version 2025 - Version 2024)'
)

chart.save(os.path.join(dir,'outputs','figures','compare_queue_versions.html'))