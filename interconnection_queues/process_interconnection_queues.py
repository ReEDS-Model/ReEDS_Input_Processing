import os
import sys
import pandas as pd
from itertools import product
import altair as alt
reeds_path = os.environ.get('REEDS_PATH', os.path.expanduser('~/Documents/Github/ReEDS/ReEDS'))
sys.path.append(reeds_path)

'''
This script processes the raw LBNL's interconnection queues data (https://emp.lbl.gov/queues) to
apply capacity deployment limit in ReEDS. Specifically, it determines 2028 and 2031 cumulative queues
at FIPS level by technology:
- 2028 cumulative queues: q_status = "active" and IA_status_clean = "IA Executed"
- 2031 cumulative queues: q_status = "active" regardless of IA_status_clean status
- 2029-2030 cumulative values are interpolated from 2028 and 2031 values
- 2027 values are interpolated from 0 and 2028 values (half of 2028 values)
'''

dir = os.getcwd()

##################### INPUTS ######################
# Most updated version of interconnection queue
filename = 'LBNL_Ix_Queue_Data_File_thru2025.xlsx'
# LBNL supplement with the detailed types behind "Other"/"Other Storage"
filename_other = 'queues_other_forNLR_2025.xlsx'
version = 2026              # release year
t_1 = 2028                  # first year to calculate queue
t_2 = 2031                  # last year to calculate queue
year_range = list(range(t_1-1, t_2+1))
year_range_str = [str(x) for x in year_range]

###################################################

# Resource types LBNL folded into aggregated categories from the 2025 vintage on, mapped to the
# category each was folded into. Only types with a matching ReEDS tech group are listed.
folded2aggregated = {
    'pumped storage': 'Other Storage',
    'biofuel': 'Other',
    'biomass': 'Other',
}

def add_detailed_types(queue_data, filename_other, type_cols):
    """Relabel the aggregated LBNL resource types using the detailed types from LBNL's supplement.

    Hybrid (co-located) requests name their detailed types in a different order than the public
    file's type columns, so each is matched to the aggregated category it was folded into rather
    than by position. A request with the same aggregated category in two type columns is skipped,
    since there is no way to tell which slot the detailed type belongs to. Capacities always come
    from the public file.
    """
    other = pd.read_excel(os.path.join(dir,'inputs',filename_other))
    detailed_by_request = {}
    for row in other.itertuples(index=False):
        # Requests with a single resource type leave type1-3 blank and only name it in type_raw
        types = [t.strip() for t in (row.type1, row.type2, row.type3) if isinstance(t, str)]
        types = types or [str(row.type_raw).strip()]
        detailed = {folded2aggregated[t.lower()]: t for t in types if t.lower() in folded2aggregated}
        if detailed:
            detailed_by_request[(str(row.q_id).strip(), str(row.entity).strip())] = detailed

    # A q_id is only unique within an interconnecting entity, so match on the pair
    request = list(zip(queue_data['q_id'].astype(str).str.strip(),
                       queue_data['entity'].astype(str).str.strip()))
    aggregated = queue_data[type_cols].apply(lambda c: c.astype(str).str.strip())
    relabeled_requests = set()
    for col in type_cols:
        # Number of type columns sharing this row's category; >1 means the slot is ambiguous
        shared = aggregated.eq(aggregated[col], axis=0).sum(axis=1)
        relabeled = pd.Series(
            [detailed_by_request.get(q, {}).get(t) if n == 1 else None
             for q, t, n in zip(request, aggregated[col], shared)],
            index=queue_data.index)
        relabeled_requests.update(q for q, t in zip(request, relabeled) if t is not None)
        queue_data[col] = relabeled.fillna(queue_data[col])

    print('Relabeled ' + str(len(relabeled_requests)) + ' of the ' + str(len(detailed_by_request))
          + ' requests in ' + filename_other + ' that report an aggregated resource type')
    # Requests missing from the public file have no capacity or location, so they can't contribute
    missing = [q for q in detailed_by_request if q not in relabeled_requests]
    if missing:
        print('  No matching request in ' + filename + ' for: '
              + ', '.join(q_id + ' (' + entity + ')' for q_id, entity in missing))
    return queue_data

# Number of technology type (as specified in the queue data file)
type_no = 3
queue_data = pd.read_excel(os.path.join(dir,'inputs',filename), sheet_name='03. Complete Queue Data')
# The first row is empty, so remove it
queue_data.columns = queue_data.iloc[0]
queue_data = queue_data[1:]

queue_data = add_detailed_types(
    queue_data, filename_other, ['type_'+str(item+1) for item in range(type_no)])

# County-to-FIPS mapping from the ReEDS repo
county_state = pd.read_csv(os.path.join(reeds_path,'inputs','zones','county_state.csv'))
county_state['FIPS'] = 'p' + county_state['FIPS'].astype(str).str.zfill(5)

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
    queue_data_temp = queue_data[['q_status', 'county', 'state', 'fips_code', 'IA_phase_clean', 'type_'+str(item),'mw_'+str(item)]]
    queue_data_temp = queue_data_temp.rename(columns={'county': 'county_name', 'fips_code': 'FIPS',
                                                      'IA_phase_clean': 'IA_status_clean',
                                                      'type_'+str(item): 'tech','mw_'+str(item):'cap'+str(item)})

    # Capacities are read as objects because of the header offset, so cast them back to numbers
    queue_data_temp['cap'+str(item)] = pd.to_numeric(queue_data_temp['cap'+str(item)], errors='coerce')

    # Only consider queues that have active status
    queue_data_active_temp = queue_data_temp[queue_data_temp['q_status']=='active']

    # Assign initial queue year (in this case 2028) to queues with IA_status_clean = 'IA Executed' and regardless of
    # IA_status_clean to final queue year (in this case 2031)
    queue_data_active_temp['online_year'] = t_1
    queue_data_active_temp.loc[queue_data_active_temp['IA_status_clean']!='IA Executed','online_year'] = t_2
    
    # Remove the queues with negative capacity and clean up
    queue_data_active_temp.loc[queue_data_active_temp['cap'+str(item)]<0,'cap'+str(item)] = 0
    queue_data_active_temp['cap'+str(item)] = queue_data_active_temp['cap'+str(item)].fillna(0)
    queue_data_active_temp['type'] = 'type'+str(item)
    queue_data_active_temp = queue_data_active_temp.rename(columns={'cap'+str(item):'cap'})
    active_queue = pd.concat([active_queue, queue_data_active_temp], axis=0).reset_index(drop=True)
    
# Sum up the queue capacities by county, tech, and online year
fips_reported = 'p' + pd.to_numeric(active_queue['FIPS'], errors='coerce').map(
    lambda x: str(int(x)).zfill(5) if pd.notna(x) else '')
name2fips = county_state.set_index(county_state['county_name']+'|'+county_state['state'])['FIPS']
fips_byname = (active_queue['county_name'].str.lower()+'|'+active_queue['state']).map(name2fips)
active_queue['FIPS'] = fips_reported.where(fips_reported.isin(county_state['FIPS']), fips_byname)
active_queue_agg = active_queue.groupby(['FIPS','tech','online_year'])['cap'].sum().reset_index()
active_queue_county = county_state.merge(active_queue_agg, on='FIPS', how='inner')

# Assign 0 queue cap value to county-year pair with no value
unique_year_FIPS = pd.DataFrame(product(active_queue_county['FIPS'].unique(),[t_1,t_2]),columns=['FIPS','online_year'])
unique_year_tech = pd.DataFrame(product(active_queue_county['tech'].unique(),[t_1,t_2]),columns=['tech','online_year'])
unique_year_FIPS_tech =unique_year_FIPS.merge(unique_year_tech, on='online_year', how='outer')

active_queue_county = active_queue_county.merge(unique_year_FIPS_tech, on=['FIPS','tech','online_year'], how='outer')
active_queue_county = active_queue_county[['FIPS','tech','online_year','cap']]
active_queue_county['cap'] = active_queue_county['cap'].fillna(0)

# Sum queue capacity by year to get cumulative queue cap by year
active_queue_county[str(t_2)] = active_queue_county['cap'].where(active_queue_county['online_year']==t_2, 0)
active_queue_county[str(t_2)] = active_queue_county.groupby(['FIPS','tech'])[str(t_2)].transform("sum")

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
reeds_techset = ['battery', 'biomass', 'coal', 'gas', 'geothermal', 'h2', 'hydro',
                 'nuclear', 'pumped-hydro', 'pv', 'wind-ofs', 'wind-ons']
active_queue_county_filtered = active_queue_county[active_queue_county['tech'].isin(reeds_techset)]

active_queue_county_filtered = active_queue_county_filtered.rename(columns={'FIPS':'r', 'tech':'tg'})

# Add tech with no queue (cap limit = 0)
#active_queue_county_filtered = (active_queue_county_filtered.groupby('r', as_index=False, group_keys=False).
#                                apply(insert_rows).reset_index(drop=True))

active_queue_county_filtered = active_queue_county_filtered.merge(csp_queue, on=['r','tg'], how='outer').fillna(0)

##################### SAVE OUTPUTS ######################
active_queue_county_filtered.to_csv(os.path.join(dir,'outputs','interconnection_queues_'+str(version-1)+'.csv'),index=False)
active_queue_county_filtered.to_csv(os.path.join(dir,'outputs','interconnection_queues.csv'),index=False)
#########################################################


############### QUEUE PLOT #######################
queue_plot = pd.melt(active_queue_county_filtered, id_vars=['r','tg'], value_vars=year_range_str)
queue_plot = queue_plot.rename(columns={'variable':'year', 'value':'cap'})
queue_plot = queue_plot.groupby(['tg','year'])['cap'].sum().reset_index()

sch_order = year_range
status_cat = ['pv','csp','wind-ons', 'wind-ofs', 'nuclear', 'battery', 'pumped-hydro',
              'biomass', 'gas', 'coal','hydro', 'geothermal', 'h2']

resource_order_idx = {
    resource: idx 
    for idx, resource in enumerate(status_cat[::-1]) # Reverse list to align colors with legend order
}        

# Create "idx" column with integer values indicating order in stacked bar
queue_plot["idx"] = queue_plot["tg"].map(resource_order_idx)

chart = alt.Chart(queue_plot).mark_bar(size=30).encode(
    x=alt.X('year:N', title=None, sort=sch_order),
    y=alt.Y('sum(cap):Q', axis=alt.Axis(grid=False, title='Capacity (MW)'), scale=alt.Scale(domain=[0, 2200000]),
            sort=status_cat),
    color=alt.Color('tg', 
                    scale=alt.Scale(range=['gold','goldenrod','skyblue','aqua','lightpink','darkseagreen','aquamarine',
                                           'saddlebrown','grey','black','lightblue','violet','turquoise']),
                    sort=status_cat),
    order=alt.Order('idx')).configure_axis(titleFontSize=15, labelFontSize=15, grid=False
                ).configure_legend(labelFontSize=15, titleFontSize=15).properties(width=200, height=350).properties(
    width=500,
    height=300,
    title='Interconnection Queue Version ' + str(version-1)
)

chart.save(os.path.join(dir,'outputs','figures','queue_versions_'+str(version-1)+'.html'))