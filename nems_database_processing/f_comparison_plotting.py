# -*- coding: utf-8 -*-
"""
Created on Fri Nov 07 14:31:00 2025

@author: apham
"""

import pandas as pd
import os
import sys
import matplotlib.pyplot as plt
import numpy as np
import altair as alt
alt.data_transformers.disable_max_rows()
from textwrap import wrap

figure_path = os.path.join('Outputs','Figures')

#%%

current_fleet_yr = int(sys.argv[1])
reeds_path = gdbnewname = sys.argv[2]
reeds_path = os.path.expanduser(reeds_path)
sys.path.append(reeds_path)

# For debugging
#current_fleet_yr=2025

gdboldname = 'ReEDS_generator_database_final_EIA-NEMS_2025_nov.csv'

#gdboldname = 'ReEDS_generator_database_final_EIA-NEMS_' + str(current_fleet_yr) + '.csv'
gdbfinalname = 'ReEDS_generator_database_final_EIA-NEMS.csv'

dfold = pd.read_csv(os.path.join('Inputs','Inheritance',gdboldname), low_memory=False)
dfnew = pd.read_csv(os.path.join('Outputs',gdbfinalname), low_memory=False)

hierarchy = pd.read_csv(os.path.join(reeds_path, 'inputs','hierarchy.csv'))

region = 'USA'

#################################
### Planned online comparison ###
#################################
online_data_new = dfnew.loc[dfnew['StartYear']>=2020]
online_data_old = dfold.loc[dfold['StartYear']>=2020]

# Graph old planned online:
max_y = 80000
sch_order = [2020,2021,2022,2023,2024,2025,2026,2027,2028,2029,2030,2031]
if current_fleet_yr < 2025:
    status_cat = ['upv', 'dupv', 'pvb', 'csp-ns','wind-ons', 'wind-ofs', 'nuclear', 'battery_li',
                'biopower', 'lfill-gas', 'gas-cc','gas-ct', 'o-g-s', 'hydED', 'hydEND','hydNPND','geohydro_allkm']
else:
    status_cat = ['upv', 'dupv','pvb_pv','csp-ns','pvb_battery','wind-ons', 'wind-ofs', 'nuclear', 'battery_li',
                'biopower', 'lfill-gas', 'gas-cc','gas-ct', 'o-g-s', 'hydED', 'hydEND', 'hydND', 'hydNPND', 'geohydro_allkm']
resource_order_idx = {
    resource: idx 
    for idx, resource in enumerate(status_cat[::-1]) # Reverse list to align colors with legend order
}        

online_data_old["idx"] = online_data_old["tech"].map(resource_order_idx)

if current_fleet_yr < 2025:
    chart = alt.Chart(online_data_old).mark_bar(size=30).encode(
        x=alt.X('StartYear:N', title=None, sort=sch_order),
        y=alt.Y('sum(cap):Q', axis=alt.Axis(grid=False, title='Capacity (MW)'), scale=alt.Scale(domain=[0, max_y]), sort=status_cat),
        color=alt.Color('tech', 
                        scale=alt.Scale(range=['gold','goldenrod','khaki','orange','skyblue','aqua','lightpink','darkseagreen',
                                               'saddlebrown','darkkhaki','grey','lightslategrey','black','aquamarine', 'cadetblue','lightblue','turquoise']),                      
                        sort=status_cat),
        order=alt.Order('idx')).configure_axis(titleFontSize=15, labelFontSize=15, grid=False
                    ).configure_legend(labelFontSize=15, titleFontSize=15).properties(width=200, height=350).properties(
        width=500,
        height=300,
        title='Planned Online Capacity (MW) - Current NEMS - ' + region
    )
else:
    chart = alt.Chart(online_data_old).mark_bar(size=30).encode(
        x=alt.X('StartYear:N', title=None, sort=sch_order),
        y=alt.Y('sum(summer_power_capacity_MW):Q', axis=alt.Axis(grid=False, title='Capacity (MW)'), scale=alt.Scale(domain=[0, max_y]), sort=status_cat),
        color=alt.Color('tech',
                        scale=alt.Scale(range=['gold','goldenrod','khaki','orange','yellowgreen','skyblue','aqua','lightpink','darkseagreen',
                                            'saddlebrown','darkkhaki','grey','lightslategrey','black','aquamarine', 'lightsteelblue','lightblue','turquoise']),                       
                        sort=status_cat),
        order=alt.Order('idx')).configure_axis(titleFontSize=15, labelFontSize=15, grid=False
                    ).configure_legend(labelFontSize=15, titleFontSize=15).properties(width=200, height=350).properties(
        width=500,
        height=300,
        title='Planned Online Capacity (MW) - Current NEMS - ' + region
    )

os.makedirs(figure_path, exist_ok=True)
chart.save(os.path.join(figure_path,'planned_online_current_NEMS.html'))

# Graph new planned online:
sch_order = [2020,2021,2022,2023,2024,2025,2026,2027,2028,2029,2030,2031]
status_cat = ['upv', 'dupv','pvb_pv','csp-ns','pvb_battery','wind-ons', 'wind-ofs', 'nuclear', 'battery_li',
              'biopower', 'lfill-gas', 'gas-cc','gas-ct', 'o-g-s', 'hydED', 'hydEND', 'hydND', 'hydNPND', 'geohydro_allkm']

resource_order_idx = {
    resource: idx 
    for idx, resource in enumerate(status_cat[::-1]) # Reverse list to align colors with legend order
}        

online_data_new["idx"] = online_data_new["tech"].map(resource_order_idx)

chart = alt.Chart(online_data_new).mark_bar(size=30).encode(
    x=alt.X('StartYear:N', title=None, sort=sch_order),
    y=alt.Y('sum(summer_power_capacity_MW):Q', axis=alt.Axis(grid=False, title='Capacity (MW)'), scale=alt.Scale(domain=[0, max_y]), sort=status_cat),
    color=alt.Color('tech', 
                    scale=alt.Scale(range=['gold','goldenrod','khaki','orange','yellowgreen','skyblue','aqua','lightpink','darkseagreen',
                                           'saddlebrown','darkkhaki','grey','lightslategrey','black','aquamarine', 'lightsteelblue','lightblue','turquoise']),
                    sort=status_cat),
    order=alt.Order('idx')).configure_axis(titleFontSize=15, labelFontSize=15, grid=False
                ).configure_legend(labelFontSize=15, titleFontSize=15).properties(width=200, height=350).properties(
    width=500,
    height=300,
    title='Planned Online Capacity (MW) - Updated NEMS - ' + region
)
chart.save(os.path.join('Outputs','Figures','planned_online_new_NEMS.html'))

# Graph difference in planned online
# Read old NEMS data:
if current_fleet_yr < 2025:
    online_data_old = online_data_old.rename(columns={'cap':'cap_old'})
    online_data_old.loc[online_data_old['tech'].str.contains("battery", na=False),'tech'] = 'battery'
else:
    online_data_old = online_data_old.rename(columns={'summer_power_capacity_MW':'cap_old'})

# Read new NEMS data:
online_data_new = online_data_new.rename(columns={'summer_power_capacity_MW':'cap_new'})
if current_fleet_yr < 2025:
    online_data_new.loc[online_data_new['tech'].str.contains("battery", na=False),'tech'] = 'battery'
    online_data_new.loc[online_data_new['tech'].str.contains("pvb", na=False),'tech'] = 'pvb'

online_data_old_gb = online_data_old.groupby(['tech','StartYear'])['cap_old'].sum().reset_index()
online_data_new_gb = online_data_new.groupby(['tech','StartYear'])['cap_new'].sum().reset_index()
online_data_compare = pd.merge(online_data_new_gb,online_data_old_gb,on=['tech','StartYear'],how='outer')

online_data_compare['cap_new'] = online_data_compare['cap_new'].fillna(0)
online_data_compare['cap_old'] = online_data_compare['cap_old'].fillna(0)
online_data_compare['cap_diff'] = online_data_compare['cap_new']-online_data_compare['cap_old']

sch_order = [2020,2021,2022,2023,2024,2025,2026,2027,2028,2029,2030,2031]
if current_fleet_yr < 2025:
    status_cat = ['upv', 'dupv','pvb','csp-ns','wind-ons', 'wind-ofs', 'nuclear', 'battery',
                  'biopower', 'lfill-gas', 'gas-cc','gas-ct', 'o-g-s', 'hydED', 'hydEND', 'hydND', 'hydNPND', 'geohydro_allkm']
else:
    status_cat = ['upv', 'dupv','pvb_pv','csp-ns','pvb_battery','wind-ons', 'wind-ofs', 'nuclear', 'battery_li',
                'biopower', 'lfill-gas', 'gas-cc','gas-ct', 'o-g-s', 'hydED', 'hydEND', 'hydND', 'hydNPND', 'geohydro_allkm']

resource_order_idx = {
    resource: idx 
    for idx, resource in enumerate(status_cat[::-1]) # Reverse list to align colors with legend order
}        

online_data_compare["idx"] = online_data_compare["tech"].map(resource_order_idx)

if current_fleet_yr < 2025:
    chart = alt.Chart(online_data_compare).mark_bar(size=30).encode(
        x=alt.X('StartYear:N', title=None, sort=sch_order),
        y=alt.Y('sum(cap_diff):Q', axis=alt.Axis(grid=False, title='Capacity (MW)'), 
                scale=alt.Scale(domain=[-40000, 60000]), 
                #scale=alt.Scale(domain=[0, 200]), 
                sort=status_cat),
        color=alt.Color('tech', 
                        scale=alt.Scale(range=['gold','goldenrod','khaki','orange','skyblue','aqua','lightpink','darkseagreen',
                                               'saddlebrown','darkkhaki','grey','lightslategrey','black','aquamarine', 'lightsteelblue','lightblue','turquoise']),
                        sort=status_cat),
        order=alt.Order('idx')).configure_axis(titleFontSize=15, labelFontSize=15, grid=False
                    ).configure_legend(labelFontSize=15, titleFontSize=15).properties(width=200, height=350).properties(
        width=500,
        height=300,
        title='Online Capacity Difference (Updated NEMS - Current NEMS) (MW)'
    )
else:
    chart = alt.Chart(online_data_compare).mark_bar(size=30).encode(
        x=alt.X('StartYear:N', title=None, sort=sch_order),
        y=alt.Y('sum(cap_diff):Q', axis=alt.Axis(grid=False, title='Capacity (MW)'), 
                scale=alt.Scale(domain=[-40000, 60000]), 
                #scale=alt.Scale(domain=[0, 200]), 
                sort=status_cat),
        color=alt.Color('tech', 
                        scale=alt.Scale(range=['gold','goldenrod','khaki','orange','yellowgreen','skyblue','aqua','lightpink','darkseagreen',
                                            'saddlebrown','darkkhaki','grey','lightslategrey','black','aquamarine', 'lightsteelblue','lightblue','turquoise']),
                        sort=status_cat),
        order=alt.Order('idx')).configure_axis(titleFontSize=15, labelFontSize=15, grid=False
                    ).configure_legend(labelFontSize=15, titleFontSize=15).properties(width=200, height=350).properties(
        width=500,
        height=300,
        title='Online Capacity Difference (Updated NEMS - Current NEMS) (MW)'
    )

chart.save(os.path.join('Outputs','Figures','planned_online_diff.html'))

#################################
### Planned retire comparison ###
#################################
retire_data_new = dfnew.loc[dfnew['RetireYear']<=2035]
retire_data_new = retire_data_new.loc[retire_data_new['RetireYear']>=2020]

retire_data_old = dfold.loc[dfold['RetireYear']<=2035]
retire_data_old = retire_data_old.loc[retire_data_old['RetireYear']>=2020]

# Graph new retired year
sch_order2 = [2020,2021,2022,2023,2024,2025,2026,2027,2028,2029,2030,2031,2032,2033,2034,2035]
status_cat = ['upv', 'dupv','pvb_pv','csp-ns','pvb_battery','wind-ons', 'nuclear', 'battery_li',
              'biopower', 'lfill-gas', 'gas-cc','gas-ct', 'o-g-s', 'coal-new','coalolduns','coaloldscr',
              'hydED', 'hydEND', 'hydND', 'hydNPND', 'pumped-hydro','geohydro_allkm']

resource_order_idx = {
    resource: idx 
    for idx, resource in enumerate(status_cat[::-1]) # Reverse list to align colors with legend order
}   

retire_data_new["idx"] = retire_data_new["tech"].map(resource_order_idx)

chart = alt.Chart(retire_data_new).mark_bar(size=30).encode(
    x=alt.X('RetireYear:N', title=None, sort=sch_order2),
    y=alt.Y('sum(summer_power_capacity_MW):Q', axis=alt.Axis(grid=False, title='Capacity (MW)'), 
            scale=alt.Scale(domain=[0, 26000])),
            #scale=alt.Scale(domain=[0,400])),
    color=alt.Color('tech', 
                    scale=alt.Scale(range=['gold','goldenrod','orange','yellowgreen','skyblue','lightpink','darkseagreen',
                                           'saddlebrown','darkkhaki','grey','lightslategrey','black', 'slategray','gainsboro','dimgrey',
                                           'aquamarine', 'lightsteelblue','lightblue','turquoise']),
                    sort=status_cat),
    order=alt.Order('idx')).configure_axis(titleFontSize=15, labelFontSize=15, grid=False
                ).configure_legend(labelFontSize=15, titleFontSize=15).properties(width=200, height=350).properties(
    width=500,
    height=300,
    title='Planned Retirement (MW) - Updated NEMS'
)
chart.save(os.path.join('Outputs','Figures','planned_retired_new_NEMS.html'))

# Graph old retired year
sch_order2 = [2020,2021,2022,2023,2024,2025,2026,2027,2028,2029,2030,2031,2032,2033,2034,2035]
if current_fleet_yr < 2025:
    status_cat = ['upv','dupv','pvb','csp-ns','pvb_battery','wind-ons', 'nuclear', 'battery_li',
                'biopower', 'lfill-gas', 'gas-cc','gas-ct', 'o-g-s', 'coal-new','coalolduns','coaloldscr',
                'hydED', 'hydEND', 'hydND', 'hydNPND', 'pumped-hydro','geohydro_allkm']
else:
    status_cat = ['upv', 'dupv','pvb_pv','csp-ns','pvb_battery','wind-ons', 'nuclear', 'battery_li',
              'biopower', 'lfill-gas', 'gas-cc','gas-ct', 'o-g-s', 'coal-new','coalolduns','coaloldscr',
              'hydED', 'hydEND', 'hydND', 'hydNPND', 'pumped-hydro','geohydro_allkm']
    
resource_order_idx = {
    resource: idx 
    for idx, resource in enumerate(status_cat[::-1]) # Reverse list to align colors with legend order
}   

retire_data_old["idx"] = retire_data_old["tech"].map(resource_order_idx)

if current_fleet_yr < 2025:
    chart = alt.Chart(retire_data_old).mark_bar(size=30).encode(
        x=alt.X('RetireYear:N', title=None, sort=sch_order2),
        y=alt.Y('sum(cap):Q', axis=alt.Axis(grid=False, title='Capacity (MW)'), 
                scale=alt.Scale(domain=[0, 26000])),
                #scale=alt.Scale(domain=[0, 400])),
        color=alt.Color('tech', 
                        scale=alt.Scale(range=['gold','goldenrod','khaki','orange','skyblue','lightpink','darkseagreen',
                                            'saddlebrown','darkkhaki','grey','lightslategrey','black','darkgrey','gainsboro','dimgrey',
                                            'aquamarine', 'lightsteelblue', 'paleturquoise','lightblue','turquoise']),
                        sort=status_cat),
        order=alt.Order('idx')).configure_axis(titleFontSize=15, labelFontSize=15, grid=False
                    ).configure_legend(labelFontSize=15, titleFontSize=15).properties(width=200, height=350).properties(
        width=500,
        height=300,
        title='Planned Retirement (MW) - Current NEMS'
    )
else:
    chart = alt.Chart(retire_data_old).mark_bar(size=30).encode(
        x=alt.X('RetireYear:N', title=None, sort=sch_order2),
        y=alt.Y('sum(summer_power_capacity_MW):Q', axis=alt.Axis(grid=False, title='Capacity (MW)'), 
                scale=alt.Scale(domain=[0, 26000])),
                #scale=alt.Scale(domain=[0, 400])),
        color=alt.Color('tech', 
                        scale=alt.Scale(range=['gold','goldenrod','orange','yellowgreen','skyblue','lightpink','darkseagreen',
                                           'saddlebrown','darkkhaki','grey','lightslategrey','black', 'slategray','gainsboro','dimgrey',
                                           'aquamarine', 'lightsteelblue','lightblue','turquoise']),
                        sort=status_cat),
        order=alt.Order('idx')).configure_axis(titleFontSize=15, labelFontSize=15, grid=False
                    ).configure_legend(labelFontSize=15, titleFontSize=15).properties(width=200, height=350).properties(
        width=500,
        height=300,
        title='Planned Retirement (MW) - Current NEMS'
    )
chart.save(os.path.join('Outputs','Figures','planned_retired_current_NEMS.html'))

# Graph difference in planned retirement
# Read old NEMS data:
retire_data_old = retire_data_old.loc[retire_data_old['RetireYear']>=2020]
retire_data_old = retire_data_old.loc[retire_data_old['RetireYear']<=2035]
if current_fleet_yr < 2025:
    retire_data_old.loc[retire_data_old['tech'].str.contains("battery", na=False),'tech'] = 'battery'
    retire_data_old = retire_data_old.rename(columns={'cap':'cap_old'})
else:
    retire_data_old = retire_data_old.rename(columns={'summer_power_capacity_MW':'cap_old'})

# Read new NEMS data:
retire_data_new = retire_data_new.loc[retire_data_new['RetireYear']>=2020]
retire_data_new = retire_data_new.loc[retire_data_new['RetireYear']<=2035]
retire_data_new = retire_data_new.rename(columns={'summer_power_capacity_MW':'cap_new'})

retire_data_old_gb = retire_data_old.groupby(['tech','RetireYear'])['cap_old'].sum().reset_index()
retire_data_new_gb = retire_data_new.groupby(['tech','RetireYear'])['cap_new'].sum().reset_index()
retire_data_compare = pd.merge(retire_data_new_gb,retire_data_old_gb,on=['tech','RetireYear'],how='outer')

retire_data_compare['cap_new'] = retire_data_compare['cap_new'].fillna(0)
retire_data_compare['cap_old'] = retire_data_compare['cap_old'].fillna(0)
retire_data_compare['cap_diff'] = retire_data_compare['cap_new']-retire_data_compare['cap_old']

sch_order = [2020,2021,2022,2023,2024,2025,2026,2027,2028,2029,2030,2031]
if current_fleet_yr < 2025:
    status_cat = ['upv', 'dupv','pvb','csp-ns','wind-ons', 'wind-ofs', 'nuclear', 'battery',
                'biopower', 'lfill-gas', 'gas-cc','gas-ct', 'o-g-s', 'coal-new','coalolduns','coaloldscr',
                'hydED', 'hydEND', 'hydND', 'hydNPND', 'pumped-hydro','geohydro_allkm']
else:
    status_cat = ['upv', 'dupv','csp-ns','pvb_battery','wind-ons', 'nuclear', 'battery_li',
                'biopower', 'lfill-gas', 'gas-cc','gas-ct', 'o-g-s', 'coal-new','coalolduns','coaloldscr',
                'hydED', 'hydEND', 'pumped-hydro','geohydro_allkm']


resource_order_idx = {
    resource: idx 
    for idx, resource in enumerate(status_cat[::-1]) # Reverse list to align colors with legend order
}        

retire_data_compare["idx"] = retire_data_compare["tech"].map(resource_order_idx)

if current_fleet_yr < 2025:
    chart = alt.Chart(retire_data_compare).mark_bar(size=30).encode(
        x=alt.X('RetireYear:N', title=None, sort=sch_order),
        y=alt.Y('sum(cap_diff):Q', axis=alt.Axis(grid=False, title='Capacity (MW)'), 
                #scale=alt.Scale(domain=[-9000, 9000]),
                scale=alt.Scale(domain=[-18000, 22000]),
                sort=status_cat),
        color=alt.Color('tech', 
                        scale=alt.Scale(range=['gold','goldenrod','khaki','orange','skyblue','lightpink','darkseagreen',
                                            'saddlebrown','darkkhaki','grey','lightslategrey','black','gainsboro','dimgrey','darkgrey',
                                            'aquamarine', 'lightsteelblue','lightblue','turquoise']),
                        sort=status_cat),
        order=alt.Order('idx')).configure_axis(titleFontSize=15, labelFontSize=15, grid=False
                    ).configure_legend(labelFontSize=15, titleFontSize=15).properties(width=200, height=350).properties(
        width=500,
        height=300,
        title='Retire Capacity Difference (Updated NEMS - Current NEMS) (MW)'
    )
else:
    chart = alt.Chart(retire_data_compare).mark_bar(size=30).encode(
        x=alt.X('RetireYear:N', title=None, sort=sch_order),
        y=alt.Y('sum(cap_diff):Q', axis=alt.Axis(grid=False, title='Capacity (MW)'), 
                #scale=alt.Scale(domain=[-9000, 9000]),
                scale=alt.Scale(domain=[-18000, 22000]),
                sort=status_cat),
        color=alt.Color('tech', 
                        scale=alt.Scale(range=['gold','goldenrod','orange','yellowgreen','skyblue','lightpink','darkseagreen',
                                            'saddlebrown','darkkhaki','grey','lightslategrey','black','gainsboro','dimgrey','darkgrey',
                                            'aquamarine', 'lightsteelblue','lightblue','turquoise']),                                       
                        sort=status_cat),
        order=alt.Order('idx')).configure_axis(titleFontSize=15, labelFontSize=15, grid=False
                    ).configure_legend(labelFontSize=15, titleFontSize=15).properties(width=200, height=350).properties(
        width=500,
        height=300,
        title='Retire Capacity Difference (Updated NEMS - Current NEMS) (MW)'
    )    

chart.save(os.path.join('Outputs','Figures','planned_retired_diff.html'))