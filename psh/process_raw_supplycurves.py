# -*- coding: utf-8 -*-
'''
Process Raw Pumped-Storage Hydropower (PSH) Supply Curve Files

This script takes in raw pumped storage hydropower supply curve data received from Evan Rosenleib
and:
 - renames columns for consistency with other supply curve files (e.g. wind, solar)
 - converts values from $/W to $/MW 
 - plots the raw supply curve data in 2004$ for error checking

NOTE: The $/W to $/MW conversion may be removed for future data updates, depending on the
      units Evan supplies the data at - check with Evan prior to processing the files

INPUTS
------
    - psh_reeds_{8|10|12}hr_{limited|reference|open}.csv: raw PSH supply curve data 
            containing site-level capital costs and coordinates for both upper and lower 
            reservoir
            
OUTPUTS
-------
    - supplycurve_psh-{8|10|12}hr_{limited|reference|open}.csv: processed PSH supply curve
            data to be copy/pasted into the ReEDS repository

@author: jvcarag
@date: 20261005 10:00
'''
import os
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

deflate_2022_to_2004 = 0.645693617

inpath = 'data/raw_supplycurves/'
outpath = 'outputs/supplycurves/'
Path(outpath).mkdir(parents=True, exist_ok=True)

#%%===================================#
#   -- PROCESS RAW SUPPLY CURVES --   #
#=====================================#

DICT_SC = {
    # inpath filename          : outpath filename
    'psh_reeds_8hr_limited'    : 'supplycurve_psh-8hr_limited',
    'psh_reeds_8hr_reference'  : 'supplycurve_psh-8hr_reference',
    'psh_reeds_8hr_open'       : 'supplycurve_psh-8hr_open',
    'psh_reeds_10hr_limited'   : 'supplycurve_psh-10hr_limited',
    'psh_reeds_10hr_reference' : 'supplycurve_psh-10hr_reference',
    'psh_reeds_10hr_open'      : 'supplycurve_psh-10hr_open',
    'psh_reeds_12hr_limited'   : 'supplycurve_psh-12hr_limited',
    'psh_reeds_12hr_reference' : 'supplycurve_psh-12hr_reference',
    'psh_reeds_12hr_open'      : 'supplycurve_psh-12hr_open',
}

DICT_SC_OUT = {}

for filename_old in DICT_SC:
    print(f'Processing supply curve: {filename_old}.csv')
    dfin = pd.read_csv(os.path.join(inpath, filename_old + '.csv'))
    # Convert cost from 2022$/W to 2022$/MW
    df = dfin.rename(columns={'max_gen_power_mw':'capacity',
                              '2022_dollars_per_mw':'capital_adder_per_mw'})
    df['capital_adder_per_mw'] *= 1e6
    filename_new = DICT_SC[filename_old]
    print(f'Saving supply curve as file: {filename_new}.csv')
    df.to_csv(os.path.join(outpath, filename_new + '.csv'), index=False)
    DICT_SC_OUT[filename_new] = df.copy()


#%%==================================#
#   -- BUILD/PLOT SUPPLY CURVES --   #
#====================================#

# User Inputs
deflate_to_2004 = True
save_plot = True

# Assemble Supply Curves #
#------------------------#

DICT_DFPLOT = {}

for case_old in DICT_SC:
    case = DICT_SC[case_old]
    dfin = DICT_SC_OUT[case]
    df = dfin[['capacity','capital_adder_per_mw']].rename(columns={'capacity':'cap_mw','capital_adder_per_mw':'cost'}).copy()
    df = df.sort_values(by='cost')
    df['cumcap_gw'] = (df['cap_mw'] * 0.001).cumsum()
    if deflate_to_2004:
        df['cost'] *= deflate_2022_to_2004
    df['cost'] *= 0.001
    DICT_DFPLOT[case] = df.copy()

# Plot Supply Curves #
#--------------------#

DICT_STYLE = {
    # casename_from_DICT_DFPLOT: (linecolor,linestyle,alpha)
    'supplycurve_psh-8hr_limited': ('C1',':',1),  'supplycurve_psh-8hr_reference': ('C2',':',1),  'supplycurve_psh-8hr_open': ('C4',':',1),
    'supplycurve_psh-10hr_limited':('C1','-.',1), 'supplycurve_psh-10hr_reference':('C2','-.',1), 'supplycurve_psh-10hr_open':('C4','-.',1),
    'supplycurve_psh-12hr_limited':('C1','-',1),  'supplycurve_psh-12hr_reference':('C2','-',1),  'supplycurve_psh-12hr_open':('C4','-',1),
}

# Map Matplotlib C-series colors and linestyles to Plotly-compatible values
COLOR_MAP = {
    'C0': '#1f77b4', 'C1': '#ff7f0e', 'C2': '#2ca02c', 'C3': '#d62728', 'C4': '#9467bd',
    'C5': '#8c564b', 'C6': '#e377c2', 'C7': '#7f7f7f', 'C8': '#bcbd22', 'C9': '#17becf',
}
LINESTYLE_MAP = {
    '-': 'solid', '--': 'dash', ':': 'dot', '-.': 'dashdot',
}

fig = make_subplots(
    rows=2, cols=1,vertical_spacing=0.10,
    subplot_titles=('Zoomed Out', 'Zoomed In'),
)

for i, casename in enumerate(DICT_DFPLOT):
    dfplot = DICT_DFPLOT[casename]
    casename

    mpl_color, mpl_style, alpha = DICT_STYLE[casename]
    color = COLOR_MAP.get(mpl_color, mpl_color)
    dash = LINESTYLE_MAP.get(mpl_style, 'solid')

    # Upper plot: lines only
    fig.add_trace(
        go.Scatter(
            x=round(dfplot['cumcap_gw'],2),
            y=round(dfplot['cost'],2),
            mode='lines',
            name=casename.split('-')[-1],
            line=dict(color=color, width=3, dash=dash),
            opacity=alpha,
            legendgroup=casename.split('-')[-1],
            showlegend=False,
            customdata=dfplot.index,
            hovertemplate="<b>Row ID:</b> %{customdata}<br>Capacity: %{x} GW<br>Cost: %{y} $/kW<extra></extra>",
        ),
        row=1, col=1
    )

    # Lower plot: lines + markers
    fig.add_trace(
        go.Scatter(
            x=round(dfplot['cumcap_gw'],2),
            y=round(dfplot['cost'],2),
            mode='lines',
            # mode='lines+markers'
            name=casename.split('-')[-1],
            line=dict(color=color, width=3, dash=dash),
            # marker=dict(size=5, color=color),
            opacity=alpha,
            legendgroup=casename.split('-')[-1],
            showlegend=True,
            customdata=dfplot.index,
            hovertemplate="<b>Row ID:</b> %{customdata}<br>Capacity: %{x} GW<br>Cost: %{y} $/kW<extra></extra>",
        ),
        row=2, col=1
    )

# Axis labels and ranges to match original view
fig.update_xaxes(range=[0, 35000], row=1, col=1)
fig.update_yaxes(title_text='Total Capital Cost [2004$/kW]', range=[0, 6000], row=1, col=1)

fig.update_xaxes(title_text='Cumulative Capacity Potential [GW]', range=[0, 200], row=2, col=1)
fig.update_yaxes(title_text='Total Capital Cost [2004$/kW]', range=[0, 3000], row=2, col=1)

fig.update_layout(
    width=900,
    height=700,
    template='plotly_white',
    title=dict(text='New PSH Supply Curves - Raw Data, no Interconnection Cost', x=0.5, xanchor='center'),
    legend=dict(x=1.02, y=0, xanchor='left', yanchor='bottom'),
    margin=dict(l=60, r=180, t=80, b=60),
)

if save_plot:
    fig.write_html(os.path.join(outpath,f'line_PSH_SC_raw.html'))
    fig.write_image(os.path.join(outpath,f'line_PSH_SC_raw.png'))

fig.show()