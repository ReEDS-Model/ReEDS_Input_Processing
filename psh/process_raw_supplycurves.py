# -*- coding: utf-8 -*-
'''
Process Raw Pumped-Storage Hydropower (PSH) Supply Curve Files

This script takes in raw pumped storage hydropower supply curve data received from the 
NLR geospatial data science team. Primary point of contact as of Oct 2026 is Evan Rosenlieb.
Script proedures include:
 - renaming columns for consistency with other supply curve files (e.g. wind, solar)
 - converting values from $/W to $/MW 
 - plotting the raw supply curve data in desired dollar year for error checking

NOTE: The $/W to $/MW conversion may be removed for future data updates, depending on the
      units of the supplied data - check with data provider prior to processing the files

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

# User Inputs #
#-------------#
# Paths
inpath = 'data/raw_supplycurves/'
outpath = 'outputs/supplycurves/'
Path(outpath).mkdir(parents=True, exist_ok=True)
reeds_path = os.path.expanduser('~/github/ReEDS')

# Update dollaryear variables below as necessary/desired
dollaryear_data = 2022
dollaryear_plot = 2025
save_plot = True


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
longest_key = len(max(DICT_SC, key=len))
DICT_SC_OUT = {}

print('Processing supply curves:')
for filename_old in DICT_SC:
    file_old = filename_old + '.csv'
    dfin = pd.read_csv(os.path.join(inpath, file_old))
    # Rename headers and convert cost from 2022$/W to 2022$/MW 
    df = dfin.rename(columns={'max_gen_power_mw':'capacity',
                              '2022_dollars_per_mw':'capital_adder_per_mw'})
    df['capital_adder_per_mw'] *= 1e6
    filename_new = DICT_SC[filename_old]
    print(f' - {file_old:<{longest_key+4}} -> {filename_new}.csv')
    df.to_csv(os.path.join(outpath, filename_new + '.csv'), index=False)
    DICT_SC_OUT[filename_new] = df.copy()


#%%==================================#
#   -- BUILD/PLOT SUPPLY CURVES --   #
#====================================#

# Assemble Supply Curves #
#------------------------#

DICT_DFPLOT = {}
print_once = 0

for case_old in DICT_SC:
    case = DICT_SC[case_old]
    dfin = DICT_SC_OUT[case]
    df = dfin[['capacity','capital_adder_per_mw']].rename(columns={'capacity':'cap_mw','capital_adder_per_mw':'cost'}).copy()
    df = df.sort_values(by='cost')
    df['cumcap_gw'] = (df['cap_mw'] * 0.001).cumsum()
    if dollaryear_plot != dollaryear_data:
        if not print_once:
            print(f'Adjusting dollar year from {dollaryear_data}$ to {dollaryear_plot}$...')
            print_once += 1
        deflator = pd.read_csv(
            os.path.join(reeds_path,'inputs','financials','deflator.csv'),index_col='*Dollar.Year'
        ).squeeze()
        dollaryear_adj_factor = deflator[dollaryear_data] / deflator[dollaryear_plot]
        df['cost'] *= dollaryear_adj_factor
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

print('Plotting supply curves...')
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
## Upper Plot
fig.update_xaxes(range=[0, 35000], row=1, col=1)
fig.update_yaxes(title_text=f'Total Capital Cost [{dollaryear_plot}$/kW]', range=[0, 8000], row=1, col=1)
## Lower Plot
fig.update_xaxes(title_text='Cumulative Capacity Potential [GW]', range=[0, 200], row=2, col=1)
fig.update_yaxes(title_text=f'Total Capital Cost [{dollaryear_plot}$/kW]', range=[0, 3510], row=2, col=1)

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