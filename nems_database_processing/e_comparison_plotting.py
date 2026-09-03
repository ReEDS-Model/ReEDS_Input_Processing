import pandas as pd
import itertools
import os
import sys
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from textwrap import wrap

debug_path = os.path.join('outputs','debug')
figure_path = os.path.join('outputs','figures')
os.makedirs(figure_path, exist_ok=True)

#%%

reeds_path = sys.argv[1]

# For debugging
# # local
#reeds_path = '~/Documents/GitHub/ReEDS/public_ReEDS/ReEDS/'
# # kestrel
#reeds_path = '//kfs2/projects/stdscen/apham/ReEDS/'                       

reeds_path = os.path.expanduser(reeds_path)                           
sys.path.append(reeds_path)
import reeds

# Read in NEMS capacity input files to compare
gdbfinalname = 'ReEDS_generator_database_final_EIA-NEMS.csv'

dfold = pd.read_csv(os.path.join(reeds_path,'inputs','capacity_exogenous',gdbfinalname), low_memory=False)
dfnew = pd.read_csv(os.path.join('outputs',gdbfinalname), low_memory=False)

# Read county2zone
county2zone = reeds.io.get_county2zone(GSw_ZoneSet='z90', as_map=False)
county2zone['FIPS'] = 'p' + county2zone.FIPS
county2zone = county2zone[['FIPS','r']]

###################### MAIN SWITCHES ######################
startyear = 2010
finalyear_retire = 2050
finalyear_online = max(dfnew['StartYear'].max(),dfold['StartYear'].max())
###########################################################

# Define techs and tech colors
techs = ['battery_li','pvb_battery','pumped-hydro','upv','dupv','pvb_pv','csp-ns','wind-ons','wind-ofs',
         'biopower','lfill-gas','geohydro_allkm','hydEND','hydED','hydNPND','hydND',
         'gas-cc','gas-ct','o-g-s','coaloldscr','coalolduns','coal-igcc','coal-new','nuclear']

color_techs = {'battery_li':'#FF4A88','pvb_battery':"#A75F8A",'pumped-hydro':"#C630B2",
               'upv':'#FFC903','dupv':'#FEE603','pvb_pv':"#A27C12",'csp-ns':"#F3660E",
               'wind-ons':'#00B6EF','wind-ofs':'#106BA7',
               'biopower':'#5B9844','lfill-gas':"#3B692A",
               'geohydro_allkm':'#A96235',
               'hydEND':'#187F94','hydED':"#37A5BB",'hydNPND':"#31D6E2",'hydND':"#5FA6A8",
               'gas-cc':'#52216B','gas-ct':'#C2A1DB','o-g-s':"#765189",
               'coaloldscr':'#222222','coalolduns':"#3E3C3C",'coal-igcc':"#5B5A5A",'coal-new':"#A19E9E",
               'nuclear':'#820000'}

def comparison_plotting_r(df, zones, finalyear,  techs, color_techs, figname, x, title):
    ncols = 10
    nrows = 9

    wspace = 0.15
    hspace = 0.35
    figsize = (12,5)
    markersize_plot = 2
    handletextpad = 0.1

    regions_nxn = np.array(zones).reshape(nrows, ncols)
    year_set = sorted(range(startyear,finalyear+1))

    ### Plot:
    fig, axes = plt.subplots(nrows,ncols, figsize=figsize, sharex=True)
    fig.subplots_adjust(wspace=wspace, hspace=hspace)
        
    for i in list(range(nrows)):
        for j in list(range(ncols)):
            ax = axes[i,j]
            region = regions_nxn[i][j]
            print(region)
            df_cap_temp = df.copy()
            df_cap_temp = df_cap_temp[df_cap_temp['r']==region].reset_index().drop(columns=['r','index'])

            # tech and year combination
            year_tech_combos = list(itertools.product(techs, year_set))
            df_cap = pd.DataFrame(year_tech_combos, columns=['tech',x])
            df_cap['summer_power_capacity_GW'] = 0.0

            for row in list(range(len(df_cap_temp))):
                tech = df_cap_temp['tech'][row]
                year = df_cap_temp[x][row]
                df_cap.loc[(df_cap['tech']==tech) & 
                           (df_cap[x]==year),
                           'summer_power_capacity_GW'] = df_cap_temp['summer_power_capacity_GW'][row]

            if (df_cap['summer_power_capacity_GW'] < 0).any():
                df_cap_pos = df_cap[df_cap['summer_power_capacity_GW']>=0]
                df_cap_neg = df_cap[df_cap['summer_power_capacity_GW']<=0]

                sns.histplot(data=df_cap_pos, x=x, hue="tech", multiple="stack",
                                            weights='summer_power_capacity_GW',
                                            hue_order=techs,palette=color_techs, binwidth=0.7, shrink=1,
                                            edgecolor=None, legend=False, discrete=True, ax = ax)
                sns.histplot(data=df_cap_neg, x=x, hue="tech", multiple="stack",
                                                            weights='summer_power_capacity_GW',
                                                            hue_order=techs,palette=color_techs, binwidth=0.7, shrink=1,
                                                            edgecolor=None, legend=False, discrete=True, ax = ax)
            else:
                sns.histplot(data=df_cap, x=x, hue="tech", multiple="stack",
                            weights='summer_power_capacity_GW',
                            hue_order=techs,palette=color_techs, binwidth=0.7, shrink=1,
                            edgecolor=None, legend=False, discrete=True, ax = ax)

            ax.set_title(region,fontsize=5,fontweight='bold',fontname="Arial",pad=-6)

            # Only display a few years in x-axis
            # Define the exact tick marks you want to show
            display_years = [2010, 2015, 2020, 2025, 2035, finalyear]

            # Set both locations and labels simultaneously
            ax.set_xticks(display_years)
            # ax.set_xticks(year_set)
            ax.set_xlabel('')
            #ax.set_xticklabels([''])
            #minor_locator = AutoMinorLocator(2)
            #ax.yaxis.set_minor_locator(minor_locator)

            ax.grid(color='lightgray', linestyle='dashed', linewidth=0.2, axis='y',zorder=0)
            ax.set_axisbelow(True)
            ax.tick_params(axis='both', which='major', width=0.5, length=1.5, pad=1)
            ax.tick_params(axis='both', which='minor', width=0.3, length=1, pad=1)
            ax.tick_params(labelsize=3)
            ax.yaxis.get_offset_text().set_fontsize(3)
            ax.tick_params(axis='x', labelrotation=90)
            ax.tick_params(right=False, labelright=False)
            ax.tick_params(top=False, labeltop=False)

            if [i,j]==[4,0]:
                ax.set_ylabel(title, fontsize=8,fontweight='bold',
                              fontname="Arial", labelpad=1)
            else:
                ax.set_ylabel('')
            
            if [i,j] == [5,9]:
                ax.plot([], c='#FF4A88', marker='s', markersize=markersize_plot, linestyle='', label='battery_li')
                ax.plot([], c='#A75F8A', marker='s', markersize=markersize_plot, linestyle='', label='pvb_battery')
                ax.plot([], c='#C630B2', marker='s', markersize=markersize_plot, linestyle='', label='pumped-hydro')
                ax.plot([], c='#FFC903', marker='s', markersize=markersize_plot, linestyle='', label='upv')
                ax.plot([], c='#FEE603', marker='s', markersize=markersize_plot, linestyle='', label='dupv')
                ax.plot([], c='#A27C12', marker='s', markersize=markersize_plot, linestyle='', label='pvb_pv')
                ax.plot([], c='#F3660E', marker='s', markersize=markersize_plot, linestyle='', label='csp-ns')
                ax.plot([], c='#00B6EF', marker='s', markersize=markersize_plot, linestyle='', label='wind-ons')
                ax.plot([], c='#106BA7', marker='s', markersize=markersize_plot, linestyle='', label='wind-ofs')
                ax.plot([], c='#5B9844', marker='s', markersize=markersize_plot, linestyle='', label='biopower')
                ax.plot([], c='#3B692A', marker='s', markersize=markersize_plot, linestyle='', label='lfill-gas')
                ax.plot([], c='#187F94', marker='s', markersize=markersize_plot, linestyle='', label='hydEND')
                ax.plot([], c='#37A5BB', marker='s', markersize=markersize_plot, linestyle='', label='hydED')
                ax.plot([], c='#31D6E2', marker='s', markersize=markersize_plot, linestyle='', label='hydNPND')
                ax.plot([], c='#5FA6A8', marker='s', markersize=markersize_plot, linestyle='', label='hydND')
                ax.plot([], c='#52216B', marker='s', markersize=markersize_plot, linestyle='', label='gas-cc')
                ax.plot([], c='#C2A1DB', marker='s', markersize=markersize_plot, linestyle='', label='gas-ct')
                ax.plot([], c='#765189', marker='s', markersize=markersize_plot, linestyle='', label='o-g-s')
                ax.plot([], c='#222222', marker='s', markersize=markersize_plot, linestyle='', label='coaloldscr')
                ax.plot([], c='#3E3C3C', marker='s', markersize=markersize_plot, linestyle='', label='coalolduns')
                ax.plot([], c='#5B5A5A', marker='s', markersize=markersize_plot, linestyle='', label='coal-igcc')
                ax.plot([], c='#A19E9E', marker='s', markersize=markersize_plot, linestyle='', label='coal-new')
                ax.plot([], c='#820000', marker='s', markersize=markersize_plot, linestyle='', label='nuclear')

                leg = ax.legend(loc='center left', bbox_to_anchor=(0.9, 0.5), fontsize = 6, 
                                    handletextpad=handletextpad, 
                                    labelspacing=0.1, frameon=False)
                plt.setp(leg.get_texts(), family='Arial', fontsize=5)
            
            # Only keep top and right spines of plot
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.spines['bottom'].set_linewidth(0.7)
            ax.spines['left'].set_linewidth(0.7)
    
    # Save data
    fig.savefig(os.path.join(figure_path,figname+'.png'), dpi=600, bbox_inches='tight')

def comparison_plotting_nat(ax,i,df,finalyear,x,title):

    print('Plotting national capacity: ' + title)
    year_set = sorted(range(startyear,finalyear+1))
    df_cap_temp = df.copy()

    # tech and year combination
    year_tech_combos = list(itertools.product(techs, year_set))
    df_cap = pd.DataFrame(year_tech_combos, columns=['tech',x])
    df_cap['summer_power_capacity_GW'] = 0.0

    for row in list(range(len(df_cap_temp))):
        tech = df_cap_temp['tech'][row]
        year = df_cap_temp[x][row]
        df_cap.loc[(df_cap['tech']==tech) & 
                    (df_cap[x]==year),
                    'summer_power_capacity_GW'] = df_cap_temp['summer_power_capacity_GW'][row]

    if (df_cap['summer_power_capacity_GW'] < 0).any():
        df_cap_pos = df_cap[df_cap['summer_power_capacity_GW']>=0]
        df_cap_neg = df_cap[df_cap['summer_power_capacity_GW']<=0]

        sns.histplot(data=df_cap_pos, x=x, hue="tech", multiple="stack",
                     weights='summer_power_capacity_GW',
                     hue_order=techs,palette=color_techs, binwidth=0.85, shrink=0.9,
                     edgecolor=None, legend=False, discrete=True, ax = ax)
        sns.histplot(data=df_cap_neg, x=x, hue="tech", multiple="stack",
                     weights='summer_power_capacity_GW',
                     hue_order=techs,palette=color_techs, binwidth=0.85, shrink=0.9,
                     edgecolor=None, legend=False, discrete=True, ax = ax)
    else:          
        sns.histplot(data=df_cap, x=x, hue="tech", multiple="stack",
                     weights='summer_power_capacity_GW',
                     hue_order=techs,palette=color_techs, binwidth=0.85, shrink=0.9,
                     edgecolor=None, legend=False, discrete=True, ax = ax)

    ax.set_title(title,fontsize=8,fontweight='bold',fontname="Arial",pad=-6)

    # Only display a few years in x-axis
    # Define the exact tick marks you want to show
    display_years = sorted(range(startyear,finalyear+1))

    # Set both locations and labels simultaneously
    ax.set_xticks(display_years)
    # ax.set_xticks(year_set)
    ax.set_xlabel('')
    #ax.set_xticklabels([''])
    #minor_locator = AutoMinorLocator(2)
    #ax.yaxis.set_minor_locator(minor_locator)

    ax.grid(color='lightgray', linestyle='dashed', linewidth=0.3, axis='y',zorder=0)
    ax.set_axisbelow(True)
    ax.tick_params(axis='both', which='major', width=0.5, length=1.5, pad=1)
    ax.tick_params(axis='both', which='minor', width=0.3, length=1, pad=1)
    if 'retire' in title:
        labelsize = 7
    else:
        labelsize = 9
    ax.tick_params(labelsize=labelsize)
    ax.yaxis.get_offset_text().set_fontsize(labelsize)
    ax.tick_params(axis='x', labelrotation=90)
    ax.tick_params(right=False, labelright=False)
    ax.tick_params(top=False, labeltop=False)

    ax.set_ylabel('[GW]', fontsize=8,fontweight='bold',fontname="Arial", labelpad=1)

    if i == 2:
        ax.plot([], c='#FF4A88', marker='s', markersize=markersize_plot, linestyle='', label='battery_li')
        ax.plot([], c='#A75F8A', marker='s', markersize=markersize_plot, linestyle='', label='pvb_battery')
        ax.plot([], c='#C630B2', marker='s', markersize=markersize_plot, linestyle='', label='pumped-hydro')
        ax.plot([], c='#FFC903', marker='s', markersize=markersize_plot, linestyle='', label='upv')
        ax.plot([], c='#FEE603', marker='s', markersize=markersize_plot, linestyle='', label='dupv')
        ax.plot([], c='#A27C12', marker='s', markersize=markersize_plot, linestyle='', label='pvb_pv')
        ax.plot([], c='#F3660E', marker='s', markersize=markersize_plot, linestyle='', label='csp-ns')
        ax.plot([], c='#00B6EF', marker='s', markersize=markersize_plot, linestyle='', label='wind-ons')
        ax.plot([], c='#106BA7', marker='s', markersize=markersize_plot, linestyle='', label='wind-ofs')
        ax.plot([], c='#5B9844', marker='s', markersize=markersize_plot, linestyle='', label='biopower')
        ax.plot([], c='#3B692A', marker='s', markersize=markersize_plot, linestyle='', label='lfill-gas')
        ax.plot([], c='#187F94', marker='s', markersize=markersize_plot, linestyle='', label='hydEND')
        ax.plot([], c='#37A5BB', marker='s', markersize=markersize_plot, linestyle='', label='hydED')
        ax.plot([], c='#31D6E2', marker='s', markersize=markersize_plot, linestyle='', label='hydNPND')
        ax.plot([], c='#5FA6A8', marker='s', markersize=markersize_plot, linestyle='', label='hydND')
        ax.plot([], c='#52216B', marker='s', markersize=markersize_plot, linestyle='', label='gas-cc')
        ax.plot([], c='#C2A1DB', marker='s', markersize=markersize_plot, linestyle='', label='gas-ct')
        ax.plot([], c='#765189', marker='s', markersize=markersize_plot, linestyle='', label='o-g-s')
        ax.plot([], c='#222222', marker='s', markersize=markersize_plot, linestyle='', label='coaloldscr')
        ax.plot([], c='#3E3C3C', marker='s', markersize=markersize_plot, linestyle='', label='coalolduns')
        ax.plot([], c='#5B5A5A', marker='s', markersize=markersize_plot, linestyle='', label='coal-igcc')
        ax.plot([], c='#A19E9E', marker='s', markersize=markersize_plot, linestyle='', label='coal-new')
        ax.plot([], c='#820000', marker='s', markersize=markersize_plot, linestyle='', label='nuclear')

        leg = ax.legend(loc='center left', bbox_to_anchor=(0.97, 0.5), 
                        fontsize=7, handletextpad=handletextpad, 
                        labelspacing=0.1, frameon=False)
        plt.setp(leg.get_texts(), family='Arial', fontsize=7)
    
    # Only keep top and right spines of plot
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_linewidth(0.7)
    ax.spines['left'].set_linewidth(0.7)

def mismatching_FIPS(df_old, df_new, x, type):
    data_new_fips = df_new.copy()
    data_new_fips = data_new_fips[['tech','FIPS',x,'summer_power_capacity_GW']]
    data_old_fips = df_old.copy()
    data_old_fips = data_old_fips[['tech','FIPS',x,'summer_power_capacity_GW']]

    data_new_fips = data_new_fips.groupby(['tech','FIPS',x], as_index=False).sum()
    data_old_fips = data_old_fips.groupby(['tech','FIPS',x], as_index=False).sum()

    data_fips = data_new_fips.merge(data_old_fips, on=['tech','FIPS',x], how='outer').fillna(0)
    data_fips['cap_diff'] = abs(data_fips['summer_power_capacity_GW_x'] - data_fips['summer_power_capacity_GW_y'])
    for f in data_fips['FIPS'].unique().tolist():
        data_fips_f = data_fips[data_fips['FIPS']==f]
        if data_fips_f['cap_diff'].sum() > 1.0E-15:
            print(f"FIPS {f} has mismatched {type} capacities between two versions of NEMS")
            data_fips_f[['tech','FIPS',x,
                         'summer_power_capacity_GW_x',
                         'summer_power_capacity_GW_y',
                         'cap_diff']].rename(
                             columns={'summer_power_capacity_GW_x':'cap_new',
                                      'summer_power_capacity_GW_y':'cap_old'}).to_csv(
                                          os.path.join(debug_path,'mismatched_'+type+'_cap_'+f+'.csv'),
                                          index=False)
        else:
            print(f"FIPS {f} has no mismatched {type} capacities between two versions of NEMS")


if __name__ == "__main__":
    print("Starting e_comparison_plotting.py")

    # Merge data with county2zone
    dfnew = dfnew.merge(county2zone, on='FIPS', how='left')
    dfold = dfold.merge(county2zone, on='FIPS', how='left')

    # Establish all zones
    zones = sorted(dfnew['r'].unique().tolist())

    #################################
    ### Planned online comparison ###
    #################################
    # Raw online data
    online_data_new = dfnew.loc[dfnew['StartYear']>=startyear]
    online_data_new['summer_power_capacity_GW'] = online_data_new['summer_power_capacity_MW']/1000
    
    online_data_old = dfold.loc[dfold['StartYear']>=startyear]
    online_data_old['summer_power_capacity_GW'] = online_data_old['summer_power_capacity_MW']/1000

    # Find FIPS that have different online years for each tech
    mismatching_FIPS(online_data_old, online_data_new, x='StartYear', type='online')

    # Aggredate across r
    online_data_new = online_data_new[['tech','r','StartYear','summer_power_capacity_GW']]
    online_data_new = online_data_new.groupby(['tech','r','StartYear'], as_index=False).sum()
    
    online_data_old = online_data_old[['tech','r','StartYear','summer_power_capacity_GW']]
    online_data_old = online_data_old.groupby(['tech','r','StartYear'], as_index=False).sum()

    # Diff online data
    online_data_diff = online_data_new.copy()
    online_data_diff = online_data_diff.rename(columns={'summer_power_capacity_GW':'cap_new'})
    online_data_diff = online_data_diff.merge(online_data_old, 
                                              on=['tech','r','StartYear'], 
                                              how='outer').rename(columns={'r_x':'r'})
    online_data_diff = online_data_diff.rename(columns={'summer_power_capacity_GW':'cap_old'})
    online_data_diff['cap_old'] = online_data_diff['cap_old'].fillna(0)
    online_data_diff['cap_new'] = online_data_diff['cap_new'].fillna(0)
    online_data_diff['summer_power_capacity_GW'] = online_data_diff['cap_new'] - online_data_diff['cap_old']

    # Raw and diff online data at national level
    online_data_new_nat = online_data_new.groupby(['tech','StartYear'], as_index=False).sum()
    online_data_old_nat = online_data_old.groupby(['tech','StartYear'], as_index=False).sum()
    online_data_diff_nat = online_data_diff.groupby(['tech','StartYear'], as_index=False).sum()

    # Plot new, old NEMS at national level and their difference
    figsize = (5,7)
    markersize_plot = 2
    handletextpad = 0.1
    nrows = 3
    fig, axes = plt.subplots(nrows,figsize=figsize, sharex=True, sharey=True)
    for i in list(range(nrows)):
        ax = axes[i]
        if i == 0:
            comparison_plotting_nat(ax,i,online_data_new_nat,finalyear_online, 
                                    x='StartYear',title='Planned online capacity - CONUS - new NEMS')
        elif i == 1:
            comparison_plotting_nat(ax,i,online_data_old_nat,finalyear_online, 
                                    x='StartYear',title='Planned online capacity - CONUS - current NEMS')
        elif i == 2:
            comparison_plotting_nat(ax,i,online_data_diff_nat,finalyear_online, 
                                    x='StartYear',title='Planned online capacity difference - CONUS')

    # Save data
    fig.savefig(os.path.join(figure_path,'planned_online_conus.png'), dpi=600, bbox_inches='tight')

    # Plot new, old NEMS at zonal level and their difference
    comparison_plotting_r(online_data_new, zones, finalyear_online, techs, color_techs, 
                          figname='planned_online_new_r', x='StartYear',
                          title='Planned online capacity by zone - new NEMS [GW]')
    comparison_plotting_r(online_data_old, zones, finalyear_online, techs, color_techs,
                          figname='planned_online_current_r', x='StartYear', 
                          title='Planned online capacity by zone - current NEMS [GW]')
    # Difference (new NEMS - current NEMS)
    comparison_plotting_r(online_data_diff, zones, finalyear_online, techs, color_techs, 
                          figname='planned_online_diff_r', x='StartYear', 
                          title='Planned online capacity difference by zone (new NEMS - current NEMS) [GW]')

    #################################
    ### Planned retire comparison ###
    #################################

    # Raw retire data
    retire_data_new = dfnew.loc[dfnew['RetireYear']<=finalyear_retire]
    retire_data_new['summer_power_capacity_GW'] = retire_data_new['summer_power_capacity_MW']/1000
    
    retire_data_old = dfold.loc[dfold['RetireYear']<=finalyear_retire]
    retire_data_old['summer_power_capacity_GW'] = retire_data_old['summer_power_capacity_MW']/1000

    # Find FIPS that have different retire years for each tech
    mismatching_FIPS(retire_data_old, retire_data_new, x='RetireYear', type='retire')

    # Aggredate across r
    retire_data_old = retire_data_old[['tech','r','RetireYear','summer_power_capacity_GW']]
    retire_data_old = retire_data_old.groupby(['tech','r','RetireYear'], as_index=False).sum()

    retire_data_new = retire_data_new[['tech','r','RetireYear','summer_power_capacity_GW']]
    retire_data_new = retire_data_new.groupby(['tech','r','RetireYear'], as_index=False).sum()

    # Diff retire data
    retire_data_diff = retire_data_new.copy()
    retire_data_diff = retire_data_diff.rename(columns={'summer_power_capacity_GW':'cap_new'})
    retire_data_diff = retire_data_diff.merge(retire_data_old, 
                                              on=['tech','r','RetireYear'], 
                                              how='outer').rename(columns={'r_x':'r'})
    retire_data_diff = retire_data_diff.rename(columns={'summer_power_capacity_GW':'cap_old'})
    retire_data_diff['cap_old'] = retire_data_diff['cap_old'].fillna(0)
    retire_data_diff['cap_new'] = retire_data_diff['cap_new'].fillna(0)
    retire_data_diff['summer_power_capacity_GW'] = retire_data_diff['cap_new'] - retire_data_diff['cap_old']

    # Raw and diff online data at national level
    retire_data_new_nat = retire_data_new.groupby(['tech','RetireYear'], as_index=False).sum()
    retire_data_old_nat = retire_data_old.groupby(['tech','RetireYear'], as_index=False).sum()
    retire_data_diff_nat = retire_data_diff.groupby(['tech','RetireYear'], as_index=False).sum()

    # Plot new, old NEMS at national level and their difference
    figsize = (5,7)
    markersize_plot = 2
    handletextpad = 0.1
    nrows = 3
    fig, axes = plt.subplots(nrows,figsize=figsize, sharex=True, sharey=True)
    for i in list(range(nrows)):
        ax = axes[i]

        if i == 0:
            comparison_plotting_nat(ax,i,retire_data_new_nat,finalyear_retire, 
                                    x='RetireYear',title='Planned retire capacity - CONUS - new NEMS')
        elif i == 1:
            comparison_plotting_nat(ax,i,retire_data_old_nat,finalyear_retire, 
                                    x='RetireYear',title='Planned retire capacity - CONUS - current NEMS')
        # Difference (new NEMS - current NEMS)
        elif i == 2:
            comparison_plotting_nat(ax,i,retire_data_diff_nat,finalyear_retire, 
                                    x='RetireYear',title='Planned retire capacity difference - CONUS')
    # Save data
    fig.savefig(os.path.join(figure_path,'planned_retire_conus.png'), dpi=600, bbox_inches='tight')

    # Plot new, old NEMS at zonal level and their difference
    comparison_plotting_r(retire_data_new, zones, finalyear_retire,  techs, color_techs, 
                          figname='planned_retire_new_r',x='RetireYear', 
                          title='Planned retire capacity [GW] - new NEMS')
    comparison_plotting_r(retire_data_old, zones, finalyear_retire,  techs, color_techs, 
                          figname='planned_retire_current_r', x='RetireYear', 
                          title='Planned retire capacity [GW] - current NEMS')
    # Difference (new NEMS - current NEMS)
    comparison_plotting_r(retire_data_diff, zones, finalyear_retire,  techs, color_techs, 
                          figname='planned_retire_diff_r', x='RetireYear', 
                          title='Planned retire capacity difference (new NEMS - current NEMS) [GW]')

    print("Finished e_comparison_plotting.py")
