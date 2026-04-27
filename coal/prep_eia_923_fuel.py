#%%
import os
import sys
import numpy as np
import zipfile
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
import seaborn as sns

#%%
# Get FIPS from ReEDS-2.0 repo
reeds_path = os.path.expanduser('~/Documents/GitHub/ReEDS/public_ReEDS/ReEDS')
sys.path.append(reeds_path)
import reeds

dir = '/Volumes/ReEDS/FY26- Historical validation/'
# Note: Coal price in EIA is in cents/MMBTU and quantity is in ton
# folder with your zip files
input_folder = "EIA_923"
dollar_year=2024
unit = 'mmbtu'                # ton or mmbtu
resolution = 'fips'         # fips, cendiv, state, national
type = 'all'                # BIT, SUB, LIG, RC, WC, all
recent_years = 20            # plot 5, 10, etc most recent years
year_weight = True

if recent_years == 5:
    yearset = [2020,2021,2022,2023,2024]
elif recent_years == 10:
    yearset = [2015,2016,2017,2018,2019,2020,2021,2022,2023,2024]
else:
    yearset = [2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023,2024]

# Read in processed coal data
df_matched = pd.read_csv(os.path.join("outputs", "NEMS_EIA_matched1.csv"))
df_unmatched = pd.read_csv(os.path.join("outputs", "NEMS_EIA_unmatched1_manually_cleaned.csv"))
df_total = pd.concat([df_matched, df_unmatched], axis=0)
df_total = df_total.rename(columns={'Plant State':'State'})

'''
##
df_20 = pd.read_csv(os.path.join('outputs','weighted_fuel_cost_across_years20.csv'))
df_20 = df_20.rename(columns={'Weighted_Fuel_Cost':'Weighted_Fuel_Cost-20-years'})
df_10 = pd.read_csv(os.path.join('outputs','weighted_fuel_cost_across_years10.csv'))
df_10 = df_10.rename(columns={'Weighted_Fuel_Cost':'Weighted_Fuel_Cost-10-years'})
df = df_20.merge(df_10, on='FIPS', how='outer')

df_5 = pd.read_csv(os.path.join('outputs','weighted_fuel_cost_across_years5.csv'))
df_5 = df_5.rename(columns={'Weighted_Fuel_Cost':'Weighted_Fuel_Cost-5-years'})

df = df.merge(df_5, on='FIPS', how='outer')
df.to_csv(os.path.join('outputs','coal_prices_by_years_weighted.csv'), index=False)

###
'''
 # %%
def main():
    final_df = prepare_fuel_product_df()
    final_df.to_csv(os.path.join('outputs','coal_prices_by_'+resolution+'_'+unit+'_'+type+'.csv'), index=False)

    #final_df = pd.read_csv(os.path.join('outputs','coal_prices_by_'+resolution+'_'+unit+'_'+type+'.csv'))
    #final_df = pd.read_csv(os.path.join('outputs','coal_prices_by_fips_mmbtu_2010-2024.csv'))
    #final_df = pd.read_csv(os.path.join('outputs','weighted_fuel_cost_across_years50.csv'))
    
    final_df = final_df[final_df['Year'].isin(yearset)]

    if year_weight:
        final_df = final_df.groupby(['FIPS'], as_index=False).agg({'Total_Fuel_Cost': 'sum', 'Quantity': 'sum'})
        final_df['Weighted_Fuel_Cost'] = final_df['Total_Fuel_Cost']/final_df['Quantity']
        final_df = final_df[final_df['Weighted_Fuel_Cost']>0]

        final_df.to_csv(os.path.join('outputs','weighted_fuel_cost_across_years'+str(recent_years)+'.csv'), index=False)
    else:
        if resolution == 'state':
            index = 'State'
        elif resolution == 'fips':
            index = 'FIPS'
        else:
            index = 'Country'
        final_df_pivot = final_df.pivot_table(values='Weighted_Fuel_Cost', index=index,
                                            columns='Year').fillna(0.0)
        final_df_pivot = final_df_pivot.reset_index()

        # Plot the weighted fuel cost:
        line_plot(final_df_pivot)
# %%

def prepare_fuel_product_df():
    all_dfs = []
    
    # Pre-calculate Deflator (only once)
    deflator_map = {}
    deflator = pd.read_csv(os.path.join(dir,'deflator.csv'))
    base_def_val = deflator.loc[deflator["*Dollar.Year"] == dollar_year, "Deflator"].values[0]
    # Create a quick-access dictionary: {year: multiplier}
    deflator_map = (deflator["Deflator"] / base_def_val).set_axis(deflator["*Dollar.Year"]).to_dict()

    # Configuration for different Excel versions
    file_config = {
        ".xlsx": {"sheet": "Page 5 Fuel Receipts and Costs", "header": 4},
        ".xls":  {"sheet": "Page 5 Fuel Receipts and Cost",  "header": 7}
    }

    for filename in os.listdir(os.path.join(dir,input_folder)):
        if not filename.endswith(".zip"):
            continue
        #if '2011' not in filename:
        #    continue

        print(filename)  

        zip_path = os.path.join(dir,input_folder, filename)
        with zipfile.ZipFile(zip_path, "r") as z:
            # Get the target file (assuming it's the second file in namelist)
            internal_file = z.namelist()[1]
            ext = os.path.splitext(internal_file)[1]
            
            if ext not in file_config:
                print(f"Unsupported file type: {ext} in {internal_file}. Skipping.")
                continue

            with z.open(internal_file) as f:
                conf = file_config[ext]

                df_EIA923 = pd.read_excel(f, sheet_name=conf["sheet"], header=conf["header"])
                df_EIA923.columns = df_EIA923.columns.str.replace('\n', '_', regex=True)
                df_EIA923 = df_EIA923.rename(columns={'Plant ID':'T_PID','Energy_Source': 'Fuel Code',
                                                      'Plant Id':'T_PID','ENERGY_SOURCE': 'Fuel Code',
                                                      'YEAR':'Year','MONTH':'Month',
                                                      'FUEL_GROUP':'Fuel_Group',
                                                      'FUEL_COST':'Fuel_Cost',
                                                      'QUANTITY':'Quantity',
                                                      'Average Heat_Content':'Average_Heat_Content',
                                                      'AVERAGE_HEAT_CONTENT':'Average_Heat_Content',
                                                      'STATE':'State',
                                                      'Plant State':'State'})
                year = df_EIA923['Year'].unique()[0]
                df_EIA923 = df_EIA923[df_EIA923['Fuel_Group']=='Coal']
                df_EIA923 = df_EIA923[['Year','Month','T_PID','Fuel Code','Fuel_Cost','Quantity','State','Average_Heat_Content']]
                df_EIA923 = df_EIA923[df_EIA923['Fuel_Cost']!='.']
                df_EIA923['Fuel_Cost'] /= 100   # Fuel in $/MMBTU

                if unit == 'ton':
                    df_EIA923['Fuel_Cost'] *=  df_EIA923['Average_Heat_Content']        # Convert $/mmbtu to $/ton

                if unit == 'mmbtu':
                    df_EIA923['Quantity'] *= df_EIA923['Average_Heat_Content']          # Convert short ton to MMBTU

                df_EIA923['Total_Fuel_Cost'] = df_EIA923['Fuel_Cost'] * df_EIA923['Quantity']
                df_EIA923 = df_EIA923.groupby(['T_PID','Fuel Code'], as_index=False).agg({'Total_Fuel_Cost': 'sum', 'Quantity': 'sum'})

                # Merge in coal prices:
                df = df_total.merge(df_EIA923,on=['T_PID','Fuel Code'],how='left')
                if type !='all':
                    df = df[df['Fuel Code']==type]
                df = df[['T_PID','Fuel Code','T_LAT','T_LONG','FIPS','State','Quantity','Total_Fuel_Cost']]
                df = df[~df['Total_Fuel_Cost'].isna()]
                
                if resolution == 'fips':
                    df_fips = df.groupby(['FIPS'], as_index=False).agg({'Total_Fuel_Cost': 'sum', 'Quantity': 'sum',
                                                                        'T_LAT':'first','T_LONG':'first','State':'first'})
                elif resolution == 'state':
                    df_fips = df.groupby(['State'], as_index=False).agg({'Total_Fuel_Cost': 'sum', 'Quantity': 'sum'})
                elif resolution == 'national':
                    df_fips = pd.DataFrame([[0, 0]],columns=['Total_Fuel_Cost','Quantity'])
                    df_fips['Total_Fuel_Cost'] = df['Total_Fuel_Cost'].sum()
                    df_fips['Quantity'] = df['Quantity'].sum()
                    df_fips['Country'] = 'USA'


                df_fips['Weighted_Fuel_Cost'] = df_fips['Total_Fuel_Cost']/df_fips['Quantity']
                df_fips['Year'] = year

                if resolution == 'fips':
                    df_fips = df_fips[['Year','FIPS','T_LAT','T_LONG','Weighted_Fuel_Cost','Total_Fuel_Cost','Quantity']]
                elif resolution == 'state':
                    df_fips = df_fips[['Year','State','Weighted_Fuel_Cost','Total_Fuel_Cost','Quantity']]
                elif resolution == 'national':
                    df_fips = df_fips[['Year','Country','Weighted_Fuel_Cost','Total_Fuel_Cost','Quantity']]

                df_fips['Weighted_Fuel_Cost'] *= deflator_map[year]
                df_fips['Total_Fuel_Cost'] *= deflator_map[year]

                all_dfs = all_dfs + [df_fips]

    df_final = pd.concat(all_dfs, ignore_index=False, sort=False)            
                  
    return df_final

def line_plot(df_pivot):
    colors = ["#509D34","#187F94",'#A96235',
              '#FFC903','#820000','#52216B',
              '#CC0079','#806A62',"#0D1B60",
              "#E15B32","#0A6206", "#1AECEC",
              "#BF00EF","#886E06", "#5D62C6"]

    # Box plot & scatter
    df_list = []
    for i in list(range(len(yearset))):
        if resolution == 'state':
            df = df_pivot[['State',yearset[i]]]
        elif resolution == 'fips':
            df = df_pivot[['FIPS',yearset[i]]]
        else:
            df = df_pivot[['Country',yearset[i]]]
        df = df.rename(columns={yearset[i]:'Weighted_Fuel_Cost'})
        df['Year'] = yearset[i]
        df_list = df_list + [df]

    df_long = pd.concat(df_list, ignore_index=False, sort=False)
    df_long['Weighted_Fuel_Cost'] = df_long['Weighted_Fuel_Cost'].replace(0, np.nan)
    df_long = df_long.reset_index()

    figsize = (20,5)
    fig, ax = plt.subplots(figsize=figsize)
    
    if resolution == 'state':
        ax = sns.stripplot(x=df_long['State'], y=df_long['Weighted_Fuel_Cost'], color ="lightpink",jitter=False, s=3)
    elif resolution == 'fips':
        ax = sns.stripplot(x=df_long['FIPS'], y=df_long['Weighted_Fuel_Cost'], color ="lightpink",jitter=False, s=3)
    else:
        ax = sns.stripplot(x=df_long['Country'], y=df_long['Weighted_Fuel_Cost'], color ="lightpink",jitter=False, s=3)
    ax.tick_params(axis ='x',labelsize=4,top=False)
    ax.tick_params(axis ='y',labelsize=8,right=False)
    if unit == 'mmbtu':
        ax.set_ylabel('Weighted Coal Fuel Cost ($/MMBTU)')
    elif unit == 'ton':
        ax.set_ylabel('Weighted Coal Fuel Cost ($/Ton)')
    # Only keep top and right spines of plot
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(color='lightgray', linestyle='dashed', linewidth=0.1, axis='y')    
    
    plt.xticks(rotation=90)
    fig.savefig(os.path.join('figures','weighted_fuel_costs_'+resolution+'_scatter_'+unit+'_'+type+'_'+'.png'), dpi=400, bbox_inches='tight')
    
    # Boxplot (MMBTU)
    figsize = (20,5)
    fig, ax = plt.subplots(figsize=figsize)
    if resolution == 'state':
        ax = sns.boxplot(x=df_long['State'], y=df_long['Weighted_Fuel_Cost'], color='lightpink', width=0.5,linewidth=0.1,
                flierprops = dict(markerfacecolor = '0.50', markersize = 0.5),orient='v')
    elif resolution == 'fips':
        ax = sns.boxplot(x=df_long['FIPS'], y=df_long['Weighted_Fuel_Cost'], color='lightpink', width=0.5,linewidth=0.1,
                flierprops = dict(markerfacecolor = '0.50', markersize = 0.5),orient='v')
    else:
        ax = sns.boxplot(x=df_long['Country'], y=df_long['Weighted_Fuel_Cost'], color='lightpink', width=0.5,linewidth=0.1,
                flierprops = dict(markerfacecolor = '0.50', markersize = 0.5),orient='v')

    ax.tick_params(axis ='x',labelsize=4,top=False)
    ax.tick_params(axis ='y',labelsize=8,right=False)
    # Only keep top and right spines of plot
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    if unit == 'mmbtu':
        ax.set_ylabel('Weighted Coal Fuel Cost ($/MMBTU)')
    elif unit == 'ton':
        ax.set_ylabel('Weighted Coal Fuel Cost ($/Ton)')
    ax.grid(color='lightgray', linestyle='dashed', linewidth=0.1, axis='y')    
    
    plt.xticks(rotation=90)
    fig.savefig(os.path.join('figures','weighted_fuel_costs_'+resolution+'_box_plot_'+unit+'_'+type+'_'+'.png'), dpi=400, bbox_inches='tight')

    # Boxplot (ton)
    #figsize = (20,5)
    #fig, ax = plt.subplots(figsize=figsize)
    #ax = sns.boxplot(x=df_long['FIPS'], y=df_long['Weighted_Fuel_Cost'], color='lightpink', width=0.5,linewidth=0.1,
    #            flierprops = dict(markerfacecolor = '0.50', markersize = 0.5),orient='v')

    #ax.tick_params(axis ='x',labelsize=4,top=False)
    #ax.tick_params(axis ='y',labelsize=8,right=False)
    # Only keep top and right spines of plot
    #ax.spines['top'].set_visible(False)
    #ax.spines['right'].set_visible(False)
    #ax.set_ylabel('Weighted Coal Fuel Cost ($/MMBTU)')
    #ax.grid(color='lightgray', linestyle='dashed', linewidth=0.1, axis='y')    
    
    #plt.xticks(rotation=90)
    #fig.savefig(os.path.join('figures','weighted_fuel_costs_fips_box_plot_ton.png'), dpi=400, bbox_inches='tight')

main()
