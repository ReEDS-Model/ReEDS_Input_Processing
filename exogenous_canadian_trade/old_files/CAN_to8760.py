#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 25 13:08:54 2021

@author: mbrown1
"""

                                                                     
import os
import pandas as pd
#from datetime import datetime

base_year_data = 2024

#os.chdir("/Users/mbrown1/Documents/GitHub/r2flexmerge/inputs/flex")
share_imports = pd.read_csv(os.path.join("share_imports.csv"))
share_imports.columns = ['r','reg','t','Val']
share_exports = pd.read_csv(os.path.join("share_exports.csv"))
share_exports.columns = ['r','reg','t','Val']
total_imports = pd.read_csv(os.path.join("total_imports.csv"))
total_imports.columns = ['reg','t','Val']
total_exports = pd.read_csv(os.path.join("total_exports.csv"))
total_exports.columns = ['reg','t','Val']

trade = pd.read_csv("PLEXOS_NARIS_trade.csv")
trade.columns = ['route','time','from','to','Val']
trade = trade[trade['route'].isin(['Canada-EI_USA-EI','Canada-WI_USA-WI','Canada-Quebec_USA-EI'])]
#convert to MWh
#12 observations per hour (every 5 minutes)
trade['Val'] = trade['Val'] / 12 
trade[['date','tnew']] = trade['time'].str.split(pat=" ",expand=True)
trade['datetime'] = pd.to_datetime(trade['time'])
trade['day'] = trade['datetime'].apply(lambda x: x.timetuple().tm_yday)

#remove leapyear
trade = trade[trade['day']!=60]
trade.loc[trade.day>60,'day'] = trade.loc[trade.day>60,'day'] - 1

#compute hour
trade['hour'] = trade['datetime'].apply(lambda x: x.timetuple().tm_hour)

#compute hour in terms of 8760
trade['h'] = (trade['day'] - 1) * 24 + trade['hour'] + 1

#sum by route and 8760 hour
collapsed = trade[['route','h','Val']].groupby(['route','h']).sum().reset_index()
collapsed['route'] = collapsed['route'].replace("Canada-EI_USA-EI",'east')
collapsed['route'] = collapsed['route'].replace("Canada-WI_USA-WI",'wecc')

#imports to us and exports from us
imports = collapsed[collapsed['Val']>0]
exports = collapsed[collapsed['Val']<0]


def share_out(df_trade,df_share,df_growth,base_year):
    '''
    this function takes the 8760 values of imports/exports
    and increases those values by the growth pattern in df_growth
    relative to the base_year of the data then shares those 
    values out to BAs via df_share
    '''
    #df_trade = imports.copy()
    #df_share = share_imports.copy()
    #df_growth = total_imports.copy()
    #base_year = base_year_data
    
    #compute relative aggregate growth relative to base_year
    df_gm = df_growth.pivot_table(index='reg',columns='t',values='Val').reset_index()
    
    #loop over all years not in the base year and divide by the base year value
    for i in range(2010,2051,1):
        if i != base_year:
            df_gm[i] = df_gm[i] / df_gm[base_year]
            
    df_gm[base_year] = 1
    df_gm = pd.melt(df_gm,id_vars=['reg'])
    
    df_trade.columns = ['reg','h','value']
    
    df_trade_growth = pd.merge(df_gm,df_trade,how='outer',on='reg')
    
    df_allmerge = pd.merge(df_trade_growth,df_share,how='outer',on=['reg','t'])
    
    #final amount is the product of.. 
    #  the annual growth factor relative to base year
    #  amount of trade occurring in that region for each 'h'
    #  the share of the BAs trade as a fraction of the whole
    df_allmerge['out'] = df_allmerge['value_x'] * df_allmerge['value_y'] * df_allmerge['Val']
    
    df_out = df_allmerge[['r','h','t','out']]
    
    return df_out

#merge together imports and exports to get net trade
imports_8760 = share_out(imports,share_imports,total_imports,base_year_data)
exports_8760 = share_out(exports,share_exports,total_exports,base_year_data)
alltrade = pd.merge(imports_8760,exports_8760,how='outer',on=['r','h','t'])
#need to drop any missing values from key indices 
alltrade = alltrade.dropna(subset=['r','h','t'])
#rename columns, fill NaNs with zero, and take net amount
alltrade.columns = ['r','h','t','imports','exports']
alltrade['imports'] = alltrade['imports'].fillna(0)
alltrade['exports'] = alltrade['exports'].fillna(0)
alltrade['net'] = alltrade['imports'] + alltrade['exports']

trade_out = alltrade[['r','h','t','net']]

#improve output formatting
trade_out['h'] = 'h' + trade_out['h'].astype('int').astype('str')
trade_out['t'] = trade_out['t'].astype('int')

trade_out.to_csv('can_trade_8760.csv',index=False)
    
# map to h17 hours...
#os.chdir("../../")
#need top 40 hours of load for h17 mapping..
load = pd.read_csv("load.csv.gz"))
load = load.rename(columns={'Unnamed: 0':'h'})
loadm = pd.melt(load,id_vars='h')
loadm['h'] = 'h' + loadm['h'].astype('str')


# hour_link_augur taken from run of defaults.csv
link = pd.read_csv("hour_link_augur.csv")

top_hours = 40

def find_top_hours(link_df,df_in,top_hours):
    link_out = pd.DataFrame()
    summer_hours = list(range(3624,5833,1))
    summ = ['h' + str(i) for i in summer_hours]
    #only want to find top hours in summer this time..
    df_in = df_in[df_in['h'].isin(summ)]
    for i in df_in['variable'].unique():
        #subset based on top hours
        link_temp = link[link['r']==i]
        #find top hours of load
        load_temp = df_in[df_in['variable']==i].nlargest(top_hours,['value'])
        #df[-df["column"].isin(["value"])]
        link_temp = link_temp[-link_temp["h_8760"].isin(load_temp['h'].unique())]
        link_adder = pd.DataFrame(data={'h_8760':load_temp['h'].unique()})
        link_adder['r'] = i
        link_adder['h_modeled'] = 'h17'
        link_out = pd.concat([link_out,link_temp,link_adder])
    
    return link_out
        

link_out = find_top_hours(link,loadm,top_hours)

trade_h17 = trade_out.copy()
trade_h17 = trade_h17.rename(columns={'h':'h_8760','net':'Val'})
trade_h17 = pd.merge(trade_h17,link_out,how='left',on=['r','h_8760'])
trade_h17_out = trade_h17[['r','h_modeled','t','Val']]
trade_h17_out = trade_h17_out.groupby(['r','h_modeled','t']).sum().reset_index()
trade_h17_out = trade_h17_out.rename(columns={'h_modeled':'h'})

trade_h17_out.to_csv("net_trade_can_h17.csv",index=False,header=False)



    
    