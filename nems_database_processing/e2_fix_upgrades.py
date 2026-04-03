# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 16:33:51 2019

@author: afrazier
"""

import pandas as pd

def fix_upgrades(nems):

    nems["Unique ID"] = nems.index
    
    subset = nems[['tech','reeds_ba','resource_region','summer_power_capacity_MW','RetireYear','StartYear','IsExistUnit','HeatRate','T_PID','TVIN','TRFURB','Unique ID']]
    
    # Note that T_SYR (now StartYear) is the online year for the most recent time the unit
    # came online. TRFURB holds the original start date of the plant.
    
    # TVIN == 6 means that the unit was retired and 7 corresponds to a unit that was refurbished
    retires = (subset['TVIN'] == 6) & (subset['RetireYear'] >= 2009)
    refurbs = (subset['TVIN'] == 7) & (subset['StartYear'] >= 2010)
    
    rets = subset[retires].reset_index(drop=True)
    refs = subset[refurbs].reset_index(drop=True)
    
    upgrades = pd.concat([refs,rets], sort=False).reset_index(drop=True)
    upgrades = upgrades.sort_values('Unique ID').reset_index(drop=True)
    
    # Make sure units before they retire
    upgrades = upgrades[upgrades['TRFURB']<=upgrades['RetireYear']].reset_index(drop=True)
    upgrades_by_id = upgrades.set_index('Unique ID')
    
    plant_refurbs = pd.DataFrame(columns=['UID_retire','UID_refurb'])
    plant_upgrades = pd.DataFrame(columns=['UID_retire','UID_refurb'])
    plant_downgrades = pd.DataFrame(columns=['UID_retire','UID_refurb'])
    intermediate_ids = []
    no_match_ids = []
    
    start = -1
    i = 0
    while i in range(0,len(upgrades),1):
        # Cycle through the rows until you find a retired unit (TVIN == 6)
        # Log the id, then keep going until you find the refurbed unit (TVIN == 7)
        if start == -1 and upgrades.loc[i,'TVIN'] == 6:
            start = i
            id_ret = upgrades.loc[i,'Unique ID']
        elif start != -1 and upgrades.loc[i,'TVIN'] == 6:
            id_intermediate = upgrades.loc[i,'Unique ID']
            if upgrades_by_id.loc[id_ret,'TRFURB'] == upgrades_by_id.loc[id_intermediate,'TRFURB']:
                intermediate_ids.append(id_intermediate)
        elif start != -1 and upgrades.loc[i,'TVIN'] == 7:
            id_refurb = upgrades.loc[i,'Unique ID']
            # If the retired and refurbished plants have the same original online year, then
            # adjust the plant as a refurbishment, capacity upgrade, or capacity downgrade
            if upgrades_by_id.loc[id_ret,'TRFURB'] == upgrades_by_id.loc[id_refurb,'TRFURB']:
                if upgrades_by_id.loc[id_refurb,'summer_power_capacity_MW'] == upgrades_by_id.loc[id_ret,'summer_power_capacity_MW']:
                    plant_refurb = pd.DataFrame(columns=['UID_retire','UID_refurb'], data=[[id_ret,id_refurb]])
                    plant_refurbs = pd.concat([plant_refurbs,plant_refurb], sort=False).reset_index(drop=True)
                elif upgrades_by_id.loc[id_refurb,'summer_power_capacity_MW'] > upgrades_by_id.loc[id_ret,'summer_power_capacity_MW']:
                    plant_upgrade = pd.DataFrame(columns=['UID_retire','UID_refurb'], data=[[id_ret,id_refurb]])
                    plant_upgrades = pd.concat([plant_upgrades,plant_upgrade], sort=False).reset_index(drop=True)
                elif upgrades_by_id.loc[id_refurb,'summer_power_capacity_MW'] < upgrades_by_id.loc[id_ret,'summer_power_capacity_MW']:
                    plant_downgrade = pd.DataFrame(columns=['UID_retire','UID_refurb'], data=[[id_ret,id_refurb]])
                    plant_downgrades = pd.concat([plant_downgrades,plant_downgrade], sort=False).reset_index(drop=True)
            else:
                no_match_ids.append(id_ret)
                i -= 2
            start = -1
        elif start == -1 and upgrades.loc[i,'TVIN'] == 7:
            pass
        i += 1
        
    nems.loc[:,'index'] = nems.loc[:,'Unique ID']
    nems = nems.set_index('index')
    
    # Assign the start date of the retired refurbished unit to the refurbished unit
    for refurb in range(0,len(plant_refurbs),1):
        nems.loc[plant_refurbs.loc[refurb,'UID_refurb'],'StartYear'] = nems.loc[plant_refurbs.loc[refurb,'UID_retire'],'StartYear']
    
    ret_cols = ['RetireYear','NukeRefRetireYear','NukeEarlyRetireYear','Nuke60RetireYear','Nuke80RetireYear']
    
    # For plants with upgraded capacity, add the capacity difference as a new plant
    for upgrade in range(0,len(plant_upgrades),1):
        for ret_col in ret_cols:
            nems.loc[plant_upgrades.loc[upgrade,'UID_retire'],ret_col] = nems.loc[plant_upgrades.loc[upgrade,'UID_refurb'],ret_col]
        cap_dif = nems.loc[plant_upgrades.loc[upgrade,'UID_refurb'],'summer_power_capacity_MW'] - nems.loc[plant_upgrades.loc[upgrade,'UID_retire'],'summer_power_capacity_MW']
        nems.loc[plant_upgrades.loc[upgrade,'UID_refurb'],'summer_power_capacity_MW'] = cap_dif
        
    remove_plants = plant_refurbs['UID_retire'].tolist() + intermediate_ids
    
    nems = nems[~nems['Unique ID'].isin(remove_plants)]
        
    return nems
