"""
Updated August 19 2026

This sub-script handles upgraded units
"""

import pandas as pd
import sys

def fix_upgrades(nems):

    # Note that T_SYR (now StartYear) is the online year for the most recent time the unit
    # came online. TRFURB holds the original start date of the plant.
    
    # TVIN == 6 means that the unit was retired and 7 corresponds to a unit that was refurbished
    upgrate_cap = ((nems['TVIN'] == 6) & (nems['RetireYear'] >= 2009)) | ((nems['TVIN'] == 7) & (nems['StartYear'] >= 2010))
    upgrades = nems[upgrate_cap]
    non_upgrades = nems[~upgrate_cap]

    # Make sure units before they retire
    upgrades = upgrades[upgrades['TRFURB']<=upgrades['RetireYear']]

    # Handle upgraded units
    upgrades_unique = upgrades.drop_duplicates(subset=['T_PID', 'T_UID'])
    for idx in upgrades_unique.index:
        idx_cat = (upgrades['T_PID']==upgrades_unique['T_PID'][idx]) & (upgrades['T_UID']==upgrades_unique['T_UID'][idx])
        unit_idx = upgrades[idx_cat]
        if len(unit_idx) == 1:
            continue
        elif len(unit_idx) == 2:
            # Get the index id for retired and upgraded plant
            retire_id = unit_idx[unit_idx['StartYear']==unit_idx['StartYear'].min()].index.values[0]
            upgrade_id = unit_idx[unit_idx['StartYear']==unit_idx['StartYear'].max()].index.values[0]

            # Get start years, retire years and capacity values of the retired and upgraded plants
            # Retired plant
            startyear_ret = unit_idx.loc[unit_idx.index==retire_id, 'StartYear'].values[0]
            retireyear_ret = unit_idx.loc[unit_idx.index==retire_id, 'RetireYear'].values[0]
            cap_ret = unit_idx.loc[unit_idx.index==retire_id, 'summer_power_capacity_MW'].values[0]
            # Upgraded plant
            startyear_ref = unit_idx.loc[unit_idx.index==upgrade_id, 'StartYear'].values[0]
            retireyear_ref = unit_idx.loc[unit_idx.index==upgrade_id, 'RetireYear'].values[0]
            cap_ref = unit_idx.loc[unit_idx.index==upgrade_id, 'summer_power_capacity_MW'].values[0]

            # If the upgraded plant's start year does not immediately follow the 
            # retired plant's retire year, keep both plants in the database
            if startyear_ref - retireyear_ret > 1:
                print('Upgraded plant does not start immediately after original plant retires, ' \
                'keep both plants in database')
                continue
            # If the upgraded plant's start year immediately follows the retired plant's retire year,
            # update start year for the upgraded plant and keep only the upgraded plant in database, 
            # unless the upgraded capacity is higher, in which case, add the capacity difference as the new plant
            elif startyear_ref - retireyear_ret == 1:
                if cap_ref - cap_ret <= 0:
                    print('No change in capacity after upgrading, or upgraded capacity is lower,' \
                          ' update start year for the upgraded plant')
                    # Update start year for the upgraded plant and drop retired plant
                    upgrades.loc[upgrades.index==upgrade_id,'StartYear'] = startyear_ret
                    upgrades = upgrades.drop(index=retire_id)
                elif cap_ref - cap_ret > 0:
                    cap_diff = cap_ref - cap_ret
                    print('Capacity increases after upgrading, add the capacity difference as the new plant')
                    # Update retire year for the retired plan
                    upgrades.loc[upgrades.index==retire_id,'RetireYear'] = retireyear_ref
                    # Add capacity difference as the new plant
                    upgrades.loc[upgrades.index==upgrade_id,'summer_power_capacity_MW'] = cap_diff
        elif len(unit_idx) > 2:
            print('There are mismatched numbers of retired and upgraded units. Please check dataset to confirm ' \
            'and adjust data clean up process in a_data_cleaning.py if needed. Exiting...')
            sys.exit()

    nems_final = pd.concat([non_upgrades,upgrades])
        
    return nems_final
