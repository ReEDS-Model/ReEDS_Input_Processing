import pandas as pd
import numpy as np
import pyomo.environ as pyo
import matplotlib.pyplot as plt
import matplotlib as mpl
import argparse
import os
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import geopandas as gpd
from shapely.geometry import LineString

import argparse

## intaking all arguments
parser = argparse.ArgumentParser()
parser.add_argument('--reeds')
parser.add_argument('--reg', default = 'MT')
parser.add_argument('--type', default = 'state')
parser.add_argument('--period_type', default= 'stress')
parser.add_argument('--hr_sample', type = int, default= 1)
parser.add_argument('--yr', type = int, default= 2024)
parser.add_argument('--PRM', type = float, default= 12)
parser.add_argument('--OS', type = float, default= 0) # enforce over supply rules, if specified, it is the percent of capacity
parser.add_argument('--importname',default = "Initial_NARIS2024")
parser.add_argument('--exportname',default = "transmission_capacity_init_AC_county_NARISplus2024")
args = parser.parse_args()

#import base files for the problem 
## If the ReEDS directories or naming conversions change, these will need adjustment
county_limits = pd.read_csv(f"{args.importname}.csv")

ba_limits = pd.read_csv(os.path.join(args.reeds,
        "inputs","transmission","transmission_capacity_init_AC_ba_NARIS2024.csv"))
transgrp_limits = pd.read_csv(os.path.join(args.reeds,
        "inputs","transmission","transmission_capacity_init_AC_transgrp_NARIS2024.csv"))
hierarchy = pd.read_csv(os.path.join(args.reeds,
        "inputs","hierarchy.csv"))
county = pd.read_csv(os.path.join(args.reeds,
        "inputs","county2zone.csv"))
NEMS = pd.read_csv(os.path.join(args.reeds,
        "inputs","capacity_exogenous","ReEDS_generator_database_final_EIA-NEMS.csv"))
T_costs = pd.read_csv(os.path.join(args.reeds,
        "inputs","transmission","transmission_distance_cost_500kVac_county.csv"))
County_shps = args.reeds.spatial.get_map('county', source='tiger')
County_shps['FIPS'] = County_shps.index.values
County_shps['rb'] = 'p' + County_shps['FIPS']
state_fips = pd.read_csv(
    os.path.join(args.reeds, 'inputs', 'shapefiles', 'state_fips_codes.csv'),
    dtype={'state_fips': str},
    index_col='state_fips',
).rename(columns={'state':'STATE', 'state_code':'STCODE'})[['STATE', 'STCODE']]
County_shps = County_shps.merge(state_fips, left_on='STATEFP', right_index=True, how='left')

# These files are currently pre-defined based upon the USA defaults scenario on ReEDS main 
# as of August 2025. This is useful for adjusting the entirety of CONUS, but they can be 
# replaced by specific regions or with other stress periods. 
loads = pd.read_csv("load_allyear.csv")
Avail_all = pd.read_csv("avail_allhours.csv")

county_limits_updated = county_limits.copy()
stress = False
if args.period_type == 'stress':
    stress = True

def Run_model(CountyLimAdjusted, NEMS, loads, Avail_all,Type = args.type, Region = args.reg.split('.'),
    OverSupply = args.OS, potential = False, potential_strict = True, av_met = 'min',
    addt_PRM = args.PRM, Export_Lines = True, Update_lines = True,
    maxv = 1500, maxv2 = 600, figure_ratio = 3, stress_only = stress, hr_n = int(args.hr_sample)):

    print('Regions:', args.reg.split('.'))

    loads.reset_index(drop = False, inplace = True)
    Region_or = args.reg
    Type_or = Type

    

    if (Type == 'state') or (Type == 'ba'):
        FIPS = [f'p{x}' if len(str(x)) ==5 else f'p0{x}' for x in county.loc[county[Type].isin(Region),'FIPS']]
    else:
        bas = hierarchy.loc[hierarchy[Type].isin(Region),'ba'].tolist()
        Region = bas
        Type = 'ba'
        FIPS = [f'p{x}' if len(str(x)) ==5 else f'p0{x}' for x in county.loc[county['ba'].isin(bas),'FIPS']]
    print('FIPS being evaluated:',FIPS)

    # Get NEMS, Avails, and loads within study area and at the specified year
    NEMS = NEMS.loc[NEMS.FIPS.isin(FIPS)]

    Avail = Avail_all.loc[Avail_all.r.isin(FIPS)]
    Avail.rename(columns = {'allh':'h'}, inplace = True)
    
    loads = loads.loc[loads.r.isin(FIPS)]
    loads.rename(columns = {'allh':'h','allt':'t','MW':'Value'}, inplace = True)
    loads = loads.loc[loads.t == args.yr]
    loads['load'] = [float(x)*1+(addt_PRM/100) for x in  loads.Value]
    
    tl = [] # technology lower case without numbers for mapping techs
    for x in Avail.i.unique():
        tsplit = x.split('_')
        if 'battery' in x:
            tl.append(x)
        elif len(tsplit)>1:
            tl.append(('_').join(tsplit[:-1]))
        else:
            tl.append(x)
    tld = dict(zip(Avail.i.unique(),tl))

    # rename the availabilities with a common name
    Avail.i = [tld[x].lower() for x in Avail.i]
    Avail_i = pd.pivot_table(data = Avail.loc[Avail.r.isin(FIPS)], index = ['i','r','h'], values = 'Value',aggfunc = av_met)
    
    CAP = pd.pivot_table(data = NEMS.loc[(NEMS.RetireYear>2024) * (NEMS.StartYear<2025) ], index = ['tech','FIPS'], values = ['cap'], aggfunc = 'sum')

    # Get available capacity dataframe
    AVCAP = pd.merge(left = Avail_i.reset_index(drop = False), right = CAP.reset_index(drop = False), left_on = ['i','r'], right_on = ['tech','FIPS'], how = 'outer')
    # fill in missing gaps with an assumed value (is not used in most cases)
    AVCAP.Value.fillna(0.75, inplace = True)
    AVCAP['AvCap'] = AVCAP.Value*AVCAP.cap

    # Create table for thermal/spinning generators
    # These tables can be handled differently later (and were initiially tested to be able to have 
    # different required generation levels as well)
    NEMS_cap_S = pd.pivot_table(data = AVCAP.loc[
        AVCAP.tech.isin(['o-g-s','biopower', 'coalolduns', 'coaloldscr', 'gas-cc', 'gas-ct',  'coal-new',
        'coal-igcc','hydED', 'hydEND', 'hydNPND', 'hydND', 'nuclear','geohydro_allkm','csp-ns', 'lfill-gas'])],
        index = ['FIPS','h'], values = ['AvCap'], aggfunc = 'sum')
    NEMS_cap_S.reset_index(drop = False,inplace=True)
    NEMS_cap_S.rename(columns = {'AvCap':'S','FIPS':'r'}, inplace = True)

    # Create table for renewables and for storage
    NEMS_cap_R = pd.pivot_table(data = AVCAP.loc[
        AVCAP.tech.isin(['wind-ofs','wind-ons','dupv', 'battery_2','battery_4','pumped-hydro', 'upv', 
        'pvb'])], index = ['FIPS','h'], values = ['AvCap'], aggfunc = 'sum')
    NEMS_cap_R.reset_index(drop = False,inplace=True)
    NEMS_cap_R.rename(columns = {'AvCap':'R','FIPS':'r'}, inplace = True)

    # Create a unified table of capacities for these two categories
    Cap = pd.DataFrame()
    for r in FIPS:
        Temp = pd.DataFrame()
        Temp['h'] = AVCAP.h.unique()
        Temp['r'] = r
        Cap = pd.concat((Cap,Temp))

    Cap = pd.merge(left = Cap,right = NEMS_cap_S, on = ['r','h'], how = 'outer')
    Cap = pd.merge(left = Cap,right = NEMS_cap_R, on = ['r','h'], how = 'outer')

    Cap = Cap[['r','h','S','R']].fillna(0)
    Cap.set_index('r',inplace = True)
    Cap = Cap.loc[Cap.h !=0]
    Cap = Cap.loc[Cap.h.isin(loads.h.unique())]

    # only accept loads for the cases in which there is capacity associated.
    loads = loads.loc[loads.h.isin(Cap.h.unique())]
    loads.set_index('r',inplace = True)

    # downscale the transmission costs for just those in the study area and 
    # make a lookup table of costs for r to rr costs
    tran_cost = pd.DataFrame(index=FIPS,columns=FIPS)
    tran_cost.fillna(0,inplace = True)
    for index,row in T_costs.loc[T_costs.r.isin(FIPS)*T_costs.rr.isin(FIPS)].iterrows():
        tran_cost.at[row['r'],row['rr']]+= row['USD2004perMW']

    # Make a copy of the the county limits file to run the process on
    CountyLimWorking = CountyLimAdjusted.loc[CountyLimAdjusted.r.isin(FIPS)*CountyLimAdjusted.rr.isin(FIPS)]
    tran_caps = pd.DataFrame(index=FIPS,columns=FIPS)
    tran_caps.fillna(0,inplace = True)
    for index,row in CountyLimWorking.iterrows():
        tran_caps.at[row['r'],row['rr']]+= row['MW_f0']
        tran_caps.at[row['rr'],row['r']]+= row['MW_r0']

    # Map the FIPS to BA, used if the BA-BA capacity constraints are active
    # To enable, uncomment that constraint
    BA_map = {x:county.loc[county['ba'] == x,'FIPS'].tolist() for x in county.loc[county[Type].isin(Region),'ba'].unique()}

    bal = pd.DataFrame(index=BA_map.keys(),columns=BA_map.keys())
    bal.fillna(0,inplace = True)
    for index,row in ba_limits.loc[
            ba_limits.r.isin(BA_map.keys())*
            ba_limits.rr.isin(BA_map.keys())].iterrows():
        bal.at[row['r'],row['rr']]+= row['MW_f0']
        bal.at[row['rr'],row['r']]+= row['MW_r0']

    hs_load = (pd.pivot_table(
        data = loads, index = 'r', columns = 'h', values = 't', aggfunc = 'count')
        ).dropna(axis = 1, how = 'any').columns.tolist()
    hs_base = list(Cap.h.unique())


    # Just aligning  hours due to some early development issues...
    hs = []
    for h in hs_base:
        if h in hs_load:
            if stress_only:
                if 'sy' in h:
                    hs.append(h)
            else:
                if 'sy' not in h:
                    hs.append(h)
    
    # Down sample the hours to test based upon the input argument
    # useful for running quick tests, but good to avoid for production runs
    hs = hs[::hr_n]
    print('Total hours: ',len(hs))

    print('starting Initialization')

    model = pyo.ConcreteModel()

    # Sets
    model.r = pyo.Set(initialize = np.array([int(x[1:]) for x in FIPS]))
    model.rr = pyo.Set(initialize = model.r)
    model.ba = pyo.Set(initialize = np.array(list(BA_map.keys())))
    model.baa = pyo.Set(initialize = model.ba)
    model.h = pyo.Set(initialize = np.array(hs))

    # Parameters
    model.pTrans = pyo.Param(model.r, model.rr, 
                initialize = {(int(r[1:]),int(rr[1:])):float(tran_caps.at[r,rr]) for r in FIPS for rr in FIPS})
    model.pTC = pyo.Param(model.r, model.rr, 
                initialize = {(int(r[1:]),int(rr[1:])):float(tran_cost.at[r,rr]) for r in FIPS for rr in FIPS},
                mutable = True)           
    model.pLoad = pyo.Param(model.r,model.h, initialize = {(int(r[1:]),h):float(loads.loc[loads.h == h].at[r,'load']) for r in FIPS for h in model.h})
    model.pCap_s = pyo.Param(model.r,model.h, initialize = {(int(r[1:]),h):float(Cap.loc[Cap.h == h].at[r,'S']) for r in FIPS for h in model.h})
    model.pCap_r = pyo.Param(model.r,model.h, initialize = {(int(r[1:]),h):float(Cap.loc[Cap.h == h].at[r,'R']) for r in FIPS for h in model.h})

    model.pBA_limits = pyo.Param(model.ba,model.baa, initialize = {
        (ba,baa):float(bal.at[ba,baa]) for ba in model.ba for baa in model.baa})

    # Variables
    model.vTranGrowth = pyo.Var(model.r, model.rr, domain = pyo.NonNegativeReals) # MW -- additional transmission capacity
    for r in model.r:
        for rr in model.rr:
            r1 = f'p{r}' if len(str(r)) ==5 else f'p0{r}'
            r2 = f'p{rr}' if len(str(rr)) ==5 else f'p0{rr}'
            if potential:
                if tran_cost.at[r1,r2] == 0:
                    model.vTranGrowth[r,rr].fix(0.0)
            elif potential_strict:
                if tran_cost.at[r1,r2] == 0:
                    model.vTranGrowth[r,rr].fix(0.0)
                if model.pTrans[r,rr] == 0:
                    model.pTC[r,rr] = model.pTC[r,rr] * 10000
            else:
                if model.pTrans[r,rr] == 0:
                    model.vTranGrowth[r,rr].fix(0.0)   



    model.vFlow = pyo.Var(model.r, model.rr, model.h, domain = pyo.NonNegativeReals) # MW -- flow between counties r to rr
    # due to the matrix structure is all positive flow

    model.vGen_s = pyo.Var(model.r, model.h, domain = pyo.NonNegativeReals) # MW -- spinning generation in r at time h
    model.vGen_r = pyo.Var(model.r, model.h, domain = pyo.NonNegativeReals) # MW -- renewable/storage generation in r at time h
    # This model neglects the storage SOC and charging as it is really just looking at the upper limit potential outputs
    # this assumption will mean that storage essentially can output at the capacity at all hours
    # to change this, there could be linking between timeslices or a general CF applied the storage, but this method 
    # is likely to minimize the model desire to build more transmission as it will reduce demand for charging.

    
    print('building Objective')
    # Objective
    def MinTransCosts(model):
        return sum(sum(model.vTranGrowth[r,rr]*model.pTC[r,rr] for r in model.r) for rr in model.rr) 
    model.obj = pyo.Objective(rule = MinTransCosts)
    
    print('starting Constraints')
    # Constraints
    #All load must be served
    def SupplyDemand(model,r,h):
        return model.pLoad[r,h] <= (sum(model.vFlow[rr,r,h] for rr in model.rr)
                                 - sum(model.vFlow[r,rr,h] for rr in model.rr) 
                                 + model.vGen_s[r,h]+model.vGen_r[r,h])
    model.SupplyDemand= pyo.Constraint(model.r, model.h, rule = SupplyDemand)

    # Flow cannot exceed the existing plus new transmission capacity
    def TransmissionCap(model,r,rr,h):
        return model.vFlow[r,rr,h] <= model.pTrans[r,rr] +  model.vTranGrowth[r,rr]
    model.TransmissionCap= pyo.Constraint(model.r, model.rr, model.h, rule = TransmissionCap)

    # If running for oversupply, there must be enough transmission capacity to export the power spinning 
    # generators at the highest availability hour, not total generation or nameplate capacity necessarily. 
    if OverSupply != 0:
        def OverCap(model,r,h):
            return model.pCap_s[r,h]*OverSupply <= sum(model.pTrans[r,rr] + model.vTranGrowth[r,rr] for rr in model.rr)
        model.OverCap= pyo.Constraint(model.r, model.h, rule = OverCap)

    # Generation cannot exceed available capacity
    def GenLimitT_high(model,r,h):
        return  model.pCap_s[r,h] >= model.vGen_s[r,h]
    model.GenLimitT_high= pyo.Constraint(model.r, model.h, rule = GenLimitT_high)

    def GenLimitR(model,r,h):
        return  model.pCap_r[r,h] >= model.vGen_r[r,h]
    model.GenLimitR= pyo.Constraint(model.r, model.h, rule = GenLimitR)
    
    '''
    # transmission between counties within BAs should not exceed the BA-BA capacity
    # This constraint doesn't need to be enforced due to the ITLGRP limits in ReEDS
    # This constraint will slow down the solve significantly (or just make it infeasible)
    def BAlimits(model, ba, baa): 
        if ba != baa:
            terms = [
                model.vFlow[r, rr]
                for r in BA_map[ba]
                for rr in BA_map[baa]
                if (r, rr) in model.vFlow
            ]
            if terms:
                return sum(terms) <= model.pBA_limits[ba, baa]
            else:
                return pyo.Constraint.Skip
        return pyo.Constraint.Skip
    model.BAlimits= pyo.Constraint(model.ba, model.baa, rule = BAlimits)
    '''

    print('starting Optimization')
    solver = pyo.SolverFactory('gurobi')
    results = solver.solve(model, tee = True)
    print(f'Solve status:{results.solver.status}, termination condition: {results.solver.termination_condition}')

    # Get transmission expansion
    vTG = pd.DataFrame(np.array([[round(model.vTranGrowth[r,rr].value,4) for r in model.r]  for rr in model.rr] ))

    CountyLimAdjusted_2 = CountyLimAdjusted.copy(deep = True)

    vTG.index = FIPS
    vTG.columns = FIPS
    vTG_reduced = vTG.replace(0,np.nan).dropna(how = 'all').dropna(axis = 1,how = 'all').replace(np.nan,'-')

    # format the output changes, put in the full input county dataset and print any new build lines. 
    for r in vTG_reduced.columns:
        for rr in vTG_reduced.index:
            if vTG_reduced.at[rr,r] != '-':
                if CountyLimAdjusted_2.loc[(CountyLimAdjusted_2.r==r)*(CountyLimAdjusted_2.rr==rr)].shape[0] >0:
                    CountyLimAdjusted_2.loc[(CountyLimAdjusted_2.r==r)*(CountyLimAdjusted_2.rr==rr),['MW_f0','MW_r0']]+=vTG_reduced.at[rr,r]
                elif CountyLimAdjusted_2.loc[(CountyLimAdjusted_2.r==rr)*(CountyLimAdjusted_2.rr==r)].shape[0] >0:
                    CountyLimAdjusted_2.loc[(CountyLimAdjusted_2.r==rr)*(CountyLimAdjusted_2.rr==r),['MW_f0','MW_r0']]+=vTG_reduced.at[rr,r]
                else:
                    new_row = {'interface':f'{r}||{rr}','r':r,'rr':rr,'MW_f0':vTG_reduced.at[rr,r],'MW_r0':vTG_reduced.at[rr,r],'MW_f1':0,'MW_r1':0}
                    print(new_row)
                    CountyLimAdjusted_2 = pd.concat((CountyLimAdjusted_2,pd.DataFrame(new_row,index = [0])), ignore_index= True)
                    
    print(vTG_reduced)
    #outputs the adjustments only in a clunky named file
    vTG_reduced.to_csv(f'New_capacity_{Region_or}_P-{potential}_O-{OverSupply}_avMet-{av_met}_PRM{addt_PRM}.csv')

    # If desired, will change the base coutny transmission to pass to the next iteration for example
    if Update_lines:
        CountyLimAdjusted = CountyLimAdjusted_2

    # If desired, will export the new transmission map
    if Export_Lines:
        CountyLimAdjusted_2.to_csv(f"{args.exportname}.csv", index=False)
        
    ## Graphing 
    print('graphing start')
    t = 'rb' if Type == 'ba' else 'STCODE'
    if t == 'rb':
        Region = FIPS
    Cap['total'] = Cap['S'] + Cap.R

    County_shps_f = County_shps.loc[County_shps[t].isin(Region)]
    County_shps_f = pd.merge(left = County_shps_f,right = Cap, left_on='rb',right_index= True,how = 'outer')
    County_shps_f = pd.merge(left = County_shps_f,right = loads.reset_index(drop = False), left_on=['rb','h'],right_on = ['r','h'],how = 'outer')
    County_shps_f


    CSF = pd.merge(left = County_shps_f, right = pd.DataFrame(vTG.T.sum()), left_on = 'rb',right_index=True, how = 'outer')
    Old = pd.merge(left = County_shps_f, right = pd.DataFrame(tran_caps.sum()), left_on = 'rb',right_index=True, how = 'outer')
    Old.rename(columns={0:'ExTran'},inplace= True)
    CSF.rename(columns={0:'NewTran'},inplace= True)
    Old['Shortfall'] = Old['load'].astype(float)-(Old['S'] + Old['R'] + Old['ExTran'])
    Old_reduced = Old.loc[Old.h.isin(hs)]
    Old_reduced = Old_reduced.sort_values(by = 'Shortfall', ascending=True).drop_duplicates(subset = 'rb', ignore_index = True, keep = 'last')
    Old_reduced[['rb','FIPS','NAME','NAMELSAD','COUNTYFP','STATE','STCODE','STATEFP','COUNTRY','h','S','R','total','r','index','t','Value','load','ExTran','Shortfall']].to_csv('Old_reduceduced_forPlotting.csv')

    County_shps_f['pt'] = County_shps_f.geometry.centroid
    LINES = pd.DataFrame(index = CountyLimWorking.interface,columns = ['a','b'])
    LINES['r'] = CountyLimWorking.r.tolist()
    LINES['rr'] = CountyLimWorking.rr.tolist()
    LINES['fLC'] = CountyLimWorking.MW_f0.tolist()
    LINES['rLC'] = CountyLimWorking.MW_r0.tolist()
    for index,row in LINES.iterrows():
        r1 = County_shps_f.loc[County_shps_f.rb == row['r']]
        r2 = County_shps_f.loc[County_shps_f.rb == row['rr']]
        LINES.at[index,'a'] = r1['pt'].tolist()[0]
        LINES.at[index,'b'] = r2['pt'].tolist()[0]
    LINES['lines'] = LINES.apply(lambda row: LineString([row['a'], row['b']]), axis=1)
    LINES = gpd.GeoDataFrame(LINES,geometry='lines')
    LINES

    CountyLimAdjusted_2_f = CountyLimAdjusted_2.loc[CountyLimAdjusted_2.r.isin(FIPS)*CountyLimAdjusted_2.rr.isin(FIPS)]
    LINES2 = pd.DataFrame(index = CountyLimAdjusted_2_f.interface,columns = ['a','b'])
    LINES2['r'] = CountyLimAdjusted_2_f.r.tolist()
    LINES2['rr'] = CountyLimAdjusted_2_f.rr.tolist()
    LINES2['fLC'] = CountyLimAdjusted_2_f.MW_f0.tolist()
    LINES2['rLC'] = CountyLimAdjusted_2_f.MW_r0.tolist()
    for index,row in LINES2.iterrows():
        r1 = County_shps_f.loc[County_shps_f.rb == row['r']]
        r2 = County_shps_f.loc[County_shps_f.rb == row['rr']]
        LINES2.at[index,'a'] = r1['pt'].tolist()[0]
        LINES2.at[index,'b'] = r2['pt'].tolist()[0]
    LINES2['lines2'] = LINES2.apply(lambda row: LineString([row['a'], row['b']]), axis=1)
    LINES2 = gpd.GeoDataFrame(LINES2,geometry='lines2')
    LINES2



    cs = 'viridis'
    cmap = mpl.colormaps[cs]
    fs = 32
    f, (ax1,ax2) = plt.subplots(1, 2, sharey=True, figsize = (fs,fs/figure_ratio))


    Old_reduced.plot(ax = ax1, edgecolor = 'grey', color = 'white', zorder = 0)
    Old_reduced['load'] = [float(x) for x in Old_reduced.load]
    Old_reduced.plot(ax = ax1,column = 'load', cmap = cs, legend=True, vmax = maxv, zorder = 0, alpha = 0.5,legend_kwds={
        "shrink": 0.5,"label":'County Demand, Capacity (point fill),\n Transmission Capacity [MW]'})

    lcolors = [cmap(x/maxv) for x in LINES['fLC']]
    LINES.plot(ax = ax1, edgecolors = '0', color = lcolors, zorder = 1)
    colors =  [cmap(x/maxv) for x in Old_reduced['S']]
    Old_reduced.geometry.centroid.plot(ax = ax1, edgecolors = '0', color = colors, zorder = 2, alpha = 0.5)



    mask = [x>0 for x in Old_reduced.Shortfall]
    alphas = [1 if x else 0 for x in mask]
    Old_reduced.plot(ax = ax2, edgecolor = 'grey', color = 'white', zorder = 0)
    Old_reduced.plot(ax = ax2,column = 'Shortfall', cmap = cs, legend=True, vmax = maxv2, vmin = 0, alpha = alphas, zorder = 0,legend_kwds={
        "shrink": 0.5,"label":'Capacity Shortfall and New Transmission [MW]'})
    new_caps = list(LINES2.loc[LINES2.index.isin(LINES.index),'fLC']-LINES.fLC)

    linetypes = ['solid']*len(new_caps)
    new_lines = LINES2.loc[~LINES2.index.isin(LINES.index)]
    #new_caps =  new_caps + new_lines.fLC.tolist()
    #linetypes += ['dotted']*len(new_lines.fLC.tolist())
    mask = [x>0 for x in new_caps]
    lcolors = [cmap(x/maxv2) for x in new_caps]
    alphas = [1 if x else 0 for x in mask]
    LINES2.iloc[:LINES.shape[0]].plot(ax = ax2, color = lcolors, alpha = alphas, zorder = 1, linestyle = 'solid')

    new_lines = LINES2.loc[~LINES2.index.isin(LINES.index)]
    new_caps2 = new_lines.fLC.tolist()
    mask = [x>0 for x in new_caps2]
    lcolors = [cmap(x/maxv2) for x in new_caps2]
    alphas = [1 if x else 0 for x in mask]
    LINES2.iloc[LINES.shape[0]:].plot(ax = ax2, color = lcolors, alpha = alphas, zorder = 1, linestyle = (0,(1,1)))
    colors =  [cmap(x/maxv2) for x in Old_reduced['S']]
    Old_reduced.geometry.centroid.plot(ax = ax2, edgecolors = '0', color = colors, zorder = 2)
    ax1.axis('off')
    ax2.axis('off')

    f.savefig(f'Quickmap_{Region_or}_P-{potential}_O-{OverSupply}_avMet-{av_met}_PRM{addt_PRM}.png')
    return CountyLimAdjusted,vTG_reduced

CL,VTG = Run_model(county_limits,NEMS,loads,Avail_all)
