import pandas as pd

REEDS_Input = pd.ExcelFile('inputs/REEDS_Input.xlsm')

# Sw_NationalAvg_Regional = 0 for National Average, 1 for Region Specific
Sw_NationalAvg_Regional = 0

if Sw_NationalAvg_Regional == 0 :
    _skiprows, _nrows, _usecols_W, _usecols_C = 77, 61, list(range(1,208)), list(range(209,367))
    tag="NationalAvg"
elif Sw_NationalAvg_Regional == 1:
    _skiprows, _nrows, _usecols_W, _usecols_C = 279, 50, list(range(1,208)), list(range(161,319))
    tag="RegionSpecific"
    
#WaterWrateqctn - region-specific water withdrawal rate (gal/MWh)    
WaterWrateqctn=pd.read_excel(REEDS_Input, sheet_name = 'Cooling Water', skiprows = _skiprows, nrows = _nrows, usecols = _usecols_W) 
WaterWrateqctn.rename(columns = {WaterWrateqctn.columns[0]:'q', WaterWrateqctn.columns[1]: "ct"}, inplace=True)
WaterWrateqctn.drop(WaterWrateqctn[WaterWrateqctn['q'].str.contains('-NSP', case=False, regex=False)].index, inplace=True)

#WaterCrateqctn - region-specific water consumption rate (gal/MWh)
WaterCrateqctn=pd.read_excel(REEDS_Input, sheet_name = 'Cooling Water', skiprows = _skiprows, nrows =_nrows, usecols = _usecols_C)
WaterCrateqctn.columns=WaterCrateqctn.columns.str.rstrip('1').str.rstrip('.')
WaterCrateqctn.rename(columns = {WaterCrateqctn.columns[0]:'q', WaterCrateqctn.columns[1]: "ct"}, inplace=True)
WaterCrateqctn.drop(WaterCrateqctn[WaterCrateqctn['q'].str.contains('-NSP', case=False, regex=False)].index, inplace=True)

ct2ctt_mapper={'once': 'o', 'recirc':'r', 'pond':'p', 'dry':'d', 'none':'n'}

wst_encoder={'fsu','fsa', 'fsl', 'fg', 'sg', 'ss', 'ww', 'n'}

WaterWrateqctn['ct'] = WaterWrateqctn['ct'].map(ct2ctt_mapper)
WaterCrateqctn['ct'] = WaterCrateqctn['ct'].map(ct2ctt_mapper)

WaterWrateqctn.insert(2, 'Withdrawal/Consumption', 'with')

WaterCrateqctn.insert(2,'Withdrawal/Consumption', 'cons')

# handle CSP things and append it: 
WaterWCrateqctn_csp=pd.read_excel(REEDS_Input, sheet_name = 'Cooling Water', skiprows = 141, nrows = 24, usecols = list(range(1,208)))
WaterWCrateqctn_csp.rename(columns = {WaterWCrateqctn_csp.columns[0]:'q', WaterWCrateqctn_csp.columns[1]: 'ct'}, inplace=True)
WaterWCrateqctn_csp['ct'] = WaterWCrateqctn_csp['ct'].map(ct2ctt_mapper)
WaterWCrateqctn_csp.insert(2,'Withdrawal/Consumption', '')
WaterWCrateqctn_csp.loc[WaterWCrateqctn_csp['q'].str.contains('Wrate'), 'Withdrawal/Consumption'] = 'with'
WaterWCrateqctn_csp.loc[WaterWCrateqctn_csp['q'].str.contains('Crate'), 'Withdrawal/Consumption'] = 'cons'
WaterWCrateqctn_csp.drop(WaterWCrateqctn_csp[WaterWCrateqctn_csp['q'].str.contains('O', case=True)].index, inplace=True)
WaterWCrateqctn_csp.loc[~WaterWCrateqctn_csp['q'].str.contains('Stor', case=True), 'q'] = 'csp-ns'
WaterWCrateqctn_csp.loc[WaterWCrateqctn_csp['q'].str.contains('Stor', case=True), 'q'] = 'csp-ws'
WaterWCrateqctn_csp.drop(WaterWCrateqctn_csp[WaterWCrateqctn_csp['q'].str.contains('csp-ws', case=True)].index, inplace=True)


cspws = ['csp1_1*csp1_12', 'csp2_1*csp2_12', 'csp3_1*csp3_12', 'csp4_1*csp4_12']

#def expand_index(index2expand):
#    _expanded_index = list()
#    i=index2expand
#    if '*' in i:
#        _start = int(i.split('*')[0].split('_')[-1])
#        _end = int(i.split('*')[1].split('_')[-1])
#        _base = i.split('_'+ i.split('*')[0].split('_')[-1])[0]
#        for i in range(_start, _end+1):
#            _expanded_index.append(f'{_base}_{i}')
#    return _expanded_index
#
#cspws_expanded=list();
#for i in cspws:
#    cspws_expanded.extend(expand_index(i))
#    
#    
#for i in range(len(cspws_expanded)):
#    WaterWCrateqctn_csp.append(WaterWCrateqctn_csp.loc[(WaterWCrateqctn_csp.q == 'csp-ws') & (WaterWCrateqctn_csp.ct == 'o') & (WaterWCrateqctn_csp['Withdrawal/Consumption'] == 'with')], ignore_index = True)
#    WaterWCrateqctn_csp.q[11+i] = cspws_expanded[i]
#    
#WaterWCrateqctn_csp.loc[(WaterWCrateqctn_csp.q == 'csp-ws') & (WaterWCrateqctn_csp.ct == 'r') & (WaterWCrateqctn_csp['Withdrawal/Consumption'] == 'with')]
#WaterWCrateqctn_csp.loc[(WaterWCrateqctn_csp.q == 'csp-ws') & (WaterWCrateqctn_csp.ct == 'd') & (WaterWCrateqctn_csp['Withdrawal/Consumption'] == 'with')]
    
# make new i based on ctt and wst
#WaterWithConsRate_i_ctt = pd.concat([WaterCrateqctn, WaterWrateqctn], axis = 0, join='inner')

WaterWithConsRate=pd.concat([WaterWrateqctn,WaterCrateqctn, WaterWCrateqctn_csp], ignore_index = True, sort = False)
WaterWithConsRate.rename(columns = {'q':'i', 'ct':'ctt', 'Withdrawal/Consumption':'w'}, inplace=True)

if Sw_NationalAvg_Regional == 1: WaterWithConsRate = WaterWithConsRate.round(4)

WaterWithConsRate.to_csv(f"water_with_cons_rate.csv", index = False)