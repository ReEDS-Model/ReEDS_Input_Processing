#### This file takes the Canadian reported imports and exports and outputs the annual total
#### imports/exports as well as the seasonal fractions used by ReEDS with a 
#### flexible 'hydro-like' operation. 

import pandas as pd
import os

Canada_Historical_FN = 'electricity-trade-summary-resume-echanges-commerciaux-electricite.xlsx' # From https://www.cer-rec.gc.ca/en/data-analysis/energy-commodities/electricity/statistics/electricity-trade-summary/index.html
Monthly_Sheet = 'Fig. 1(m), Fig. 3(m)'

Canada_Projection_FN = 'Electricity_Interchange.xlsx' # From https://apps.cer-rec.gc.ca/ftrppndc/dflt.aspx?GoCTemplateCulture=en-CA

# Ensure the headers for the provinces match the excel file
Province_header_dict = {'Newfoundland and Labrador':6,
                        'Prince Edward Island':24,
                        'Nova Scotia':33,
                        'New Brunswick':42,
                        'Quebec': 51,
                        'Ontario': 60,
                        'Manitoba': 69,
                        'Alberta': 78,
                        'British Columbia': 87,
                        'Saskatchewan': 96}

########################################
### Seasonal Fraction file creation ####
########################################

Data_directory = 'data'
Output_folder = 'outputs'
if not os.path.exists(Output_folder):
    os.makedirs(Output_folder)

Season_dict = {'summ': [6,7,8],
               'fall': [9,10,11],
               'wint': [12,1,2],
               'spri': [3,4,5]}

Historical = pd.read_excel(os.path.join(Data_directory, 'raw_inputs', Canada_Historical_FN), sheet_name = Monthly_Sheet)
Historical.columns = ['Date','Exports_MWh','Imports_MWh','Exports_$','Imports_$']  # Ensure this order and content is correct for current year.
Historical['Year'] = Historical.Date.dt.year
Historical['Month'] = Historical.Date.dt.month
for s in Season_dict.keys():
    Historical.loc[Historical.Month.isin(Season_dict[s]),'Season'] = s
    
# removing partial years
for y in Historical.Year.unique():
    if len(Historical.loc[Historical.Year == y,'Month'])<12:
        Historical.loc[Historical.Year == y,'Full_Year'] = False
    else:
        Historical.loc[Historical.Year == y,'Full_Year'] = True
Historical = Historical.loc[Historical.Full_Year]

# Getting the annual balnces and the monthly portions of this
for y in Historical.Year.unique():
    Historical.loc[Historical.Year == y,'Annual_Export_MWh'] = Historical.loc[Historical.Year == y,'Exports_MWh'].sum()
    Historical.loc[Historical.Year == y,'Annual_Import_MWh'] = Historical.loc[Historical.Year == y,'Imports_MWh'].sum()


Historical['Export_Month_Fraction'] = Historical.Exports_MWh/Historical.Annual_Export_MWh
Historical['Import_Month_Fraction'] = Historical.Imports_MWh/Historical.Annual_Import_MWh


# generating the Imports season fractions from available years
Import_Season_Fractions = pd.DataFrame(pd.pivot_table(Historical, values = ['Import_Month_Fraction'], index = 'Season',  aggfunc = 'sum'))
Import_Season_Fractions/=len(Historical.Year.unique())
Import_Season_Fractions.reset_index(inplace = True, drop = False)
order_dict = dict(zip(Season_dict.keys(),range(4)))
Import_Season_Fractions['order'] = [order_dict[x] for x in Import_Season_Fractions.Season]
Import_Season_Fractions.sort_values(by = 'order',inplace = True)
Import_Season_Fractions.drop('order',axis = 1, inplace = True)
Import_Season_Fractions.columns = ['*szn','frac']
Import_Season_Fractions.round(4).to_csv(os.path.join(Output_folder, 'can_imports_quarter_frac.csv'), index = False)

# generating the Exports season fractions from available years
Export_Season_Fractions = pd.DataFrame(pd.pivot_table(Historical, values = ['Export_Month_Fraction'], index = 'Season',  aggfunc = 'sum'))
Export_Season_Fractions/=len(Historical.Year.unique())
Export_Season_Fractions.reset_index(inplace = True, drop = False)
order_dict = dict(zip(Season_dict.keys(),range(4)))
Export_Season_Fractions['order'] = [order_dict[x] for x in Export_Season_Fractions.Season]
Export_Season_Fractions.sort_values(by = 'order',inplace = True)
Export_Season_Fractions.drop('order',axis = 1, inplace = True)
Export_Season_Fractions.columns = ['*szn','frac']
Export_Season_Fractions.round(4).to_csv(os.path.join(Output_folder, 'can_exports_szn_frac.csv'), index = False)


########################################
##### Annual totals file creation ######
########################################

Province_to_county_map = pd.read_csv(os.path.join(Data_directory, 'province_to_county_map.csv'))
Province_to_county_map.fillna(0, inplace = True)


Province_I_E = pd.DataFrame()
for p in Province_header_dict.keys():
    header = Province_header_dict[p]
    
    Projection = pd.read_excel(os.path.join(Data_directory, 'raw_inputs', Canada_Projection_FN), header = header, nrows = 6, index_col = 0)
    Projection*=1000 # Converting from GWh to MWh
    Projection = Projection.T
    Projection = Projection[['Exports','Imports']]
    Projection.columns = ['Imports','Exports'] # Reversed to be US centered
    Projection['Province'] = p
    Projection.reset_index(inplace = True, names = 'Year')
    Province_I_E = pd.concat((Province_I_E,Projection))
    
Province_I_E.reset_index(inplace = True, drop = True)
Province_I_E.Year = Province_I_E.Year.astype(int)
Province_I_E = Province_I_E.loc[Province_I_E.Year >= 2010]

Province_I_E_county = pd.merge(Province_I_E,Province_to_county_map, on  = 'Province')


### Exports from US

Province_I_E_county_export = Province_I_E_county.copy(deep = True)
counties = Province_to_county_map.columns.tolist()
counties.remove('Province')

for county in counties:
    Province_I_E_county_export[county]*=Province_I_E_county_export.Exports

Exports = pd.DataFrame(pd.pivot_table(Province_I_E_county_export, values = counties,index = 'Year', aggfunc = 'sum').T)
Exports.reset_index(inplace = True, drop = False, names = 'r')
order_dict = dict(zip(counties,range(len(counties))))
Exports['order'] = [order_dict[x] for x in Exports.r]
Exports.sort_values(by = 'order',inplace = True)
Exports.drop('order',axis = 1, inplace = True)
Exports.set_index('r', inplace = True)

Exports.round(1).to_csv(os.path.join(Output_folder, 'can_exports.csv'))


### Imports from US

Province_I_E_county_import = Province_I_E_county.copy(deep = True)
counties = Province_to_county_map.columns.tolist()
counties.remove('Province')

for county in counties:
    Province_I_E_county_import[county]*=Province_I_E_county_import.Imports

Imports = pd.DataFrame(pd.pivot_table(Province_I_E_county_import, values = counties,index = 'Year', aggfunc = 'sum').T)
Imports.reset_index(inplace = True, drop = False, names = 'r')
order_dict = dict(zip(counties,range(len(counties))))
Imports['order'] = [order_dict[x] for x in Imports.r]
Imports.sort_values(by = 'order',inplace = True)
Imports.drop('order',axis = 1, inplace = True)
Imports.set_index('r', inplace = True)

Imports.round(1).to_csv(os.path.join(Output_folder, 'can_imports.csv'))

print(f"Run complete. See outputs folder for outputs which are inputs to ReEDS.")