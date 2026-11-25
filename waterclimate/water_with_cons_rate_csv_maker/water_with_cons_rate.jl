using DataFrames
using CSV
using PyCall

pd = pyimport("pandas")

REEDS_Input = pd.ExcelFile("inputs/REEDS_Input.xlsm")

# Sw_NationalAvg_Regional = 0 for National Average, 1 for Region Specific
    Sw_NationalAvg_Regional = 0

if Sw_NationalAvg_Regional == 0
    _skiprows, _nrows, _usecols_W, _usecols_C = 77, 61, collect(1:158), collect(209:366)
    tag="NationalAvg"
elseif Sw_NationalAvg_Regional == 1
    _skiprows, _nrows, _usecols_W, _usecols_C = 279, 50, collect(1:158), collect(161:318)
    tag="RegionSpecific"
else 
    println("Select either 0 or 1!")
end

WaterWrateqctn=pd.read_excel(REEDS_Input, sheet_name = "Cooling Water", skiprows = _skiprows, nrows = _nrows, usecols = _usecols_W) 
WaterCrateqctn=pd.read_excel(REEDS_Input, sheet_name = "Cooling Water", skiprows = _skiprows, nrows =_nrows, usecols = _usecols_C)
