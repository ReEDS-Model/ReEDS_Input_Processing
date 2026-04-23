# This script processes Renewable Portfolio Standards (RPS) and Clean Energy Standards (CES) data for various states,
# the outputs include `rps_fraction.csv`, `ces_fraction.csv`, and `hydrofrac.csv` which are used in ReEDS model inputs.

import pandas as pd
import numpy as np
import os

print("...Starting data processing for RPS, CES and hydrofrac...")

### ===========================================
### ===========Load Input Data=================
### ===========================================

### Input `RPS data for NREL_June 2025.xlsx` file and convert it to a DataFrame
### If update the input file, please make sure the below table parameters are updated accordingly.
### This file is provided annually by Galen Barbose at LBNL
### ----------------------------------------------------------------------------

filename                = os.path.join("inputs", "RPS data for NREL_June 2025.xlsx")

# Statewide Load sheet as sales data and RPS & CES Demand Projections sheet for RPS and CES data
# are used for capculate `rps_fraction` and `ces_fraction`.
Salessheetname          = "Statewide Load"
Salessheet_usecols      = "B:BC"
Salessheet_skiprows     = 5
Salessheet_nrows        = 52

RPSsheetname            = "RPS & CES Demand Projections"
RPSsheet_usecols        = "A:BB"
RPSsheet_skiprows       = 2
RPSsheet_nrows          = 97

# Hydro sheet is used for hydrofrac calculation.
Hydrosheetname          = "Non-RE Accounting"

Hydrosheet_RPS_usecols  = "A:E"
Hydrosheet_RPS_skiprows = 2
Hydrosheet_RPS_nrows    = 33

Hydrosheet_CES_usecols  = "A:D"
Hydrosheet_CES_skiprows = 39
Hydrosheet_CES_nrows    = 16

### Input voluntary RPS data which is downloaded from NREL Green Power Data
### If update the input file, please make sure the below table parameters are updated accordingly.
### https://www.nlr.gov/analysis/green-power
### -----------------------------------------------------------------------------

# These data are used to calculate voluntary RPS fraction and will be appended to `rps_fraction.csv`
filename_voluntary      = os.path.join("inputs", "nrel-green-power-data-v2023.xlsx")
Voluntarysheetname      = "Marketwide Estimates"
Voluntarysheet_usecols  = "A:C"
Voluntarysheet_skiprows = 2
Voluntarysheet_nrows    = 15

### Input non-US RPS data which is provided by ReEDS team
### If update the input file, please make sure the below table parameters are updated accordingly.
### ---------------------------------------------------------------

# This file contains RPS data for non-US regions and will be used to append to `rps_fraction.csv`
# only include NS (Nova Scotia).
filename_nonUS          = os.path.join("inputs", "RPS_nonUS.csv")
nonUS_state             = "NS"

### Input `gen_ann.csv` file and `hierarchy.csv` file and convert it to a DataFrame
### These files are provided by ReEDS team using ReEDS latest Reference case results
### ----------------------------------------------------------------------------------

# These two files represent hydro power generation and are used to calculate hydrofrac.
hierarchy               = pd.read_csv('./inputs/hierarchy.csv').drop(columns=['Value'])
gen                     = pd.read_csv('./inputs/gen_ann.csv')
hydro_year              = 2023       # The year for which hydrofrac is calculated

### ===========================================
### ===========Functions=======================
### ===========================================

### Function to calculate RPS fractions
### RPS_all is `Total RPS` in the `RPS & CES Demand Projections` sheet divided by retail sales.
### RPS_solar is the sum of solar-related rows divided by retail sales.
### RPS_wind is the sum of wind-related rows divided by retail sales.
### The results are saved to `rps_fraction_intermediate.csv` in the intermediate output directory.
### -----------------------------------------------------------------------------------

def calculate_rps_fraction(main_excel_file, voluntary_file, non_us_file):

    ### Load and process retail sales data from `Statewide Load` sheet.
    ### `DC` sales are added to `MD` sales.
    df_retail_sales = pd.read_excel(main_excel_file, sheet_name=Salessheetname, usecols=Salessheet_usecols, skiprows=Salessheet_skiprows, nrows=Salessheet_nrows)
    df_retail_sales = df_retail_sales.melt(id_vars=["State"], var_name="Year", value_name="Sales")
    df_retail_sales = df_retail_sales.pivot(index="Year", columns="State", values="Sales").reset_index()
    df_retail_sales["MD"] += df_retail_sales.get("DC", 0)
    df_retail_sales = df_retail_sales.drop(columns=["DC"], errors='ignore')
    df_retail_sales = df_retail_sales.melt(id_vars=["Year"], var_name="State", value_name="Sales")
    In_Retail_Sales = df_retail_sales[df_retail_sales["State"] != "DC"]

    # State RPS calculation
    In_RPS = pd.read_excel(main_excel_file, sheet_name=RPSsheetname, usecols=RPSsheet_usecols, skiprows=RPSsheet_skiprows, nrows=RPSsheet_nrows)

    In_RPS = In_RPS.rename(
        columns={"State": "State", 
                 "Special Notes": "Notes", 
                 "RPS Tier or Carve Out": "Tier_CarveOut"}
                 )
    In_RPS["State"] = In_RPS["State"].ffill()
    In_RPS.drop(columns=["Notes"], inplace=True)
    In_RPS = In_RPS[~In_RPS["State"].isin(["HI", "Total"])]

    RPS_reshaped = In_RPS.melt(id_vars=["State", "Tier_CarveOut"], var_name="Year", value_name="Value")
    RPS_reshaped = RPS_reshaped.pivot_table(index=["State", "Year"], columns="Tier_CarveOut", values="Value").reset_index().fillna(0)
    RPS_reshaped.columns = RPS_reshaped.columns.str.strip()
    RPS_reshaped["State"] = RPS_reshaped["State"].str.strip()
    RPS_reshaped["TotalRPS_ReEDS"] = RPS_reshaped["Total RPS"]

    # Subtract specific rows from `RPS & CES Demand Projections` if those rows exist
    # These information are not used for RPS calculation
    # You might want to manually update this part if the sheet structure changes.
    adjustments = {
        "MA": ["Class II MSW"], 
        "NC": ["Swine Waste Carve-Out", "Poultry Waste Carve-Out"],
        "NH": ["Class I (Thermal)"], 
        "PA": ["Tier II"], 
        "ME": ["Thermal"]
    }
    for state, cols in adjustments.items():
        for col in cols:
            if col in RPS_reshaped.columns:
                RPS_reshaped.loc[RPS_reshaped["State"] == state, "TotalRPS_ReEDS"] -= RPS_reshaped.loc[RPS_reshaped["State"] == state, col]

    RPS_reshaped.loc[RPS_reshaped["State"] == "MD", "TotalRPS_ReEDS"] += RPS_reshaped.loc[RPS_reshaped["State"] == "DC", "TotalRPS_ReEDS"].values

    RPStarget = pd.merge(RPS_reshaped[['State', 'Year', 'TotalRPS_ReEDS']], In_Retail_Sales, on=['Year', 'State'], how='left')
    RPStarget['RPS'] = RPStarget['TotalRPS_ReEDS'] / RPStarget['Sales']
    RPStarget = RPStarget[RPStarget['State'] != "DC"]

    # Define a function to calculate solar and wind RPS fractions
    def calculate_solar_wind(RPS_reshaped, In_Retail_Sales, tech):
        if tech == "solar":
            colnames = [col for col in [
                "Class I (Solar)", "Solar Carve-Out", "New Solar Requirement (total)",
                "Class II (Solar Carve-Out)", "Solar Carve-Out (see note)"
            ] if col in RPS_reshaped.columns]
            colout = "solarcarveout"
        elif tech == "wind":
            colnames = [col for col in [
                "New Wind Requirement", "Offshore Wind Carve-Out"
            ] if col in RPS_reshaped.columns]
            colout = "windcarveout"
        else:
            raise ValueError(f"'{tech}' is not supported. Choose 'solar' or 'wind'.")

        df = RPS_reshaped[['State', 'Year'] + colnames].copy()
        df['total'] = df[colnames].sum(axis=1)

        # Merge DC into MD
        if "DC" in df['State'].values:
            dc_total = df.loc[df['State'] == "DC", 'total'].values
            df.loc[df['State'] == "MD", 'total'] += dc_total if dc_total.size > 0 else 0

        # Merge with retail sales and calculate carve-out
        df = pd.merge(df[['State', 'Year', 'total']], In_Retail_Sales, on=['Year', 'State'], how='left')
        df[colout] = df['total'] / df['Sales']
        df = df[df['State'] != "DC"]

        return df

    Solar = calculate_solar_wind(RPS_reshaped, In_Retail_Sales, "solar")
    Wind = calculate_solar_wind(RPS_reshaped, In_Retail_Sales, "wind")

    RPStarget = RPStarget[['Year', 'State', 'RPS']].merge(Solar[['Year', 'State', 'solarcarveout']], on=['Year', 'State'], how='left')
    RPStarget = RPStarget.merge(Wind[['Year', 'State', 'windcarveout']], on=['Year', 'State'], how='left')
    RPStarget.fillna(0, inplace=True)
    RPStarget.columns = ['t', 'st', 'rps_all', 'rps_solar', 'rps_wind']

    # Append voluntary RPS data
    # Use 2010-2023 data as historical data
    # and project future data until 2050 using the minimum absolute growth rate from historical data.
    voluntary_data = pd.read_excel(voluntary_file, sheet_name=Voluntarysheetname, usecols=Voluntarysheet_usecols, skiprows=Voluntarysheet_skiprows, nrows=Voluntarysheet_nrows)
    voluntary_data = voluntary_data.rename(columns={'Year': 'Year'})

    total_sales = In_Retail_Sales[In_Retail_Sales["Year"] > 2009].groupby("Year")["Sales"].sum().reset_index().rename(columns={"Sales": "US_Total_Sales_GWh"})

    voluntary_data_historical = pd.merge(voluntary_data, total_sales, on='Year', how='left')
    voluntary_data_historical['rps_all'] = (voluntary_data_historical['Sales (million MWh)'] * 1000) / voluntary_data_historical['US_Total_Sales_GWh']
    voluntary_data_historical.rename(columns={'Year': 't'}, inplace=True)
    voluntary_data_historical['st'] = 'voluntary'
    voluntary_data_historical = voluntary_data_historical[['t', 'st', 'rps_all']].assign(rps_solar=0.0, rps_wind=0.0)

    rps_series = voluntary_data_historical[['t', 'rps_all']].dropna()
    rps_series = rps_series[rps_series['t'] <= 2023].sort_values('t')
    min_growth = rps_series['rps_all'].diff().min()

    last_year = rps_series['t'].max()
    last_value = rps_series.loc[rps_series['t'] == last_year, 'rps_all'].values[0]

    future_years = list(range(last_year + 1, 2051))
    rps_future = [last_value + min_growth * i for i in range(1, len(future_years) + 1)]
    voluntary_data_projected = pd.DataFrame({'t': future_years, 'rps_all': rps_future, 'st': 'voluntary', 'rps_solar': 0.0, 'rps_wind': 0.0})

    voluntary_all = pd.concat([voluntary_data_historical, voluntary_data_projected], ignore_index=True)

    # Append non-US RPS data
    nonus_data = pd.read_csv(non_us_file)
    nonus_data = nonus_data[nonus_data['st'] == nonUS_state]

    # Combine and Save RPS Data
    final_rps = pd.concat([RPStarget, nonus_data, voluntary_all], ignore_index=True)
    final_rps = final_rps[final_rps['t'] > 2009].sort_values(by=['st', 't'])

    output_path = os.path.join("outputs", "intermediate outputs", "rps_fraction_intermediate.csv")
    final_rps.to_csv(output_path, index=False)
    print(f"...Intermediate RPS data processed and saved to {output_path}")

### Function to calculate CES fractions
### Then the total CES fraction is the sum of RPS  plus incremental CES in the `RPS & CES Demand Projections` sheet divided by retail sales.
### The results are saved to `ces_fraction_intermediate.csv` in the intermediate output directory.
### -----------------------------------------------------------------------------------

def calculate_ces_fraction(main_excel_file):

    ### Load and process retail sales data from `Statewide Load` sheet.
    ### `DC` sales are added to `MD` sales.
    df_retail_sales = pd.read_excel(main_excel_file, sheet_name=Salessheetname, usecols=Salessheet_usecols, skiprows=Salessheet_skiprows, nrows=Salessheet_nrows)
    df_retail_sales = df_retail_sales.melt(id_vars=["State"], var_name="Year", value_name="Sales")
    df_retail_sales = df_retail_sales.pivot(index="Year", columns="State", values="Sales").reset_index()
    df_retail_sales["MD"] += df_retail_sales.get("DC", 0)
    df_retail_sales = df_retail_sales.drop(columns=["DC"], errors='ignore')
    df_retail_sales = df_retail_sales.melt(id_vars=["Year"], var_name="State", value_name="Sales")
    In_Retail_Sales = df_retail_sales[df_retail_sales["State"] != "DC"]

    # CES calculation
    CES_df = pd.read_excel(main_excel_file, sheet_name=RPSsheetname, usecols=RPSsheet_usecols, skiprows=RPSsheet_skiprows, nrows=RPSsheet_nrows)

    CES_df = CES_df.rename(
        columns={"State": "State", 
                 "Special Notes": "Notes", 
                 "RPS Tier or Carve Out": "Tier_CarveOut"}
                 )
    CES_df["State"] = CES_df["State"].ffill()
    CES_df.drop(columns=["Notes"], inplace=True)

    CES_long = CES_df.melt(id_vars=["State", "Tier_CarveOut"], var_name="Year", value_name="Value")
    CES_long["Value"] = pd.to_numeric(CES_long["Value"], errors="coerce")
    CES_wide = CES_long.pivot_table(index=["State", "Year"], columns="Tier_CarveOut", values="Value", fill_value=0).reset_index()

    CES_wide["TotalRPS_ReEDS"] = CES_wide["Total RPS"]
    CES_wide["TotalCES_ReEDS"] = CES_wide["TotalRPS_ReEDS"] + CES_wide.get("CES (incremental to RPS)", 0)

    CEStarget = CES_wide[["State", "Year", "TotalCES_ReEDS"]]
    CEStarget = CEStarget[CEStarget["State"] != "DC"]
    CEStarget = CEStarget.merge(In_Retail_Sales, on=["Year", "State"], how="left")
    CEStarget["CES"] = CEStarget["TotalCES_ReEDS"] / CEStarget["Sales"]
    CEStarget = CEStarget[["Year", "State", "CES"]]
    CEStarget = CEStarget[CEStarget["Year"].astype(int) > 2009]

    years = list(range(2010, 2051))

    # Dynamically determine CES states by detecting state with a "CES" row in the `RPS & CES Demand Projections` sheet
    ces_states = sorted(
        CES_df.loc[CES_df["Tier_CarveOut"].str.contains("CES", case=False, na=False), "State"].unique()
    )
    rows = []
    for region in ces_states:
        row = {"st": region}
        for y in years:
            val = CEStarget.loc[(CEStarget["State"] == region) & (CEStarget["Year"].astype(int) == y), "CES"]
            row[y] = val.values[0] if not val.empty else 0
        rows.append(row)
    
    out_df = pd.DataFrame(rows, columns=["st"] + years)

    # Reshape to long format and sort
    out_df = out_df.melt(id_vars='st', var_name='*t', value_name='Value')
    out_df['*t'] = out_df['*t'].astype(int)
    out_df = out_df.sort_values(by=['st', '*t'])
    out_df = out_df[['*t', 'st', 'Value']]

    # Save to CSV
    output_path = os.path.join("outputs", "intermediate outputs", "ces_fraction_intermediate.csv")
    out_df.to_csv(output_path, index=False)
    print(f"...Intermediate CES data processed and saved to {output_path}")

### Function to calculate hydro fractions in selected year
### Then hydro fraction is calculated as the hydro's serving in `Non-RE Accounting` sheet divided by the total ReEDS hydro generation.
### The results are saved to `hydro_fraction.csv` in the output directory.
### -----------------------------------------------------------------------------------

def calculate_hydrofrac(main_excel_file, gen_df, hierarchy_df):

    # Load and Process ReEDS Hydro Generation Data
    mergeddf = gen_df.merge(hierarchy_df, how='left', on='r')
    hydro_list = ['hydED', 'hydEND', 'hydUD', 'hydUND']
    filtered_df_hydro = mergeddf[mergeddf['i'].isin(hydro_list)]
    df_grouped_hydro = filtered_df_hydro.groupby(by=["st", "t"])["Value"].sum().reset_index()
    df_grouped_hydro.rename(columns={"st": "State", "t": "Year", "Value": "ReEDSgen_MWh"}, inplace=True)
    df_grouped_hydro["ReEDSgen_GWh"] = df_grouped_hydro["ReEDSgen_MWh"] / 1000

    # Load hydro's serving in `Non-RE Accounting` sheet
    rps_data = pd.read_excel(main_excel_file, sheet_name=Hydrosheetname, usecols=Hydrosheet_RPS_usecols, skiprows=Hydrosheet_RPS_skiprows, nrows=Hydrosheet_RPS_nrows)
    rps_data.columns = ["State", "Includes", "Hydro", "MSW", "Other_Non_RE"]
    rps_data[["Hydro", "MSW", "Other_Non_RE"]] = rps_data[["Hydro", "MSW", "Other_Non_RE"]].apply(pd.to_numeric, errors="coerce").fillna(0)
    if "DC" in rps_data["State"].values:
        dc_row = rps_data[rps_data["State"] == "DC"]
        rps_data.loc[rps_data["State"] == "MD", ["Hydro", "MSW", "Other_Non_RE"]] += dc_row[["Hydro", "MSW", "Other_Non_RE"]].values
        rps_data = rps_data[rps_data["State"] != "DC"]
    rps_data["GalenRPS_GWh"] = rps_data["Hydro"]

    ces_data = pd.read_excel(main_excel_file, sheet_name=Hydrosheetname, usecols=Hydrosheet_CES_usecols, skiprows=Hydrosheet_CES_skiprows, nrows=Hydrosheet_CES_nrows)
    ces_data.columns = ["State", "Includes", "Hydro", "Nuclear"]
    ces_data["Hydro"] = pd.to_numeric(ces_data["Hydro"], errors="coerce").fillna(0)
    if "DC" in ces_data["State"].values:
        dc_row = ces_data[ces_data["State"] == "DC"]
        ces_data.loc[ces_data["State"] == "MD", "Hydro"] += dc_row["Hydro"].values[0]
        ces_data = ces_data[ces_data["State"] != "DC"]
    ces_data["GalenCES_GWh"] = ces_data["Hydro"]

    df_galen = pd.merge(rps_data[["State", "GalenRPS_GWh"]], ces_data[["State", "GalenCES_GWh"]], on="State", how="outer").fillna(0)

    # Calculate Hydro Fractions for selected year
    hydro_selected_year = df_grouped_hydro[df_grouped_hydro["Year"] == hydro_year].copy()
    df_final = pd.merge(hydro_selected_year, df_galen, on="State", how="outer").fillna(0)

    df_final["hydrofrac_RPS"] = np.where(df_final["ReEDSgen_GWh"] > 0, np.minimum(df_final["GalenRPS_GWh"] / df_final["ReEDSgen_GWh"], 1), 0)
    df_final["hydrofrac_RPS"] = df_final["hydrofrac_RPS"].apply(lambda x: x if x > 0.001 else 0)

    df_final["hydrofrac_CES"] = np.where(df_final["ReEDSgen_GWh"] > 0, np.minimum((df_final["GalenRPS_GWh"] + df_final["GalenCES_GWh"]) / df_final["ReEDSgen_GWh"], 1), 0)
    df_final["hydrofrac_CES"] = df_final["hydrofrac_CES"].apply(lambda x: x if x > 0.001 else 0)

    # Format and save
    outdf = df_final[["State", "hydrofrac_RPS", "hydrofrac_CES"]].rename(columns={"State": "st", "hydrofrac_RPS": "RPS_All", "hydrofrac_CES": "CES"})
    output_path = os.path.join("outputs", "hydrofrac_policy.csv")
    outdf.sort_values("st").round(9).to_csv(output_path, index=False)
    print(f"...hydrofrac data generated and saved to {output_path}")


### Function to piecewise interpolate RPS and CES time series data after 2023
### This function takes intermediate outputs files `rps_fraction_intermediate.csv` and `ces_fraction_intermediate.csv`
### and generates interpolated policy files `rps_fraction.csv` and `ces_fraction.csv`.
### This is to allows for smooth transitions in policy values over time so ReEDS can "foreseen" the future policy changes.
### -----------------------------------------------------------------------------------


def interpolate_policy_file(input_path, output_path, 
                            value_columns, 
                            index_col='t', 
                            state_col='st', 
                            column_tolerances=None, 
                            base_year=2023):
    
    print(f"...Interpolating policy file: {input_path} to {output_path}")

    def piecewise_interpolate(series, base_year, tolerance):
        series = series.copy()
        series.index = series.index.astype(int)
        historical = series[series.index < base_year]
        future = series[series.index >= base_year]

        if future.isnull().all():
            return series

        change_points = {}
        prev_value = None
        for year in future.index:
            value = future.loc[year]
            if prev_value is None or abs(value - prev_value) > tolerance:
                change_points[year] = value
                prev_value = value

        if future.index[-1] not in change_points:
            change_points[future.index[-1]] = future.loc[future.index[-1]]

        change_points = pd.Series(change_points).sort_index()
        interpolated = pd.Series(index=future.index, dtype=float)

        for (y0, y1) in zip(change_points.index[:-1], change_points.index[1:]):
            v0 = change_points.loc[y0]
            v1 = change_points.loc[y1]
            years_in_range = future.index[(future.index >= y0) & (future.index <= y1)]
            interpolated.loc[years_in_range] = [
                v0 + (v1 - v0) * (year - y0) / (y1 - y0) for year in years_in_range
            ]

        # Ensure interpolated values are at least as large as the original values
        interpolated = interpolated.combine(future, func=max)

        return pd.concat([historical, interpolated]).sort_index().ffill().fillna(future.min())

    df = pd.read_csv(input_path)
    df_out = df[[index_col, state_col]].drop_duplicates().sort_values([state_col, index_col]).copy()

    for col in value_columns:
        tol = column_tolerances[col] if column_tolerances and col in column_tolerances else 0.01
        df_pivot = df.pivot(index=index_col, columns=state_col, values=col)
        df_interp = df_pivot.apply(piecewise_interpolate, base_year=base_year, tolerance=tol)
        df_interp_long = df_interp.reset_index().melt(id_vars=index_col, var_name=state_col, value_name=col)
        df_out = pd.merge(df_out, df_interp_long, on=[index_col, state_col], how='left')

    df_out = df_out.sort_values([state_col, index_col])
    df_out.to_csv(output_path, index=False)


### ===========================================
### ===========Main============================
### ===========================================

if __name__ == "__main__":

    os.makedirs("outputs", exist_ok=True)

    # --- Run Processing Functions ---

    calculate_rps_fraction(
        main_excel_file=filename,
        voluntary_file=filename_voluntary,
        non_us_file=filename_nonUS
    )

    calculate_ces_fraction(
        main_excel_file=filename
    )

    interpolate_policy_file(
        input_path=os.path.join("outputs", "intermediate outputs", "rps_fraction_intermediate.csv"),
        output_path=os.path.join("outputs", "rps_fraction.csv"),
        value_columns=["rps_all", "rps_solar", "rps_wind"],
        index_col="t",
        state_col="st",
        column_tolerances={
            "rps_all": 0.05,
            "rps_solar": 0.01,
            "rps_wind": 0.01
        }
    )

    interpolate_policy_file(
        input_path=os.path.join("outputs", "intermediate outputs", "ces_fraction_intermediate.csv"),
        output_path=os.path.join("outputs", "ces_fraction.csv"),
        value_columns=["Value"],
        index_col="*t",
        state_col="st",
        column_tolerances={"Value": 0.08}
    )

    calculate_hydrofrac(
        main_excel_file=filename,
        gen_df=gen,
        hierarchy_df=hierarchy
    )

    print("...Data processing complete!")