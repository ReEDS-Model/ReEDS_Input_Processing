import numpy as np
import pandas as pd
from pathlib import Path

## user settings

# path to ReEDS 
reeds_path = "/projects/stdscen/bsergi/ReEDS"
# key: value --> ReEDS filename: filename from dgen team
dgen_scenarios = {
    "stscen2026_midcase": "SS26_01_Mid_Case_Cumulative_distPVcap_MW_county_FIPS"
}
# existing ReEDS scenario to use for historical estimates
hist_dgen_scen = "stscen2023_mid_case"

## dGen CT county mapping
# Mapping from old to new CT counties provided by dGen team. This is a temporary
# fix until the dGen team is able to update their model use the new counties.
ct_counties = pd.read_csv(Path("raw_dgen_data") / "CT_lkup_counties_2020_to_2022.csv")
ct_counties['r_old'] = 'p' + ct_counties['fips_code_2020'].astype(str).str.zfill(5)
ct_counties['r_new'] = 'p' + ct_counties['fips_code_2022'].astype(str).str.zfill(5)
ct_counties = ct_counties[['r_old', 'r_new', 'pct_overlap']]
ct_counties['pct_overlap'] /= 100

## historical data
# currently using old dgen estimates to fill in years before first dgen estimate
dgen_inputs_path = Path(reeds_path) / "inputs" / "dgen_model_inputs"
dgen_hist_path = dgen_inputs_path / hist_dgen_scen / f"distpvcap_{hist_dgen_scen}.csv"
dgen_hist_df = pd.read_csv(dgen_hist_path, index_col='r')
dgen_hist_df.columns = dgen_hist_df.columns.astype(int)

## procedure
for scen, file in dgen_scenarios.items():
    # load file
    print(f"processing {scen}")
    df_in = pd.read_csv(Path("raw_dgen_data") / f"{file}.csv")

    # format region names use FIPS code
    df_in['r'] = 'p' + df_in['fips_code'].astype(str).str.zfill(5)
    year_cols = [col for col in df_in.columns if str(col).isdigit()]
    df = df_in[['r'] + year_cols].set_index('r')
    df.columns = df.columns.astype(int)

    # reallocate old CT county values to new CT counties using pct_overlap multipliers
    value_cols = list(df.columns)
    ct_old = df.loc[ct_counties['r_old'].unique()]
    ct_new = ct_counties.merge(ct_old, left_on='r_old', right_index=True)
    if not np.allclose(ct_new.groupby(['r_old'])['pct_overlap'].sum(), 1):
        raise ValueError("CT county percentage overlaps do not sum to 100%.")
    ct_new[value_cols] = ct_new[value_cols].mul(ct_new['pct_overlap'], axis=0)
    ct_new = ct_new.groupby('r_new')[value_cols].sum()
    ct_new.index.name = 'r'
    df = pd.concat([
        df.drop(index=ct_old.index),
        ct_new
    ])

    # if p46113 fips code is present map to the new code for Oglala Lakota County SD (p46102)
    df = df.rename(index={"p46113": "p46102"})

    # concatenate historical estimates
    first_year = min([int(year) for year in year_cols])
    df_out = pd.concat([
        dgen_hist_df.loc[:, dgen_hist_df.columns < first_year],
        df
    ], axis=1)

    # flag issues
    missing_before_first_year = df_out.loc[
        :, df_out.columns < first_year
    ].isna().any(axis=1)
    missing_after_first_year = df_out.loc[
        :, df_out.columns >= first_year
    ].isna().any(axis=1)

    rows_missing_before_first_year = df_out.index[missing_before_first_year]
    rows_missing_after_first_year = df_out.index[missing_after_first_year]

    if len(rows_missing_before_first_year):
        print(
            f"rows with missing values before {first_year}:\n"
            f"{rows_missing_before_first_year.tolist()}"
        )
    if len(rows_missing_after_first_year):
        print(
            f"rows with missing values from {first_year} onward:\n"
            f"{rows_missing_after_first_year.tolist()}"
        )

    # sort
    df_out = df_out.sort_index() 

    # create folder and save to ReEDS
    out_path = dgen_inputs_path / scen 
    out_path.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_path / f"distpvcap_{scen}.csv", index=True)
    print(f"...saved to {out_path}")
