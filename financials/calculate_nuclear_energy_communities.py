import pandas as pd
import os
os.makedirs('outputs', exist_ok=True)

def get_ct_county_crosswalk():
    # Source:
    # https://www2.census.gov/geo/docs/reference/ct_change/ct_cou_to_cousub_crosswalk.xlsx
    df = pd.read_excel('data/nuclear_energy_communities/ct_cou_to_cousub_crosswalk.xlsx')
    df = df[['OLD_COUNTYFP\n(INCITS31)', 'NEW_COUNTYFP\n(INCITS31)']].dropna()
    df.columns = ['old_county_fips', 'new_county_fips']
    df['FIPS'] = 'p09' + df['old_county_fips'].astype(int).astype(str).str.zfill(3)
    df['FIPS_new'] = 'p09' + df['new_county_fips'].astype(int).astype(str).str.zfill(3)
    df = df[['FIPS', 'FIPS_new']].drop_duplicates()
    return df

def add_FIPS_column(df):
    df['FIPS'] = (
        'p'
        + df['fipstate'].astype(str).str.zfill(2)
        + df['fipscty'].astype(str).str.zfill(3)
    )
    return df

def get_nuclear_energy_counties(year, msa_county_map, ct_county_crosswalk):
    # Note: These files are fairly large so they are left out of the repo.
    # They can be downloaded from https://www.census.gov/programs-surveys/cbp/data/datasets.html
    # by navigating to the desired year and clicking "County File" in the "Datasets" list.
    fpath = f'data/nuclear_energy_communities/cbp{year - 2000}co.txt'
    try:
        df = pd.read_csv(fpath)
    except FileNotFoundError:
        error_string = f"""The CBP dataset {fpath} was left out of the repo due to size.
        Download it from https://www.census.gov/programs-surveys/cbp/data/datasets.html."""
        raise FileNotFoundError(error_string)

    df.columns = [col.lower() for col in df.columns]
    df = add_FIPS_column(df)

    # Impute suppressed values using process described in Appendix A of
    # https://home.treasury.gov/system/files/8861/EnergyCommunities_Data_Documentation.pdf
    # Note the suppresion flags and value ranges are not provided after 2017        
    if year <= 2017:
        irs_empflag_value_map = {
            'A': 5,
            'B': 40,
            'C': 137,
            'E': 312,
            'F': 625,
            'G': 1375,
            'H': 3125,
            'I': 6250,
            'J': 13750,
            'K': 31250,
            'L': 62500,
            'M': 125000
        }
        df.loc[df.emp_nf.isin(['D', 'S']), 'emp'] = (
            df.loc[df.emp_nf.isin(['D', 'S']), 'empflag']
            .map(irs_empflag_value_map)
        )

    # Calculate the nuclear employment rate (NER) for each MSA
    df = (
        pd.merge(
            df[['FIPS', 'naics', 'emp']],
            msa_county_map[['msa', 'FIPS']],
            on='FIPS',
            how='left'
        )
        .assign(total_emp=0, nuclear_emp=0)
    )
    nuclear_naics = ['221113']
    df.loc[df.naics.isin(nuclear_naics), 'nuclear_emp'] = (
        df.loc[df.naics.isin(nuclear_naics), 'emp'].fillna(0)
    )
    df.loc[df.naics == '------', 'total_emp'] = (
        df.loc[df.naics == '------', 'emp'].fillna(0)
    )
    df_msa = df.groupby('msa', as_index=False)[['total_emp', 'nuclear_emp']].sum()
    df_msa['nuclear_employment_rate'] = df_msa['nuclear_emp'] / df_msa['total_emp']

    # Determine MSAs whose NER is at least 0.17 percent
    df_msa = df_msa.loc[df_msa.nuclear_employment_rate >= 0.0017]

    # Determine counties belonging to the above MSAs
    df_msa = df_msa.merge(msa_county_map[['msa', 'FIPS']], on='msa', how='left')
    if year < 2022:
        # Replace old CT FIPS codes with the FIPS codes used in ReEDS
        df_msa = df_msa.merge(ct_county_crosswalk, on='FIPS', how='left')
        df_msa.loc[df_msa.FIPS_new.notna(), 'FIPS'] = (
            df_msa.loc[df_msa.FIPS_new.notna(), 'FIPS_new']
        )    
    nuclear_energy_counties = df_msa.FIPS.tolist()

    return nuclear_energy_counties

def main():
    print("Starting calculation...")
    reeds_path = os.path.expanduser('~/github/ReEDS')

    # Source:
    # https://www.census.gov/programs-surveys/cbp/technical-documentation/reference/metro-area-geography-reference.html
    msa_county_map_22 = pd.read_csv('data/nuclear_energy_communities/msa_county_reference22.txt', encoding='latin1')
    msa_county_map_22 = add_FIPS_column(msa_county_map_22)
    
    msa_county_map_17 = pd.read_csv('data/nuclear_energy_communities/msa_county_reference17.txt', encoding='latin1')
    msa_county_map_17 = add_FIPS_column(msa_county_map_17)

    ct_county_crosswalk = get_ct_county_crosswalk()

    county2zone = pd.read_csv(os.path.join(reeds_path, 'inputs/county2zone.csv'))
    county2zone['FIPS'] = 'p' + county2zone['FIPS'].astype(str).str.zfill(5)

    nuclear_energy_counties_all_years = []
    for year in range(2010, 2024):
        if year < 2022:
            # Replace CT's rows with MSAs using CT's old FIPS codes
            msa_county_map = pd.concat([
                msa_county_map_22.loc[~msa_county_map_22.FIPS.str.startswith('p09')],
                msa_county_map_17.loc[msa_county_map_17.FIPS.str.startswith('p09')]
            ])
        else:
            msa_county_map = msa_county_map_22

        nuclear_energy_counties = get_nuclear_energy_counties(year, msa_county_map, ct_county_crosswalk)
        nuclear_energy_counties_all_years.extend(nuclear_energy_counties)

    nuclear_energy_communities = pd.DataFrame(
        columns=['County Region'],
        data=sorted(
            [county for county in list(set(nuclear_energy_counties_all_years))
            if county in list(county2zone.FIPS)]
        )
    )

    out_fpath = 'outputs/nuclear_energy_communities.csv'
    nuclear_energy_communities.to_csv(out_fpath, index=False)
    print(f"Run complete. See {out_fpath} for outputs.")

if __name__ == "__main__":
    main()