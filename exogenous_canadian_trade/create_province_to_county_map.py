#### This file takes the reported capacities of the transmission lines
#### connecting Canadian provinces to U.S. counties and outputs the
#### amount of each province's transmission capacity that is
#### associated with each county.

import pandas as pd
import json


def get_line_data(line_id_certificate_map):    
    # U.S.-Canada line data from the Canada Energy Regulator (CER)
    # Data contain line voltages and capacities and are labeled by the regulatory
    # certificate(s) governing each line 
    df_cer = pd.read_excel('data/raw_inputs/CER_usa_can_lines.xlsx')
    df_cer.columns = df_cer.loc[0]
    df_cer = df_cer.loc[1:].reset_index(drop=True)
    df_cer['Capacity'] = df_cer.Capacity.str.replace(',', '').astype(float)
    df_cer.loc[df_cer.Status != 'Operating', 'Capacity'] = 0
    df_cer = (
        df_cer.loc[:,~df_cer.columns.duplicated()]
        .drop(columns=['LengthUnit', 'Status', 'IPL characteristics'])
    )
    df_cer['Origin'] = df_cer['Origin'].ffill().replace('Québec', 'Quebec')
    df_cer['Destination'] = df_cer['Destination'].ffill()
    df_cer['voltage'] = df_cer.Voltage.astype(float)

    # U.S.-Canada line data from the Energy Information Administration (EIA)
    # Data contain line voltages and county-level endpoint locations
    df_eia = pd.read_csv("data/EIA_usa_can_lines.csv")
    # Map line IDs to their corresponding regulatory certificate(s)
    df_eia['certificates'] = df_eia['line_ID'].map(line_id_certificate_map)
    df_eia = df_eia.explode('certificates')

    # Merge datasets on their regulatory certificates
    df_lines = df_eia.merge(
        df_cer,
        left_on='certificates',
        right_on='Original regulatory instrument',
        suffixes=('_eia', '_cer')
    )
    return df_lines


def main():
    # Read mapping from line names (as specified in the EIA dataset) to their
    # corresponding regulatory certificates (as specified in the CER dataset)
    with open('data/line_id_certificate_map.json', 'r') as f:
        line_id_certificate_map = json.load(f)

    # Get international line data and calculate county-level transmission capacities
    df_lines = get_line_data(line_id_certificate_map)
    county_capacities = (
        df_lines.rename(columns={'Origin_eia': 'Province', 'Destination_eia': 'State'})
        .groupby(['Province', 'State', 'County'])
        ['Capacity']
        .sum()
        .reset_index()
    )
    county_capacities = county_capacities.loc[county_capacities.Capacity > 0].copy()

    # Rename counties to match ReEDS counties file and merge to get corresponding pFIPS
    reeds_counties = pd.read_excel(
        '../Region_Disaggregation/processing_scripts/county_reeds_corrected0310.xlsx',
        header=0,
        usecols=['NAME','STATE_NAME','pFIPS']
    )
    county_capacities['County'] = county_capacities['County'].str.replace('Saint', 'St.')
    assert (
        len(reeds_counties.loc[reeds_counties.NAME.isin(county_capacities.County)].NAME.unique())
        == len(county_capacities.County.sort_values().unique())
    )
    county_capacities = (
        county_capacities.merge(
            reeds_counties[['NAME', 'STATE_NAME', 'pFIPS']],
            left_on=['County', 'State'],
            right_on=['NAME', 'STATE_NAME']
        )
        [['Province', 'pFIPS', 'Capacity']]
    )

    # Calculate proportion of each province's transmission capacity going to each county
    # and pivot table to create province-to-county map
    county_capacities['proportion_of_province_capacity'] = (
        county_capacities['Capacity']
        / county_capacities.groupby('Province')['Capacity'].transform('sum')
    )
    province_to_county_map = pd.pivot_table(
        county_capacities,
        index='Province',
        columns='pFIPS',
        values='proportion_of_province_capacity'
    )
    assert(all(province_to_county_map.sum(axis=1) == 1))

    # Export
    out_fpath = "data/province_to_county_map.csv"
    province_to_county_map.to_csv(out_fpath)
    print(f"Run complete. See {out_fpath} for outputs.")


if __name__ == "__main__":
    main()