#### This file takes raw data from the EIA trilateral border crossing
#### transmission line dataset and filters it down to just the lines
#### connecting the U.S. and Canada. It also splits up multi-line rows
#### that list multiple voltages and attaches an ID to each row.

import pandas as pd
import re
import os
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd

def main():
    # International line data from the Energy Information Administration (EIA)
    df_eia = pd.read_csv("data/raw_inputs/EIA_trilateral_lines.csv")

    # Get US-to-Canada information for line border crossings
    df_eia_usa = (
        df_eia.loc[(
            (df_eia['Country From'] == 'United States')
            & (df_eia['Country To'] == 'Canada')
        )]
        .reset_index(drop=True)
        .copy()
    )
    gdf_eia_usa = gpd.GeoDataFrame(
        df_eia_usa,
        geometry=gpd.points_from_xy(df_eia_usa.Longitude, df_eia_usa.Latitude),
        crs="EPSG:4326"
    )

    # Get Canada-to-US information for line border crossings
    df_eia_can = (
        df_eia.loc[(
            (df_eia['Country From'] == 'Canada')
            & (df_eia['Country To'] == 'United States')
        )]
        .reset_index(drop=True)
        .copy()
    )
    gdf_eia_can = gpd.GeoDataFrame(
        df_eia_can,
        geometry=gpd.points_from_xy(df_eia_can.Longitude, df_eia_can.Latitude),
        crs="EPSG:4326"
    )

    # Combine US-to-Canada and Canada-to-US information by nearest
    # neighbor matching of border crossing locations
    gdf_eia = (
        gpd.sjoin_nearest(
            gdf_eia_usa.to_crs("EPSG:3857"),
            gdf_eia_can.to_crs("EPSG:3857"),
            how='left',
            lsuffix='usa',
            rsuffix='can'
        )
        .to_crs("EPSG:4326")
    )

    # Rename relevant columns to match CER line data
    column_name_map = {
        'State / Province / Territory_can': 'Origin',
        'State / Province / Territory_usa': 'Destination',
        'Owner Name (Company)_can': 'Company',
        'Name of Line_can': 'LineName',
        'City (Site Name)_can': 'CanadianSite',
        'City (Site Name)_usa': 'AmericanSite',
        'County_usa': 'County',
        'Line Voltage (kV)_can': 'Voltage'
    }
    df_eia = gdf_eia.rename(columns=column_name_map)
    # The Nelway lines are split into two rows in the U.S.-Canada dataset but
    # but one row in the Canada-U.S. dataset, so we drop one of the duplicate rows.
    df_eia = df_eia.loc[df_eia['Name of Line_usa'] != 'Nelway 2'].copy()
    df_eia = df_eia[column_name_map.values()]

    # Split rows that comprise multiple lines
    df_eia['voltage_numlines'] = df_eia['Voltage'].str.split('/')
    df_eia = df_eia.explode('voltage_numlines')
    df_eia['voltage'] = (
        df_eia.voltage_numlines
        .apply(lambda x: x.split('(')[0] if '(' in x else re.sub(r'[a-zA-Z]', '', x))
        .astype(float)
    )
    df_eia['num_lines'] = (
        df_eia.voltage_numlines
        .apply(lambda x: re.search(r"\((.*?)\)", x).group(1) if '(' in x else 1)
        .astype(int)
    )
    df_eia = (
        df_eia.drop(columns=['Voltage', 'voltage_numlines'])
        .reset_index(drop=True)
    )
    df_eia['line_ID'] = (
        df_eia.CanadianSite
        + '-'
        + df_eia.AmericanSite
        + '_'
        + df_eia.voltage.astype(int).astype(str)
    )
    df_eia.loc[df_eia.LineName == 'No Name', 'LineName'] = (
        df_eia.CanadianSite + '-' + df_eia.AmericanSite + ' IPL'
    )
    df_eia['LineName'] = df_eia.LineName + '_' + df_eia.voltage.astype(int).astype(str)

    out_fpath = "data/EIA_usa_can_lines.csv"
    df_eia.to_csv(out_fpath, index=False)
    print(f"Run complete. See {out_fpath} for outputs.")


if __name__ == "__main__":
    main()