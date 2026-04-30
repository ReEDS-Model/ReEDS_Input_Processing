
import pandas as pd
import os
import sys
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd

#reedspath = os.path.expanduser('~/github/ReEDS/')
reedspath = os.path.expanduser('~/Documents/Github/ReEDS/ReEDS/')
reedspath = os.path.expanduser(reedspath)
sys.path.append(reedspath)
import reeds

def main():
    hardrock = gpd.read_file(
        os.path.join('geologic-salt-cavern-storage', 'Hardrock_Shapefile_4326.shp')
    )
    hardrock_geom = (
        hardrock.to_crs('ESRI:102008')
        .dissolve()
        .loc[0,'geometry']
        .buffer(0.)
    )

    salt = gpd.read_file(
        os.path.join('geologic-salt-cavern-storage', 'salt_Shapefile_4326.shp')
    )
    salt_geom = (
        salt.to_crs('ESRI:102008')
        .dissolve()
        .loc[0,'geometry']
        .buffer(0.)
    )
    
    region_levels = ['county','ba']

    for region_level in region_levels:
        if region_level == 'county':
            dfout = reeds.spatial.get_map('county', source='tiger').to_crs('ESRI:102008')
            ## Format for ReEDS
            dfout['FIPS'] = dfout.index.values
            dfout['rb'] = 'p' + dfout['FIPS']
            state_fips = pd.read_csv(
                os.path.join(reedspath, 'inputs', 'shapefiles', 'state_fips_codes.csv'),
                dtype={'state_fips': str},
                index_col='state_fips',
            ).rename(columns={'state':'STATE', 'state_code':'STCODE'})[['STATE', 'STCODE']]
            dfout = dfout.merge(state_fips, left_on='STATEFP', right_index=True, how='left')
        elif region_level == 'ba':
            dfout = gpd.read_file(
                os.path.join(reedspath, 'inputs', 'shapefiles', 'US_PCA')
            )
        dfout = dfout.set_index('rb')
        dfout['geometry'] = dfout['geometry'].buffer(0.)
        dfout['hardrock'] = dfout.intersection(hardrock_geom)
        dfout['salt'] = dfout.intersection(salt_geom)
        dfout['km2'] = dfout.geometry.area / 1e6
        for col in ['hardrock','salt']:
            dfout[col+'_km2'] = dfout[col].area / 1e6
            dfout[col+'_frac'] = dfout[col+'_km2'] / dfout['km2']

        ### Write list of regions, keeping the cheapest for each
        cutoff = 0.01
        outname = {
            'hardrock':'h2_storage_hardrock',
            'salt':'h2_storage_saltcavern',
            'underground':'h2_storage_undergroundpipe',
        }

        dfwrite = (
            pd.concat(
                {col: pd.Series(dfout.loc[dfout[col+'_frac'] > cutoff].index.values)
                for col in ['hardrock','salt']}
            )
            .reset_index(level=1, drop=True)
            .rename('rb')
            .reset_index()
            .rename(columns={'index':'*h2stortype'})
            .assign(exists=1)
            .pivot(index='rb',columns='*h2stortype',values='exists')
            .reindex(dfout.index)
            .fillna(0)
            .astype(int)
        )
        dfwrite['keep'] = (
            dfwrite.apply(
                lambda row: (
                    'salt' if row.salt
                    else 'hardrock' if row.hardrock
                    else 'underground'
                ),
                axis=1
            )
            .replace(outname)
        )
        dfwrite = (
            dfwrite.reset_index()
            .rename(columns={'keep':'*h2_stor'})
            [['*h2_stor','rb']]
        )
        dfwrite.to_csv(
            os.path.join('outputs',f'h2_storage_{region_level}.csv'),
            index=False
        )

if __name__ == "__main__":
    main()