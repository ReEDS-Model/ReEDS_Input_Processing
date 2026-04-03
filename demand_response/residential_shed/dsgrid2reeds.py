'''
Script to convert dsgrid parquet file into h5 file for ReEDS
'''
#%%
import pandas as pd 
import numpy as np 
import h5py
import datetime
import re
import os

# path the input processing repo 
reeds_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#%%
# Functions to read/write h5 files 

def write_profile_to_h5(df, filename, outfolder, compression_opts=4):
    """Writes dataframe to h5py file format used by ReEDS. Used in ReEDS and hourlize

    This function takes a pandas dataframe and saves to a h5py file. Data is saved to h5 file as follows:
        - the data itself is saved to a dataset named "data"
        - column names are saved to a dataset named "columns"
        - the index of the data is saved to a dataset named "index"; in the case of a multindex,
          each index is saved to a separate dataset with the format "index_{index order}"
        - the names of the index (or multindex) are saved to a dataset named "index_names"

    Parameters
    ----------
    df
        pandas dataframe to save to h5
    filename
        Name of h5 file
    outfolder
        Path to folder to save the file (in ReEDS this is usually the inputs_case folder)

    Returns
    -------
    None
    """
    outfile = os.path.join(outfolder, filename)
    with h5py.File(outfile, 'w') as f:
        # save index or multi-index in the format 'index_{index order}')
        for i in range(df.index.nlevels):
            # get values for specified index level
            indexvals = df.index.get_level_values(i)
            # save index
            if isinstance(indexvals[0], bytes):
                # if already formatted as bytes keep that way
                f.create_dataset(f'index_{i}', data=indexvals, dtype='S30')
            elif indexvals.name == 'datetime':
                # if we have a formatted datetime index that isn't bytes, save as such
                timeindex = (
                    indexvals.to_series().apply(datetime.datetime.isoformat).reset_index(drop=True)
                )
                f.create_dataset(f'index_{i}', data=timeindex.str.encode('utf-8'), dtype='S30')
            else:
                # Other indices can be saved using their data type
                f.create_dataset(f'index_{i}', data=indexvals, dtype=indexvals.dtype)

        # save index names
        index_names = pd.Index(df.index.names)
        if len(index_names):
            f.create_dataset(
                'index_names', data=index_names, dtype=f'S{index_names.map(len).max()}'
            )

        # save column names as string type
        if len(df.columns):
            f.create_dataset('columns', data=df.columns, dtype=f'S{df.columns.map(len).max()}')

        # save data if it exists
        if df.empty:
            pass
        elif len(df.dtypes.unique()) == 1:
            dtype = df.dtypes.unique()[0]
            f.create_dataset(
                'data',
                data=df.values,
                dtype=dtype,
                compression='gzip',
                compression_opts=compression_opts,
            )
        else:
            types = df.dtypes.unique()
            print(df)
            raise ValueError(f"{outfile} can only contain one datatype but it contains {types}")

        return df

def read_file(filename, index_columns=1):
    """
    Read input file of various types (for backwards-compatibility)
    """
    # Try reading a .h5 file written by pandas
    try:
        df = pd.read_hdf(filename+'.h5')
    # Try reading a .h5 file written by h5py
    except (ValueError, TypeError, FileNotFoundError, OSError):
        try:
            with h5py.File(filename+'.h5', 'r') as f:
                keys = list(f)
                datakey = 'data' if 'data' in keys else ('cf' if 'cf' in keys else 'load')
                ### If none of these keys work, we're dealing with EER-formatted load
                if datakey not in keys:
                    years = [int(y) for y in keys if y != 'columns']
                    df = pd.concat(
                        {y: pd.DataFrame(f[str(y)][...]) for y in years},
                        axis=0)
                    df.index = df.index.rename(['year','hour'])
                else:
                    df = pd.DataFrame(f[datakey][:])
                    df.index = pd.Series(f['index']).values
                df.columns = pd.Series(f['columns']).map(
                    lambda x: x if type(x) is str else x.decode('utf-8')).values
        # Fall back to .csv.gz
        except (FileNotFoundError, OSError):
            df = pd.read_csv(
                filename+'.csv.gz', index_col=list(range(index_columns)),
                float_precision='round_trip',
            )

    return df

#%%
# dictionary to define shed types and resource name in reeds 
dr_types = {
    'dr_shed_1': 'cooling_shed_capacity',
    'dr_shed_2': 'heating_shed_capacity'
}

# create h5 files from parquet 
df_pivot = pd.read_parquet(os.path.join(reeds_path,'Residential_shed','dsgrid_data', 'table_pivot_shed.parquet'))

# create list to hold dataframes for each year
df_all_years = []

for year in df_pivot['all_years'].unique():
    #Filter to single year
    df_year = df_pivot[df_pivot['all_years'] == year]
    
    # Create a DataFrame with a timestamp column for the year 2018
    dr_hourly = pd.DataFrame({'timestamp': pd.date_range(start='2018-01-01', end='2018-12-31 23:00:00 ', freq='H',tz='UTC-06:00')})

    # Iterate over unique regions in 'reeds_pca'
    for ba in  df_pivot['reeds_pca'].unique():
        # Filter data for the region (ba)
        df_region =  df_year[ df_year['reeds_pca'] == ba]

        # Make sure timestamps from df_region align with dr_hourly by reindexing or resampling
        df_region = df_region.set_index('time_ct').reindex(dr_hourly['timestamp']).fillna(0)  # Reindex to align timestamps
        
        # Add the region-specific dr capacity data to dr_hourly
        dr_hourly[f'dr_shed_1|{ba}'] = df_region[dr_types['dr_shed_1']].values
        dr_hourly[f'dr_shed_2|{ba}'] = df_region[dr_types['dr_shed_2']].values


    dr_hourly = dr_hourly.rename(columns={'timestamp':'datetime'})

    # Set 'timeindex' as index
    dr_hourly['datetime'] = pd.to_datetime(dr_hourly['datetime']).dt.tz_convert(None)
    dr_hourly.set_index('datetime', inplace=True)
    # Add a year column to the DataFrame
    dr_hourly.insert(0, 'year', int(year))

    df_all_years.append(dr_hourly)

# Concatenate all DataFrames in the list into a single DataFrame
df_all = pd.concat(df_all_years)

df_all.astype(np.float32).to_hdf(os.path.join(reeds_path,'Residential_shed','reeds_inputs','dr_shed_hourly.h5'), key = 'data', complevel=4)

#%%
# Check the data
read_file(os.path.join(reeds_path,'Residential_shed','reeds_inputs','dr_shed_hourly'))
