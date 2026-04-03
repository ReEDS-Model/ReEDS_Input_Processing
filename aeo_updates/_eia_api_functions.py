# -*- coding: utf-8 -*-
"""
Created on Wed Feb 22 14:16:27 2023

@author: aschleif

Functions to retrieve/process data from the EIA API

"""
# Import packages
import pandas as pd
import requests
import os

def get_api_key():
    """
    Get EIA API key from environment variable.
    
    Returns:
        str: API key
        
    Raises:
        ValueError: If API key is not found
    """
    api_key = os.getenv('EIA_API_KEY')
    if not api_key:
        raise ValueError(
            "EIA API key not found. Please set the EIA_API_KEY environment variable:\n"
            "1. To register for an API key : https://www.eia.gov/opendata/register.php\n"
            "2. Set the EIA_API_KEY environment variable. Instructions can be found in AEO_Updates/README.md"
        )
    return api_key

# Get API key using secure method
api_key = get_api_key()

def create_EIA_url(api_key, route, data_columns, facets, freq=None, start=None, end=None):
    url = 'https://api.eia.gov/v2/electricity/{}/data?api_key={}'.format(route, api_key)
    for data in data_columns:
        # assert data in route_data[route]
        url += '&data[]={}'.format(data)
    for fkey in facets:
        # assert fkey in route_facets[route]
        for fval in facets[fkey]:
            url += '&facets[{}][]={}'.format(fkey, fval)
    if freq is not None:
        url += '&frequency={}'.format(freq)
    if start is not None:
        url += '&start={}'.format(start)
    if end is not None:
        url += '&end={}'.format(end)
    return url

def create_SEDS_url(api_key, series_IDs, facets={}, freq=None, start=None, end=None):
    url = 'https://api.eia.gov/v2/seds/data?api_key={}'.format(api_key)
    url += '&data[]=value'
    for serID in series_IDs:
        url += '&facets[seriesId][]={}'.format(serID)
    for fkey in facets:
        # assert fkey in route_facets[route]
        for fval in facets[fkey]:
            url += '&facets[{}][]={}'.format(fkey, fval)
    if freq is not None:
        url += '&frequency={}'.format(freq)
    if start is not None:
        url += '&start={}'.format(start)
    if end is not None:
        url += '&end={}'.format(end)
    return url

def retrieve_EIA_data(url):
    r = requests.get(url)
    json_data = r.json()
    if 'warnings' in json_data['response'].keys():
        for i in range(0,len(json_data['response']['warnings'])):
            print('...' + json_data['response']['warnings'][i]['warning'])
            print(json_data['response']['warnings'][i]['description'])
    data_keys = list( json_data['response']['data'][0].keys() )
    df = pd.DataFrame(columns = ['year', 'quarter', 'month'] + data_keys)
    for i in range(0, len(json_data['response']['data'])):
        df_temp = pd.DataFrame(json_data['response']['data'][i], index=[i])
        df = pd.concat([df, df_temp], ignore_index=True)
    if json_data['response']['frequency'] == 'monthly':
        df[['year','month']] = df.period.str.split('-',expand=True).astype(int)
        df.sort_values(by=['year','month'], inplace=True)
    elif json_data['response']['frequency'] == 'quarterly':
        df[['year','quarter']] = df.period.str.split('-Q',expand=True).astype(int)
        df.sort_values(by=['year','quarter'], inplace=True)
    elif json_data['response']['frequency'] == 'annual':
        df['year'] = df['period'].astype(int)
        df.sort_values(by=['year'], inplace=True)
    df.reset_index(inplace=True, drop=True)
    return df

