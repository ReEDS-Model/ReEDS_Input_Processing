"""
This script processes and produces ATB costs input files for ReEDS.
Global and technology-specific settings for Scenario, Case, CRPYears etc. are defined in the settings.yaml file.
The script uses the reeds2 environment.
"""

#%% ===========================================================================
### --- IMPORTS ---
### ===========================================================================
import argparse
import os
import numpy as np
import pandas as pd
import shutil
import yaml
from pandas.errors import EmptyDataError
import sys
import requests
from io import StringIO

# mapping for column headers in ATBe file vs. ATB flat file
atbe_col_mapping = {
    'core_metric_parameter': 'Parameter',
    'core_metric_case':      'Case',
    'tax_credit_case':       'TaxCreditCase',
    'crpyears':              'CRPYears',
    'technology':            'Technology',
    'display_name':          'DisplayName',
    'scenario':              'Scenario',
    'core_metric_variable':  'variable',
}

# define zero marginal cost technologies (to assign zero 'vom' if input data is missing)
zero_vom_techs = {'upv', 'wind-ons', 'wind-ofs', 'battery'}

#%% ===========================================================================
### --- FUNCTIONS ---
### ===========================================================================

def subset_atb_rows(subset_rows, atb_data_in):
    """
    Subset the ATB flat file data to desired Case | CRPYears rows

    Parameters
    ----------
    subset_rows: dict
        Default {Case: "Exp + TC", CRPYears: 30} for most techs; some techs can have other values for Case, e.g., Market, R&D etc.)
    atb_data_in: pd.DataFrame
        ATB flat file dataframe to subset
    """
    atb_data = atb_data_in.copy()
    atb_col_vals = {}
    # subset to relevant cases (typically Market case and 30-year CRP, but specified in settings.yaml) 
    for sr in subset_rows:
        print(f"Subsetting to {sr}={subset_rows[sr]}")
        if sr not in atb_data.columns:
            raise ValueError(f"Column {sr} not in ATB file.")
        else:
            atb_col_vals[sr] = atb_data_in[sr].unique().tolist()
            atb_data = atb_data.loc[atb_data[sr] == subset_rows[sr]]
    return atb_data, atb_col_vals

def fetch_atb_from_url(atbyear, settings):
    """
    Retrieve ATBe input csv from a user-provided URL.

    Returns
    -------
    pd.DataFrame
        Loaded ATBe file dataframe.
    """
    url = settings.get('url', '').strip() if settings else ''
    if url:
        print(f"Using url from settings.yaml: {url}")
    else:
        print(f"\nEnter the url to the ATB {atbyear} file (ATBe.csv).")
        print("ATB data is published at: https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=ATB%2Felectricity%2Fcsv%2F&limit=50")
        url = input("Enter URL: ").strip()
        if not url:
            raise ValueError("No URL provided. Cannot fetch an input ATBe file.")

    print(f"Downloading ATB {atbyear} flat file from {url}")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.text), low_memory=False)
    print(f"Downloaded {len(df):,} rows.")
    df = df.rename(columns=atbe_col_mapping)
    return df

def load_atb_flat_file(settings, args, techs_to_run):
    """
    load ATB flat file with all inputs for specified techs

    Parameters
    ----------
    settings: dict
        Parameter settings from settings.yaml
    args: argparse.Namespace
        Used for debug mode
    techs_to_run: list[str]
        Ordered list of tech keys (matching keys in settings['techs'])
    """
    source = settings.get('atb_source', 'file')  # default to 'file' if not specified

    if source == 'url':
        atb_data_in = fetch_atb_from_url(settings['atbyear'], settings)
    else:
        thisdir = os.path.dirname(os.path.abspath(__file__))
        filepath = os.path.join(thisdir, 'input_data', settings['filename'])
        if not os.path.isfile(filepath):
            raise FileNotFoundError(f"Could not find {filepath}. Check 'filename' in settings.yaml.")
        print(f"Loading {filepath}")
        atb_data_in = pd.read_csv(filepath, low_memory=False)

    if args.debug:
        breakpoint()
    
    # store validated/subsetted data for all requested techs here
    validated_data_list = []

    # iterate over requested techs to validate and subset immediately
    for tech in techs_to_run:
        tech_settings = settings['techs'][tech]
        
        # 1. Get tech-specific subset settings
        if 'subset_rows' not in tech_settings:
            raise ValueError(f"Missing 'subset_rows' for {tech} in settings.yaml")
        
        # 2. Filter by Technology
        tech_rows_full = atb_data_in[atb_data_in['Technology'] == tech_settings['Technology']]
        
        if tech_rows_full.empty:
            print(f"Warning: No data found for Technology '{tech_settings['Technology']}' ({tech}).")
            continue

        # 3. Apply specific subset_rows (Case, CRPYears, etc.)
        # subset_atb_rows returns (subset_df, col_vals)
        tech_subset, _ = subset_atb_rows(tech_settings['subset_rows'], tech_rows_full)

        # 4. Filter for specific DisplayName(s) if applicable
        if 'DisplayName' in tech_settings:
            atb_techs = tech_settings['DisplayName']
            atb_techs_list = list(atb_techs.keys()) if isinstance(atb_techs, dict) else [atb_techs]

            # append all subtechs present
            found_data = tech_subset[tech_subset['DisplayName'].isin(atb_techs_list)]
            missing_subtechs = [t for t in atb_techs_list if t not in found_data['DisplayName'].values]

            if found_data is not None and not found_data.empty:
                validated_data_list.append(found_data)
            
            # fallback: handle any subtechs that were missing from the subset
            for atb_tech_name in missing_subtechs:
                # Fallback logic: check if it exists in the wider tech data (e.g., for a different Case)
                temp_df = tech_rows_full[tech_rows_full['DisplayName'] == atb_tech_name]
                
                if temp_df.empty:
                    print(f"Warning: DisplayName '{atb_tech_name}' not found in source data for {tech}.")
                    continue
                print(f"Could not subset {atb_tech_name} data to {tech_settings['subset_rows']}.")
                print(f"Available Case options: {temp_df.Case.unique()}")
                
                new_case = str(input("Select an alternative Case to proceed, or (q) to quit: "))
                
                if new_case.lower() == "q":
                    sys.exit("Exiting now.")
                elif new_case not in temp_df.Case.unique():
                    print(f"'{new_case}' not found in available Case options. Exiting.")
                    sys.exit(1)
                else:
                    # Grab the data for the alternative case
                    new_subset_rows = tech_settings['subset_rows'].copy()
                    new_subset_rows['Case'] = new_case
                    
                    # We only need this specific subtech with the new case
                    subtech_data, _ = subset_atb_rows(new_subset_rows, temp_df)
                    validated_data_list.append(subtech_data)
        else:
            # If no DisplayName filtering is needed, just keep the successful subset
            if not tech_subset.empty:
                validated_data_list.append(tech_subset)

    # Combine all valid chunks
    if not validated_data_list:
        raise EmptyDataError(f"\n\nNo valid data found for any of the requested technologies: {techs_to_run}")

    atb_data_final = pd.concat(validated_data_list, ignore_index=True)
    return atb_data_final

def get_atb_file_path(tech, atbyear, scenario, settings):
    """
    function to construct full filepath and per-tech ATB filename

    Parameters
    ----------
    tech: str
        Tech identifier used as the file prefix (e.g., 'gas', 'upv' etc.)
    atbyear: int
        ATB year (e.g., 2025) to process
    scenario: str
        ATB scenario (Moderate/Advanced) from settings.yaml
    settings: dict
        Loaded settings from settings.yaml
    """
    # allow per-tech override of the filename in existing ReEDS input files
    # e.g. settings['techs']['wind-ons']['reeds_name'] = "ons-wind"
    filename_root = settings['techs'].get(tech, {}).get('reeds_name', tech)
    filename = f"{filename_root}_ATB_{atbyear}_{scenario.lower()}"
    filepath = os.path.join(
        settings['reedspath'], 'inputs', 'plant_characteristics', filename + ".csv"
    )
    return filename, filepath

def check_columns(df, col_list, setting, tech):
    """
    function to check that all columns specified in settings.yml exist in the data

    Parameters
    ----------
    col_list: list[str]
        Sequence of required column names based on tech settings
    setting: str
        Settings key being validated (used in the error message, e.g. 'indexcols' or 'cols')
    tech: str
        Technology identifier
    """
    missing_cols = [c for c in col_list if c not in df.columns]
    if len(missing_cols) > 0:
        raise ValueError(
            f"The following columns are specified as '{setting}' but are not in the data: {missing_cols}. "
            f"Please update your 'settings.yaml' for {tech}."
            )

def load_historic_atb_files(tech_data, tech, settings, dollaryear, deflator):
    """
    function to load previous ATB and historic capacity factors

    Parameters
    ----------
    tech_data: pd.DataFrame
        ATB dataframe for the techs (after subset and pivoting); must contain 'Scenario' column and index columns from settings.yaml
    tech: str
        Technology key name in settings['techs']
    settings: dict
        Parsed settings.yaml values (paths, indexcols, cost_cols, atbyear, reeds_start_year, etc.)
    dollaryear: pd.Series
        Mapping of scenario -> dollar year from dollaryear.csv
    deflator: pd.Series
        Mapping of dollar year -> deflator multiplier from deflator.csv
    """
    # load previous year ATB files from ReEDS
    tech_data_historic = []
    prevyear = settings['atbyear']-1
    tech_settings = settings['techs'][tech]

    # determine id columns (index cols excluding Scenario and year)
    idcols = [c for c in tech_settings['indexcols'] if c not in ['Scenario', 't']]
    # iterate over scenarios to load for historic data
    for scenario in tech_data['Scenario'].unique():
        prev_file, filepath = get_atb_file_path(tech, prevyear, scenario, settings)
        # try tech-specific historic filename override before falling back to moderate
        if not os.path.isfile(filepath):
            hist_name = tech_settings.get('filename_historical')
            if hist_name:
                hist_path = os.path.join(settings['reedspath'], 'inputs', 'plant_characteristics', hist_name)
                if os.path.isfile(hist_path):
                    print(f"...using historic override file {hist_name} for {tech} / {scenario}")
                    prev_file = os.path.splitext(hist_name)[0]
                    filepath = hist_path
                else:
                    print(f"...historic override {hist_name} not found; continuing to fallback logic.")
            # check that file exists
            if not os.path.isfile(filepath):
                # use moderate as a fallback (if there's no moderate file throw an error)
                prev_file, filepath = get_atb_file_path(tech, prevyear, 'moderate', settings)
                if not os.path.isfile(filepath):
                    # if backfilling data defer adjustments until that step
                    if 'backfill_data' in tech_settings.get('functions', []):
                        return tech_data
                    else:
                        raise FileNotFoundError(f"Could not find {filepath}.\n"
                        "If previous ATB files are not available, you can add 'backfill_data' as a "
                        "function to this tech in the settings.yml file and rerun to fill in earlier data."
                        )
        dfin = pd.read_csv(filepath)
        dfin['Scenario'] = scenario

        # ensure historic file columns match the internal names expected by settings
        # apply per-tech rename mapping if present so we can use settings['cost_cols'] reliably
        if 'renamecols' in tech_settings:
            dfin = dfin.rename(columns=tech_settings['renamecols'])

        # quick guard: if cost_cols still missing, show helpful message and raise
        missing_cost_cols = [c for c in settings['cost_cols'] if c not in dfin.columns]
        if missing_cost_cols:
            raise KeyError(
                f"Historic file {filepath} is missing expected cost columns: {missing_cost_cols}. "
                f"Available columns: {list(dfin.columns)}. "
                "Add a 'renamecols' mapping for this tech in settings.yaml or update settings['cost_cols']."
            )
        # restrict historic rows to only sub-tech identifiers present in current tech_data
        for idcol in idcols:
            if idcol in dfin.columns and idcol in tech_data.columns:
                allowed = pd.Series(tech_data[idcol].unique())
                dfin = dfin[dfin[idcol].isin(allowed)]
        # skip empty historic chunk (don't append empty frames)
        if dfin.empty:
            continue

        # convert any cost columns to current dollar year (deflate to 2004, then inflate to present year)
        try:
            inflator = deflator[dollaryear[prev_file]] / deflator[settings['dollaryear']] 
        except KeyError:
            print(f"ERROR: {prev_file} not found in the dollaryear.csv file.")
            sys.exit(1)

        dfin[settings['cost_cols']] = dfin[settings['cost_cols']] * inflator
        tech_data_historic.append(dfin)
    if tech_data_historic:
        tech_data_historic = pd.concat(tech_data_historic).reset_index(drop=True)
    else:
        tech_data_historic = pd.DataFrame(columns=tech_data.columns)

    # for techs with cf_improvement we need to reset using the historical cf values before normalizing
    tech_settings = settings['techs'][tech]
    if 'cf_improvement' in tech_data_historic:
        historic_cf = pd.read_csv(os.path.join("input_data", "historic_capacity_factors.csv"))
        # merge on all indexcols except scenario and rsc_mult
        mergecols = [col for col in tech_settings['indexcols'] if col not in ['Scenario', 'rsc_mult']]
        historic_cf = historic_cf.loc[historic_cf.tech == tech, mergecols + ['cf']]
        tech_data_historic = tech_data_historic.merge(historic_cf, on=mergecols, how="left")
        tech_data_historic = tech_data_historic.assign(cf_improvement=tech_data_historic['cf']).drop('cf', axis=1)
        # fill historic cf values forward
        fillcols = [col for col in tech_settings['indexcols'] if col not in ['t', 'rsc_mult']]
        tech_data_historic['cf_improvement'] = tech_data_historic.groupby(fillcols)['cf_improvement'].fillna(method='ffill')
    
    # combine and drop duplicate any rows that overlap with new ATB values
    tech_data['atbyear'] = int(settings['atbyear'])
    tech_data_historic['atbyear'] = int(prevyear)
    tech_data = pd.concat([tech_data_historic, tech_data])
    tech_data = tech_data.drop_duplicates(subset=tech_settings['indexcols'], keep="last")
    tech_data = tech_data.reset_index(drop=True)

    return tech_data

def normalize_cf(tech, settings, df):
    """
    function to normalize capacity factor to relevant base year and scenario (specified by 'cfbase')

    Parameters
    ----------
    tech: str
        Technology key in settings['techs'] for which cf_improvement values will be normalized
    settings: dict
        Parsed settings.yaml values. Expects settings['techs'][tech]['cfbase'] to define the base
        selection. Example cfbase entries:
            - keys with exact match values to select the base row(s)
            - optional 'keepcols' list to indicate merge keys when multiple columns define the base
            - 't' may be used to indicate the base year
    """
    print("Normalizing cf_improvement")
    tech_settings = settings['techs'][tech]
    cf_base = df.copy()
    for k,v in tech_settings['cfbase'].items():
        if k in df.columns:
            print(f"...base {k}={v}")
            cf_base = cf_base.loc[(cf_base[k] == v)]
    if 'keepcols' in tech_settings['cfbase']:
        cf_base = cf_base[tech_settings['cfbase']['keepcols'] + ['cf_improvement']].rename(columns={"cf_improvement":"cf_base"})
        df = df.merge(cf_base, on=tech_settings['cfbase']['keepcols'], how='left')
        df['cf_improvement'] = df['cf_improvement'] / df['cf_base']
        df = df.drop(columns='cf_base')
    else:
        if len(cf_base) > 1:
            raise ValueError("Error: more than one value found for cf_improvement without specifying which columns to keep.")
        else:
            df['cf_improvement'] = df['cf_improvement'] / cf_base['cf_improvement'].values[0]

    # check that base level is properly normalized to 1
    assert (df.loc[cf_base.index, 'cf_improvement'] == 1).all(), "Check cf_improvement normalization."

    return df

def smooth_hist_cf(tech, settings, df):
    """
    function to interpolate historical capacity factor values through 2035

    Parameters
    ----------
    tech: str
        Technology key in settings['techs'] to use when looking up cfbase/base year
    settings: dict
        Expects settings['techs'][tech]['cfbase']['t'] to specify the base year for interpolation
    df: pd.DataFrame
        Input dataframe containing ['Scenario', 't', 'cf_improvement'] columns
    """
    # sort by scenario and year
    df = df.sort_values(by=['Scenario', 't'])
    # remove values between 2022 (ATB starts in 2023) and atb base year (usually 2035)
    tech_settings = settings['techs'][tech]
    baseyear = tech_settings['cfbase']['t']
    df['cf_improvement'] = np.where((df['t'] > 2022) & (df['t'] < baseyear), np.nan, df['cf_improvement'])
    # interpolate to fill dropped values
    df['cf_improvement'] = df.groupby('Scenario')['cf_improvement'].transform(lambda x: x.interpolate(method='linear'))

    return df

def add_coal_techs(tech, settings, df, deflator, techcol='i'):
    """
    function to copy costs for existing coal techs from coal-new

    Parameters
    ----------
    tech: str
        Technology key in settings['techs']
    settings: dict
        Parsed settings.yaml values
    df: pd.DataFrame
        Must include `techcol` and cost columns for base coal technology
    deflator: pd.Series
        Used to convert constant-dollar adder amounts to the ATB dollar year
    techcol: str, optional
        Column name identifying the technology label in `df` (default 'i')

    """
    # list of "new" coal techs to add
    coal_techs = ['CoalOldUns', 'CoalOldScr', 'CofireNew', 'CofireOld']
    # existing tech from which to get costs
    base_coal_tech = 'Coal-new'
    # for cofire plants include a fixed adder to capital cost
    # originally 305 $/kW in 2017$, so convert to current dollar year
    cofire_adder = 305 * deflator.loc[2017] / deflator.loc[settings['dollaryear']]
    
    df_add_all = []
    for ct in coal_techs:
        # copy and rename data
        print(f"...adding data for {ct} using {base_coal_tech}")
        df_add = df.loc[df[techcol] == base_coal_tech].copy()
        df_add[techcol] = ct
        # include cofire adder
        if 'Cofire' in ct:
            print(f"...including cofire adder")
            df_add['capcost'] += cofire_adder
        # add to list
        df_add_all.append(df_add)
    
    # combine entries and merge with original data
    df_add_all = pd.concat(df_add_all)
    df_out = pd.concat([df, df_add_all])
    # drop duplicates. keep values added here since previous ones
    # come from the historic files and we want to preserve the newer ATB when available.
    tech_settings = settings['techs'][tech]
    df_out = df_out.drop_duplicates(subset=tech_settings['indexcols'], keep="last")

    return df_out 

def add_beccs_techs(tech, settings, df, techcol='i'):
    """
    function to copy costs for beccs_mod to beccs_max

    Parameters
    ----------
    tech: str
        Technology key in settings['techs']
    df: pd.DataFrame
        Must include column `techcol` for beccs technologies
    techcol: str, optional
        Column name identifying the technology label in `df` (default 'i')
    """
    # copy beccs_max from beccs_mod
    df_add = df.copy()
    df_add['i'] = "beccs_max"
    df_out = pd.concat([df, df_add])
    
    return df_out

def format_continuous_battery(tech, settings, df):
    """
    function to format battery technology data

    Parameters
    ----------
    tech: str
        Technology key from settings (used for logging)
    settings: dict
        Parsed settings.yaml values (for expected column names)
    df: pd.DataFrame
        Input dataframe with columns that will be replaced by the battery cost fields (the function drops ['capcost', 'fom'] before merging)
    """
    # load battery power and energy costs (manually created file using ATB workbook)
    battery_costs = pd.read_csv(os.path.join('input_data', f"battery_costs_{settings['atbyear']}.csv"))
    # reshape and format
    battery_costs = pd.melt(battery_costs, id_vars=['cost','Scenario'], var_name='t')
    battery_costs = battery_costs.pivot(index=['Scenario', 't'], columns='cost', values='value').reset_index().rename_axis(None, axis=1)
    battery_costs['t'] = pd.to_numeric(battery_costs['t'], errors='coerce').astype('Int64')
    # assign FOM (assumed to be 2.5% of capital costs)
    fom_mult = 0.025
    battery_costs['fom'] = battery_costs['capcost'] * fom_mult
    battery_costs['fom_energy'] = battery_costs['capcost_energy'] * fom_mult
    # merge with placeholder data and return
    df = df.drop(['capcost', 'fom'], axis=1).merge(battery_costs)
    # if rte is missing from the ATB data, assign 0.85 as the default value
    if 'rte' not in df.columns:
        print("Warning: 'Round-Trip Efficiency' not found in ATB data for battery. Assigning default rte = 0.85.")
        df['rte'] = 0.85
    return df 

def add_csp_techs(tech, settings, df, techcol='i'):
    """
    function to create and append CSP tech types by scaling base tech values

    Parameters
    ----------
    tech: str
        Technology key in settings['techs']
    settings: dict
        Parsed settings.yaml values to determine index columns and cost column names
    df: pd.DataFrame
        Must include `techcol` and cost columns listed in settings['cost_cols'])
    techcol: str, optional
        Column name identifying the technology label in `df` (default 'i').
    """
    # load cost ratios for csp techs
    csp_ratios = pd.read_csv(os.path.join("input_data", f"csp_cost_ratios_{settings['atbyear']}.csv"))
    print("updating csp tech costs using the following ratios:")
    print(csp_ratios[['type','ratio']])

    # merge with data
    base_csp_tech = csp_ratios.loc[csp_ratios.base_tech==1, "type"].squeeze()
    if isinstance(base_csp_tech, pd.Series):
        raise ValueError("Multiple base csp techs specified in 'csp_cost_ratios.csv")
    
    # loop through non-base csp techs
    df_add_all = []
    for _,row in csp_ratios.loc[csp_ratios.base_tech==0].iterrows():
        print(f"...updating cost data for {row['type']} using multiplier of {row['ratio']}")
        # get data for baseline csp tech
        df_add = df.loc[df.type == base_csp_tech].copy()
        # update tech name
        df_add['type'] = row['type']
        # apply multiplier to cost fields
        df_add[settings['cost_cols']] *= row['ratio']
        # add to list
        df_add_all.append(df_add)
    
    # combine entries and merge with original data
    df_add_all = pd.concat(df_add_all)
    df_out = pd.concat([df, df_add_all])
    # drop duplicates. keep values added here since previous ones
    # come from the historic files and we want to preserve the newer ATB when available.
    tech_settings = settings['techs'][tech]
    df_out = df_out.drop_duplicates(subset=tech_settings['indexcols'], keep="last")

    return df_out

def backfill_data(tech, settings, df):
    """
    Function to backfill historic years' values with the first observed ATB-year values from the flat file

    Parameters
    ----------
    tech: str
        Technology key in settings['techs']
    settings: dict
        Uses:
        - settings['reeds_start_year'] (start year to backfill to)
        - settings['techs'][tech]['indexcols'] (grouping keys)
        - settings['atbyear'] (to detect current vs historic rows)
    df: pd.DataFrame
        Must include year column 't' and index columns defined in settings['techs'][tech]['indexcols']
    """
    # get index and value columns, including indexcols without year
    indexcols = settings['techs'][tech]['indexcols']
    indexcols_noyear = [i for i in indexcols if i != "t"] 
    valcols = [c for c in df.columns if c not in indexcols]

    # check if historic data has been assigned
    if "atbyear" in df.columns:
        # set all data from year before this atb to missing
        df.loc[df.atbyear != settings['atbyear'], valcols] = np.nan
    else:
        # if not assign data through ReEDS start year
        settings['reeds_start_year']
        # identify first year by technology
        first_atb_year = df.groupby(by=indexcols_noyear)['t'].min()
        backfill_df = pd.DataFrame()
        # create empty data
        for t_add in range(settings['reeds_start_year'], first_atb_year.max()):
            df_add = first_atb_year.copy()
            df_add[:] = t_add
            backfill_df = pd.concat([backfill_df, pd.DataFrame(df_add[df_add < first_atb_year])])
        backfill_df = backfill_df.reset_index()
        df = pd.concat([backfill_df, df])
    
    # now backfill by group using indexcols except t
    df[valcols] = df.groupby(indexcols_noyear)[valcols].bfill()

    return df

def process_tech_file(atb_data, tech, settings, filenames, dollaryear, deflator, sensitivity_name, outfolder, args):
    """
    function to format per-technology output files

    Parameters
    ----------
    atb_data: pd.DataFrame
        Technology-level data from ATB flat-file
    tech: str
        Technology-specific key in settings['techs'] to process
    settings: dict
        Parsed settings.yaml values
    filenames: list
        List to which produced filenames will be appended.
    dollaryear: pd.Series
        Mapping of scenario -> dollar year
    deflator: pd.Series
        Mapping of dollar year -> deflator multiplier
    sensitivity_name: str or None
        Optional suffix added to output filenames for sensitivity runs
    outfolder: str
        Directory path where output CSVs will be written
    args: argparse.Namespace
        CLI arguments for debug mode and run options
    """
    # subset to relevant technology
    print(f"Processing {tech} file.")
    tech_settings = settings['techs'][tech]
    tech_data = atb_data.loc[atb_data['Technology'] == tech_settings['Technology']]
    # apply tech-specific subset_rows (Case, CRPYears)
    if 'subset_rows' in tech_settings:
        tech_data, _ = subset_atb_rows(tech_settings['subset_rows'], tech_data)
    else:
        raise ValueError(f"Missing 'subset_rows' for {tech} in settings.yaml")
    # get subtech(s), specified either as a dictionary or a single string
    if 'DisplayName' in tech_settings:
        if isinstance(tech_settings['DisplayName'], dict):
            tech_data = tech_data.loc[tech_data['DisplayName'].isin(tech_settings['DisplayName'].keys())]
            tech_data = tech_data.replace({'DisplayName': tech_settings['DisplayName']})
            # verify all expected subtechs were found in the data
            missing_subtechs = set(tech_settings['DisplayName'].keys()) - set(tech_data['DisplayName'].unique())
            if missing_subtechs:
                print(f"Warning: the following DisplayName(s) for {tech} were not found in the ATB subset: {sorted(missing_subtechs)}")
        else:
            print(f"Subsetting to {tech_settings['DisplayName']}")
            tech_data = tech_data.loc[tech_data['DisplayName'] == tech_settings['DisplayName']]
    if tech_data.empty:
        if args.debug:
            breakpoint()
        else:
            raise EmptyDataError(f"\n\nNo data found for {tech} in ATB data. Check file for values for specified subset_rows.")
    
    # add in columns as needed
    if 'addcols' in tech_settings:
        print(f"Adding columns {tech_settings['addcols']}")
        for col in tech_settings['addcols']:
            tech_data[col] = tech_settings['addcols'][col]

    # rename columns as needed
    if 'renamecols' in tech_settings:
        print(f"Renaming columns {tech_settings['renamecols']}")
        tech_data = tech_data.rename(columns=tech_settings['renamecols'])

    # grab columns needed for this particular tech
    atb_params = [k for k, v in settings['param_names'].items() if v in tech_settings['cols']]
    tech_data_out = tech_data.loc[tech_data.Parameter.isin(atb_params)].copy()
    # check if empty    
    if tech_data_out.empty:
        raise EmptyDataError("\n\n"
                            "Dataframe is empty after subsetting; check 'param_names' in settings.yaml."
                            f"Current values: {settings['param_names']}"
        )
    # check columns and then pivot to wide format
    check_columns(tech_data_out, tech_settings['indexcols'], 'indexcols', tech)
    tech_data_out = tech_data_out.pivot(index=tech_settings['indexcols'], columns='Parameter', values='value')
    tech_data_out = tech_data_out.rename_axis(None, axis=1)
    # map parameters to new column names and format
    tech_data_out = tech_data_out.rename(columns=settings['param_names'])
    # convert numeric columns 
    tech_data_out = tech_data_out.apply(pd.to_numeric).reset_index()

    # if vom is missing in the input ATB data, assign zero
    if tech in zero_vom_techs and 'vom' not in tech_data_out.columns:
        print(f"Warning: 'Variable O&M' missing in ATB data for {tech}. Assigning vom = 0.0")
        tech_data_out['vom'] = 0.0

    # if processing continuous battery techs update energy cost data
    if 'battery' in tech:
        tech_data_out = format_continuous_battery(tech, settings, tech_data_out)

    # add in historic values
    tech_data_out = load_historic_atb_files(tech_data_out, tech, settings, dollaryear, deflator)

    # run any additional modification functions
    if 'functions' in tech_settings:
        for function_name in tech_settings['functions']:
            selected_function = FUNCTION_MAPPING.get(function_name.lower())
            if selected_function:
                print(f"Running {function_name}")
                if function_name == "add_coal_techs":
                    tech_data_out = selected_function(tech, settings, tech_data_out, deflator)
                else:
                    tech_data_out = selected_function(tech, settings, tech_data_out)
            else:
                raise NameError(f"'{function_name}' is not a supported function. "
                                "Define and add to FUNCTION_MAPPING.")

    # for fuel cells, backfill capcost values as 9999 for all pre-2035 years
    if tech == 'fuelcell' and 'capcost' in tech_data_out.columns:
        print("Assigning capcost = 9999 for fuelcell years before 2035")
        tech_data_out.loc[tech_data_out['t'] < 2035, 'capcost'] = 9999

    ## save to file by scenario
        
    # if moderate is a scenario, process it first so we use it to check for duplicates
    tech_data_out['Scenario'] = tech_data_out['Scenario'].str.lower()
    scenarios = list(tech_data_out['Scenario'].unique())
    if 'moderate' in scenarios:
        scenarios.insert(0, scenarios.pop(scenarios.index('moderate')))

    for scenario in scenarios:
        if sensitivity_name is not None:
            filename = f"{tech}_ATB_{settings['atbyear']}_{scenario}_{sensitivity_name}.csv"
        else:
            filename = f"{tech}_ATB_{settings['atbyear']}_{scenario}.csv"
        # subset to scenario
        check_columns(tech_data_out, tech_settings['cols'], 'cols', tech)
        scendata = tech_data_out.loc[tech_data_out.Scenario == scenario, tech_settings['cols']]
        # round to specified decimal places
        scendata = scendata.round(settings['decimals'])
        # sort in order of columns
        scendata = scendata.sort_values(by=scendata.columns.to_list()).reset_index(drop=True)

        # if processing the moderate case or scendata_out doesn't exist, save current data to scendata_out
        if scenario == "moderate" or 'scendata_out' not in locals():
            scendata_out = scendata.copy()
        # otherwise check if file is identical to moderate case
        else:
            # if current case is identical to the moderate, skip saving it and continue to next scenario
            if scendata.equals(scendata_out):
                print(f"...{scenario} is identical to the moderate scenario, skipping.")
                continue
            else:
                scendata_out = scendata.copy()

        # save file
        print(f"Saving {filename}")
        scendata_out.to_csv(os.path.join(outfolder, filename), index=False)
        # keep list of filenames for copying to ReEDS later
        filenames.append(filename)

def update_dollaryear(settings, filenames, dollaryear):
    """
    function to create dollar year entries for new tech files

    Parameters
    ----------
    settings: dict
        Parsed settings['dollaryear'] value as the dollar year to assign to the new files
    filenames: list[str]
        Output filename produced by this run; strips the ".csv" suffix to derive Scenario names
    dollaryear: pd.Series
        Scenario -> Dollar.Year mapping (index=Scenario, name='Dollar.Year')
    """
    # strip .csv from filenames
    scennames = [f[:-4]  for f in filenames]
    # create new dollar year entries
    newdollaryear = pd.Series(data=[settings['dollaryear']] * len(scennames), index=scennames)
    # combine old and new and format for ReEDS
    newdollaryear = pd.concat([dollaryear, newdollaryear]).sort_index()
    # drop duplicates
    newdollaryear = newdollaryear[~newdollaryear.index.duplicated(keep='first')]
    newdollaryear.name = "Dollar.Year"
    newdollaryear.index.name = "Scenario"

    return newdollaryear

def get_financials(atb_data, settings, tech, financial_params, scenarios=["Moderate","*"]):
    """
    function to get full financial parameters for a specific technology

    Parameters
    ----------
    atb_data: pd.DataFrame
        ATB flat file data
    settings: dict
        Parsed 'decimals' and 'reeds_start_year' values
    tech: str
        ATB technology name to filter to
    financial_params: dict
        Mapping of ATB parameter name -> target column name for pivoting
    scenarios: list[str], optional
        List of scenario names to include (default ["Moderate","*"])
    """
    techfinancial = atb_data.loc[(atb_data.Technology == tech) 
                                & (atb_data.Scenario.isin(scenarios))
                                & (atb_data.Parameter.isin(financial_params))
                                ].copy()
    if techfinancial.empty:
        avail_params = atb_data.loc[(atb_data.Technology == tech) 
                                    & (atb_data.Scenario.isin(scenarios)), "Parameter"
                                    ].unique()
        if args.debug:
            breakpoint()
        else:

            raise EmptyDataError(f"Looking for the following financial parameters for {tech}:\n{list(financial_params.keys())}.\n"
                                 f"Found the following parameters:\n{list(avail_params)}"
                                 )
    techfinancial = techfinancial.rename(columns={"variable":"t"})
    # reorder columns
    techfinancial = techfinancial.pivot(index="t", columns='Parameter', values='value').rename(columns=financial_params)
    colorder = [v for k,v in financial_params.items()]
    # convert to numeric and round
    techfinancial = techfinancial[colorder].apply(pd.to_numeric).round(settings['decimals'])
    # reindindex to include years before atb
    atb_end_year = techfinancial.index.values.max()
    techfinancial = techfinancial.reindex(index=list(range(settings['reeds_start_year'],atb_end_year+1))).fillna(method='bfill').reset_index()

    return techfinancial

def update_financials(settings, atb_data, outfolder):
    """
    function to write updated system and tech financial outputs

    Parameters
    ----------
    settings: dict
        Expects keys such as 'atbyear', 'reedspath', 'wacc_base_tech', 'decimals', and tech-specific configs under settings['techs'])
    atb_data: pd.DataFrame
        ATB flat file data used to extract financial parameters and WACC
    outfolder: str
        Output directory path
    """
    print("Processing system financials.")

    financial_params = {
        'Interest Rate Nominal':'interest_rate_nom',
        'Rate of Return on Equity Nominal':'rroe_nom',
        'Debt Fraction':'debt_fraction',
        'Tax Rate (Federal and State)':'tax_rate'
    }

    ## system financial file
    # uses base tech specified in settings (typically natural gas)
    sysfinancial = get_financials(atb_data, settings, settings['wacc_base_tech'], financial_params)
    
    ## tech financials
    # get old tech financials file
    print("Processing tech financials.")
    prevyear = settings['atbyear']-1
    filename_old = f"financials_tech_ATB{prevyear}.csv"
    financials_old = pd.read_csv(os.path.join(settings['reedspath'], 'inputs', 'financials', filename_old))

    wacc = atb_data.loc[(atb_data['Parameter'] == "WACC Real") & (atb_data['Scenario'] == "Moderate"), ["Technology", "variable", "value"]]
    wacc = wacc.rename(columns={"variable": "t", "value":"wacc_real"})
    wacc['wacc_real'] = pd.to_numeric(wacc['wacc_real'])
    # use natural gas as baseline
    wacc_baseline = wacc.loc[wacc.Technology == settings['wacc_base_tech']]
    wacc_baseline = wacc_baseline.rename(columns={"wacc_real":"wacc_real_base"}).drop("Technology", axis=1)
    
    wacc = wacc.merge(wacc_baseline, on=['t'])
    # get financing difference relative to base
    wacc['finance_diff_real_update'] = wacc['wacc_real'] - wacc['wacc_real_base']

    # get WACC values for techs that were processed
    financials_update = []
    for tech in settings['techs']: 
        print(f"Loading financials for {tech}")

        # for batteries we need to calculate the WACC Real value
        if tech == "battery":
            # batteries are missing WACC, so need to get individual components and recalculate
            # since we don't have debt fraction for batteries we take the value from PV for now
            battery_params = financial_params.copy()
            battery_params.pop('Debt Fraction')
            battery_params.update({'Inflation Rate':'inflation_rate'})
            pv_param = {'Debt Fraction': financial_params['Debt Fraction']}
            battfinancial = get_financials(atb_data, settings, settings['techs'][tech]['Technology'], battery_params)
            pvfinancial = get_financials(atb_data, settings, settings['techs']['upv']['Technology'], pv_param)
            # merge and calculate WACC in two steps:
            # 1. WACC Nominal = Debt Fraction * Interest Rate Nominal * (1 - Tax Rate) + (1 - Debt Fraction) * Rate of Return
            # 2. WACC Real = (1 + WACC Nominal) / (1 + Inflation Rate) - 1
            wacc_tech = battfinancial.merge(pvfinancial)
            wacc_tech['wacc_nom'] = (wacc_tech['debt_fraction'] 
                                     * wacc_tech['interest_rate_nom'] 
                                     * (1 - wacc_tech['tax_rate']) 
                                     + (1 - wacc_tech['debt_fraction'])
                                     * wacc_tech['rroe_nom']
            )
            wacc_tech['wacc_real'] = (
                (1 + wacc_tech['wacc_nom']) / (1 + wacc_tech['inflation_rate']) - 1 
            ) 
            wacc_tech = wacc_tech.merge(wacc_baseline, on=['t'])
            wacc_tech['finance_diff_real_update'] = wacc_tech['wacc_real'] - wacc_tech['wacc_real_base']

        else:
            wacc_tech = wacc.loc[wacc.Technology == settings['techs'][tech]['Technology']].copy()

        if wacc_tech.empty:
            print(f"...no wacc found for {tech}, skipping.")
        else:
            # overwrite tech with new financial name if specified
            if 'financialname' in settings['techs'][tech]:
                wacc_tech['i'] = settings['techs'][tech]['financialname']
                wacc_tech = wacc_tech.drop_duplicates()
            # otherwise use exist tech name
            else:
                wacc_tech = wacc_tech.rename(columns={'Technology':'i'})
            # add to list of new financials to update
            financials_update.append(wacc_tech[['i', 't', 'finance_diff_real_update']])
            
    financials_update = pd.concat(financials_update)

    # merge with old file and rewrite new year values upto the max available ATB year
    max_atb_year = int(financials_update['t'].max())
    max_old_year = int(financials_old['t'].max())
    if max_atb_year > max_old_year:
        new_years = list(range(max_old_year + 1, max_atb_year + 1))
        # get unique (i, country) combinations from old file
        id_combos = financials_old[['i', 'country']].drop_duplicates()
        # create new rows for each (i, country) × new year
        new_rows = id_combos.merge(pd.DataFrame({'t': new_years}), how='cross')
        # merge old file's non-year columns onto new rows (forward-fill from last known year)
        last_year_vals = financials_old.loc[financials_old['t'] == max_old_year].drop('t', axis=1)
        new_rows = new_rows.merge(last_year_vals, on=['i', 'country'], how='left')
        financials_old = pd.concat([financials_old, new_rows], ignore_index=True)
    financials_out = financials_old.merge(financials_update, on=['i', 't'], how='left')
    # fill forward by country and tech to create values for pre-ATB years
    financials_out['finance_diff_real_update'] = financials_out.groupby(['i','country'])['finance_diff_real_update'].fillna(method='bfill')
    
    # update relevant techs while leaving others in place
    financials_out['finance_diff_real'] = np.where(financials_out['finance_diff_real_update'].isna(), 
                                                   financials_out['finance_diff_real'], 
                                                   financials_out['finance_diff_real_update'].round(settings['decimals'])
                                                   )

    # write system and tech financial files
    sysfile_new = f"financials_sys_ATB{settings['atbyear']}.csv"
    sysfinancial.to_csv(os.path.join(outfolder, sysfile_new),index=False)

    techfile_new = f"financials_tech_ATB{settings['atbyear']}.csv"
    financials_out = financials_out[financials_old.columns]
    financials_out.to_csv(os.path.join(outfolder, techfile_new),index=False)

    return sysfile_new, techfile_new

#%% ===========================================================================
### --- Main ---
### ===========================================================================
def main(args):    
    # load yaml file with settings
    yamlfile = 'settings.yaml'
    with open(yamlfile) as f:
        settings = yaml.safe_load(f)
    thisdir = os.path.dirname(os.path.abspath(__file__))
    outfolder = os.path.join(thisdir, 'output')

    # if output folder does not exist, create it
    if not os.path.exists(outfolder):
        os.mkdir(outfolder)
    
    # get list of techs to process
    if args.techs[0] == 'all':
        techs_to_run = list(settings['techs'].keys())
    else: 
        techs_to_run = args.techs
        missing_techs = [t for t in techs_to_run if t not in settings['techs']]
        if missing_techs:
            raise ValueError(f"The following technologies are not in settings['techs']: {missing_techs}")
    
    # check if ReEDS repo is correctly specified
    if not os.path.isdir(settings['reedspath']):
        raise FileNotFoundError(f"Could not find '{settings['reedspath']}'; check path in settings.yaml.")
    
    # load dollaryear file in ReEDS
    dollaryear = pd.read_csv(os.path.join(settings['reedspath'], 'inputs', 'plant_characteristics', 'dollaryear.csv'), 
                             index_col='Scenario').squeeze()
    deflator = pd.read_csv(os.path.join(settings['reedspath'], 'inputs', 'financials', 'deflator.csv'), 
                           index_col='*Dollar.Year').squeeze()
    # load ATB flat file
    atb_data = load_atb_flat_file(settings, args, techs_to_run)

    ## process technology files
    filenames = []
    if args.skip_costs:
        print("Skipping cost files.")
    else:
        print("---------------------")
        for tech in techs_to_run:    
            process_tech_file(atb_data, tech, settings, filenames, 
                            dollaryear, deflator, args.sensitivity_name, outfolder, args)
            print("---------------------")
        # update dollaryear file
        newdollaryear = update_dollaryear(settings, filenames, dollaryear)
    
    ## update financials file
    if args.skip_financials:
        print("Skipping financials.")
    else:
        sysfinancialfile, techfinancialfile = update_financials(settings, atb_data, outfolder)

    ## copy new files to ReEDS
    if settings['copy_to_reeds']:

        if not args.skip_costs:
            # copy tech files
            for f in filenames:
                shutil.copy(os.path.join(outfolder,f), os.path.join(settings['reedspath'],'inputs','plant_characteristics',f))
            # update dollaryear file for tech files
            newdollaryear.to_csv(os.path.join(settings['reedspath'],'inputs','plant_characteristics','dollaryear.csv'), index=True)
        
        # copy financial files
        if not args.skip_financials:
            shutil.copy(os.path.join(outfolder,sysfinancialfile), os.path.join(settings['reedspath'],'inputs','financials',sysfinancialfile))
            shutil.copy(os.path.join(outfolder,techfinancialfile), os.path.join(settings['reedspath'],'inputs','financials',techfinancialfile))


if __name__ == "__main__":
    print("Processing ATB files")
    parser = argparse.ArgumentParser(description="Generate ATB files.")
    parser.add_argument('--techs', '-t', nargs='+', default=['all'],
                    help='1 or more strings with techs that should be processed.')
    parser.add_argument('--sensitivity_name', '-s', type=str, 
                    help='suffix to append to file name for sensitivities')
    parser.add_argument('--skip_financials', '-f', action="store_true",
                    help='skip updating financials for this run')
    parser.add_argument('--skip_costs', '-c', action="store_true",
                    help='skip updating cost files for this run')
    parser.add_argument('--debug', '-d', action="store_true",
                    help='option to run in debug mode')
    args = parser.parse_args()

    # list of supported custom functions to call from settings.yml
    FUNCTION_MAPPING = {
        'normalize_cf': normalize_cf,
        'smooth_hist_cf': smooth_hist_cf,
        'add_coal_techs': add_coal_techs,
        'add_csp_techs': add_csp_techs,
        'add_beccs_techs': add_beccs_techs,
        'backfill_data': backfill_data
    }
    main(args)
