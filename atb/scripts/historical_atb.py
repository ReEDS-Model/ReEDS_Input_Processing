"""Map archived ATB base-year estimates into ReEDS historical series."""

from pathlib import Path
import re

import numpy as np
import pandas as pd

from atb_config import resolve_atb_path


MONETARY = {'capcost', 'capcost_energy', 'fom', 'fom_energy', 'vom'}
PARAMETERS = {
    'OCC': 'capcost', 'Fixed O&M': 'fom', 'Variable O&M': 'vom',
    'Heat Rate': 'heatrate', 'CF': 'cf_improvement',
    'Round-Trip Efficiency': 'rte',
}


def archive_path(config):
    source = config['historical_atb']
    return resolve_atb_path(source['directory']) / source['normalized_filename']


def technology_series(technology, detail, vintage):
    """Return a reviewed ReEDS family/series, or None for unmatched designs."""
    name = str(detail).strip()
    name = re.sub(r'\s*-\s*(Advanced|Moderate|Conservative)$', '', name)
    name = name.split(' -> ')[0]
    group = technology.replace('Conventional - ', '').replace('_FE', '')
    if group in ('Coal', 'Coal_FE'):
        if name in ('Coal-new', 'Coal-new-AvgCF', 'newAvgCF'):
            return 'coal', 'Coal-new'
        if name in ('Coal-IGCC', 'Coal-IGCC-AvgCF', 'IGCCAvgCF'):
            return 'coal', 'Coal-IGCC'
        if name in ('Coal-95%-CCS', 'Coal-CCS-95%'):
            return 'coal-ccs', 'coal-CCS_mod'
    if group in ('Gas', 'Natural Gas', 'NaturalGas'):
        names = {
            'Gas-CC': 'Gas-CC', 'Gas-CC-AvgCF': 'Gas-CC', 'CCAvgCF': 'Gas-CC',
            'Gas-CT': 'Gas-CT', 'Gas-CT-AvgCF': 'Gas-CT', 'CTAvgCF': 'Gas-CT',
            'NG F-Frame CC': 'Gas-CC', 'NG F-Frame CT': 'Gas-CT',
            'NG Combined Cycle (F-Frame)': 'Gas-CC',
            'NG 2-on-1 Combined Cycle (F-Frame)': 'Gas-CC',
            'NG Combustion Turbine (F-Frame)': 'Gas-CT',
            'NG 1-on-1 Combined Cycle (H-Frame)': 'Gas-CC_H_1x1',
            'NG 2-on-1 Combined Cycle (H-Frame)': 'Gas-CC_H_2x1',
        }
        if name in names:
            return 'gas', names[name]
        ccs = {
            'NG Combined Cycle (F-Frame) 95% CCS': 'Gas-CC-CCS_mod',
            'NG 2-on-1 Combined Cycle (F-Frame) 95% CCS': 'Gas-CC-CCS_mod',
            'NG 1-on-1 Combined Cycle (H-Frame) 95% CCS': 'Gas-CC_H_1x1-CCS_mod',
            'NG 2-on-1 Combined Cycle (H-Frame) 95% CCS': 'Gas-CC_H_2x1-CCS_mod',
        }
        if name in ccs:
            return 'gas-ccs', ccs[name]
        if name.startswith('NG combined cycle 95% CCS (F-frame basis'):
            return 'gas-ccs', 'Gas-CC-CCS_mod'
        if name == 'NG Fuel Cell':
            return 'fuelcell', 'ng-fuel-cell'
    if group == 'Nuclear':
        if name in ('Nuclear', 'Nuclear - AP1000', 'Nuclear - Large'):
            return 'nuclear', 'Nuclear'
        if name in ('Nuclear - Small Modular Reactor', 'Nuclear - Small'):
            return 'nuclear-smr', 'Nuclear-SMR'
    if group == 'Biopower' and name in ('Dedicated', 'Biopower - Dedicated'):
        return 'biopower', 'biopower'
    if group in ('Solar - CSP', 'CSP'):
        if name in ('CSP - 10hrs TES - Class 3', 'CSP - 10 hrs TES - Class 3',
                    '10hrs TES - Class 2', 'CSP - Class 2'):
            return 'csp', 'csp2'
        if vintage <= 2020 and name == 'Class3':
            return 'csp', 'csp2'
    if vintage >= 2021:
        if group in ('Offshore Wind', 'OffShoreWind'):
            if name in ('Class 1', 'Class1', 'Offshore Wind - Class 1'):
                return 'wind-ofs', 'fixed'
            if name in ('Class 8', 'Class8', 'Offshore Wind - Class 8'):
                return 'wind-ofs', 'floating'
        if group in ('Land-Based Wind', 'LandbasedWind') and name in (
            'Class 4', 'Class4', 'Land-Based Wind - Class 4 - Technology 1',
        ):
            return 'wind-ons', '*'
        if group in ('Solar - Utility PV', 'UtilityPV') and name in (
            'Class 4', 'Class4', 'Utility PV - Class 4',
        ):
            return 'upv', '*'
    return None


def load_archive(settings):
    """Load the normalized archive once per formatting run."""
    if '_historical_atb' not in settings:
        path = archive_path(settings['config'])
        if not path.is_file():
            raise FileNotFoundError(
                f'ATB historical archive missing: {path}. Run '
                "'python scripts/scrape_historical_atb.py' first."
            )
        data = pd.read_csv(path, keep_default_na=False)
        keys = ['technology', 'series', 'metric', 'year']
        if data.duplicated(keys).any():
            raise ValueError('Duplicate base-year estimates in ATB historical archive.')
        if not (data['year'] == data['atb_year'] - 2).all():
            raise ValueError('ATB historical years must equal release year minus two.')
        settings['_historical_atb'] = data
    return settings['_historical_atb']


def archive_series(settings, tech, series, metric, boundary):
    data = load_archive(settings)
    selected = data.loc[
        data.technology.eq(tech) & data.series.eq(str(series))
        & data.metric.eq(metric) & data.year.lt(boundary)
        & data.atb_year.le(settings['atbyear'])
    ].copy()
    return selected.sort_values('year')


def apply_archive_history(frame, tech, settings, deflator):
    """Apply archived estimates, with explicit filling and absent-series fallback."""
    smooth = settings['config']['processing'].get('smooth_cost_curves', {})
    modes = smooth.get('technologies', {}).get(tech, {}).get('historical_data', {})
    if not smooth.get('enabled') or not any(
        'atb' in (mode.values() if isinstance(mode, dict) else [mode])
        for mode in modes.values()
    ):
        return frame
    options = settings['config']['historical_atb']
    fallback = options['missing_series']
    if fallback not in ('manual', 'broadcast', 'error'):
        raise ValueError('historical_atb.missing_series must be manual, broadcast, or error.')
    tech_settings = settings['techs'][tech]
    ids = [c for c in tech_settings['indexcols'] if c not in ('Scenario', 't')]
    identity = next((c for c in ('i', 'type', 'turbine') if c in ids), None)
    output = frame.copy()
    for keys, group in frame.groupby(['Scenario', *ids], dropna=False):
        keys = keys if isinstance(keys, tuple) else (keys,)
        label = dict(zip(['Scenario', *ids], keys))
        boundary = settings['atb_series_start'][tech].get(keys)
        # Retired designs have no current projection. Their manual history stays usable.
        boundary = boundary or smooth['projection_start_year']
        historical = group.index[group.t.lt(boundary)]
        series = label.get(identity, '*') if tech != 'wind-ons' else '*'
        for metric, mode in modes.items():
            if isinstance(mode, dict):
                mode = mode.get(label.get(tech_settings.get('history_class_column')))
            if mode != 'atb' or not len(historical):
                continue
            source_series = 'csp2' if tech == 'csp' else series
            data = archive_series(settings, tech, source_series, metric, boundary)
            if data.empty:
                if fallback == 'error':
                    raise ValueError(f'No archived ATB base-year data for {tech}/{series}/{metric}.')
                if fallback == 'broadcast':
                    anchor = group.loc[group.t.eq(boundary), metric]
                    if len(anchor) != 1:
                        raise ValueError(f'Cannot broadcast absent ATB history for {tech}/{series}/{metric}; use manual fallback.')
                    output.loc[historical, metric] = float(anchor.iloc[0])
                print(f'  {tech}/{series}/{metric}: no archived estimate; {fallback} fallback')
                continue
            values = data.value.astype(float).to_numpy()
            if metric in MONETARY:
                values *= data.dollar_year.map(deflator).to_numpy() / deflator[settings['dollaryear']]
            if metric == 'cf_improvement':
                reference = settings['cf_normalization_bases'][tech]
                reference = reference[series] if isinstance(reference, dict) else reference
                values /= reference
            if tech == 'csp' and metric in MONETARY:
                from generate_atb_files import load_csp_cost_ratios
                values *= load_csp_cost_ratios(settings).set_index('type').loc[series, 'ratio']
            if tech == 'wind-ofs' and metric in ('capcost', 'fom'):
                multipliers = pd.read_csv(resolve_atb_path(
                    f"manual_input/offshore_cost_multipliers_{settings['atbyear']}.csv"
                )).set_index('turbine')
                values *= multipliers.loc[series, metric]
            if not np.isfinite(values).all() or (values < 0).any():
                raise ValueError(f'Invalid ATB history for {tech}/{series}/{metric}.')
            output.loc[historical, metric] = np.interp(
                group.loc[historical, 't'], data.year, values
            )
            print(f'  {tech}/{series}/{metric}: ATB base years {data.year.min()}–{data.year.max()}; missing years interpolate/nearest')
    return output
