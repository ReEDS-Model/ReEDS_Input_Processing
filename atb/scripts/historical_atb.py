"""Map archived ATB base-year estimates into ReEDS historical series."""

import re


from atb_config import resolve_atb_path


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
        # The 2022-2023 H-class assumptions specify 2x1 plants.
        if 2022 <= vintage <= 2023:
            if name in ('NG H-Frame CC', 'NG Combined Cycle (H-Frame)'):
                return 'gas', 'Gas-CC_H_2x1'
            if name in ('NG H-Frame CC 95% CCS', 'NG Combined Cycle (H-Frame) 95% CCS',
                        'NG combined cycle 95% CCS (H-frame basis'):
                return 'gas-ccs', 'Gas-CC_H_2x1-CCS_mod'
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
    if group == 'Biopower' and name in ('CofireOld', 'CofireNew'):
        return 'coal', name
    if group in ('Solar - CSP', 'CSP'):
        if name in ('CSP - 10hrs TES - Class 3', 'CSP - 10 hrs TES - Class 3',
                    '10hrs TES - Class 2', 'CSP - Class 2'):
            return 'csp', 'csp2'
        if vintage <= 2020 and name == 'Class3':
            return 'csp', 'csp2'
    if vintage >= 2020:
        if group in ('Offshore Wind', 'OffShoreWind'):
            if name in ('Class 1', 'Class1', 'Offshore Wind - Class 1', 'Class 1 - Offshore Fixed'):
                return 'wind-ofs', 'fixed'
            if name in ('Class 8', 'Class8', 'Offshore Wind - Class 8', 'Class 8 - Offshore Floating'):
                return 'wind-ofs', 'floating'
        if group in ('Land-Based Wind', 'LandbasedWind') and name in (
            'Class 4', 'Class4', 'Land-Based Wind - Class 4',
            'Land-Based Wind - Class 4 - Technology 1',
        ):
            return 'wind-ons', '*'
        if group in ('Solar - Utility PV', 'UtilityPV') and name in (
            'Class 4', 'Class4', 'Utility PV - Class 4',
        ):
            return 'upv', '*'
    return None



def archive_series(settings, tech, series, metric, boundary):
    from historical_data import select_history
    data = select_history(settings, "atb", tech, series, metric)
    return data.loc[data.year.lt(boundary) & data.atb_year.le(settings["atbyear"])].sort_values("year")
