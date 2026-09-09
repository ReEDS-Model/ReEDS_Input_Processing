"""Prepare observed cost and capacity-factor series."""

import os
import numpy as np
import pandas as pd
from atb_config import ATB_DIR
ATBDIR = str(ATB_DIR)


def _observed_history_source_path(settings):
    """Return the configured normalized observed-history CSV path."""
    source_settings = settings['config'].get('historical_cost_sources', {})
    return os.path.join(
        ATBDIR,
        source_settings['directory'],
        source_settings['normalized_filename'],
    )


def _observed_values_by_year(tech, mapping, settings, deflator, observed=None):
    """Read, filter, and deflate one observed series into {year: value}."""
    if observed is None:
        source_path = _observed_history_source_path(settings)
        if not os.path.isfile(source_path):
            raise FileNotFoundError(
                f"Observed historical-cost file is missing: {source_path}. "
                "Run scripts/historical_data_scraper.py first."
            )
        observed = pd.read_csv(source_path)
        for column, value in mapping.get('filters', {}).items():
            observed = observed.loc[observed[column] == value]
    if observed.empty:
        raise KeyError(
            f"No observed historical rows match the configured mapping for {tech}."
        )
    if observed['year'].duplicated().any():
        duplicate_years = sorted(
            observed.loc[observed['year'].duplicated(False), 'year'].unique()
        )
        raise ValueError(
            f"Observed historical mapping for {tech} has duplicate years: "
            f"{duplicate_years}"
        )
    if mapping.get('output_column') == 'cf_improvement':
        if not (observed['metric'].eq('capacity_factor').all()
                and observed['unit'].eq('fraction').all()):
            raise ValueError(f"{tech} CF history must contain capacity_factor fractions.")
        values = pd.to_numeric(observed['value'], errors='raise')
        if not (np.isfinite(values) & values.gt(0) & values.le(1)).all():
            raise ValueError(f"{tech} observed capacity factors must be in (0, 1].")
        # Dimensionless observations have no dollar year and are never deflated.
        return dict(zip(observed['year'].astype(int), values.astype(float)))
    if mapping.get('output_column') == 'storage_duration':
        values = pd.to_numeric(observed['value'], errors='raise')
        if not (observed['metric'].eq('storage_duration').all()
                and observed['unit'].eq('hours').all()
                and (np.isfinite(values) & values.gt(0)).all()):
            raise ValueError("Battery duration history must contain finite positive hours.")
        return dict(zip(observed['year'].astype(int), values.astype(float)))
    observed = observed.assign(dollar_year=pd.to_numeric(observed['dollar_year'], errors='raise'))
    if observed['dollar_year'].isna().any() or not observed.dollar_year.mod(1).eq(0).all():
        raise ValueError(
            f"Observed historical mapping for {tech} requires a dollar_year."
        )

    current_dollar_year = int(settings['dollaryear'])
    converted_values = {}
    for row in observed.itertuples(index=False):
        source_dollar_year = int(row.dollar_year)
        if source_dollar_year not in deflator.index:
            raise KeyError(
                f"Deflator table has no value for observed dollar year "
                f"{source_dollar_year}."
            )
        converted_values[int(row.year)] = (
            float(row.value)
            * float(deflator[source_dollar_year])
            / float(deflator[current_dollar_year])
        )
    return converted_values


def battery_history_sources(raw, mapping, settings):
    """Select whole cost/duration cohorts, preferring the configured LBNL sample."""
    split = settings['config']['historical_cost_sources']['battery_cost_split']

    def select(filters):
        data = raw
        for column, value in filters.items():
            data = data.loc[data[column].eq(value)]
        if data.empty or data.year.duplicated().any():
            raise ValueError(f'Missing or ambiguous battery source: {filters}')
        return data

    costs, durations = select(mapping['filters']), select(split['duration_filters'])
    if 'preferred_cost_filters' in split:
        preferred = select(split['preferred_cost_filters'])
        matched = select(split['preferred_duration_filters'])
        if not set(preferred.year).issubset(matched.year):
            raise ValueError('Preferred battery costs lack matching cohort durations')
        costs = pd.concat([costs.loc[~costs.year.isin(preferred.year)], preferred])
        durations = pd.concat([durations.loc[~durations.year.isin(preferred.year)],
                               matched.loc[matched.year.isin(preferred.year)]])
    if not costs.unit.isin(['USD/kW', 'USD/kWh']).all():
        raise ValueError('Battery system costs must use USD/kW or USD/kWh')
    return costs, durations.loc[durations.year.isin(costs.year)]


def _split_battery_history(total_costs, settings, deflator, durations=None):
    """Scale ATB reference components to observed totals at cohort durations."""
    from battery_workbook import extract_battery_costs

    split = settings['config']['historical_cost_sources']['battery_cost_split']
    if durations is None:
        durations = _observed_values_by_year(
            'battery', {'filters': split['duration_filters'], 'output_column': 'storage_duration'},
            settings, deflator,
        )
    missing = sorted(set(total_costs) - set(durations))
    if missing:
        raise ValueError(f"Battery cost years lack observed cohort duration: {missing}")
    reference = extract_battery_costs(settings['workbook_path'])
    reference = reference.loc[reference['Scenario'].eq(split['reference_scenario'])]
    year = int(split['reference_year'])
    if year not in reference or reference['cost'].duplicated().any():
        raise ValueError(f"Missing or ambiguous ATB battery split reference for {year}")
    components = reference.set_index('cost')[year]
    power, energy = float(components['capcost']), float(components['capcost_energy'])
    if not all(np.isfinite(value) and value > 0 for value in (power, energy)):
        raise ValueError("ATB battery reference components must be finite and positive.")
    result = {'capcost': {}, 'capcost_energy': {}}
    for year, total in total_costs.items():
        if not np.isfinite(total) or total <= 0:
            raise ValueError(f"Invalid battery total cost for {year}: {total}")
        scale = total / (power + durations[year] * energy)
        result['capcost'][year] = scale * power
        result['capcost_energy'][year] = scale * energy
    return result
