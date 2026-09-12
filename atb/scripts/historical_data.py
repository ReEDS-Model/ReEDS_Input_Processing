"""Read prepared history without accessing scraped historical inputs."""

import json

import numpy as np
import pandas as pd

from atb_config import resolve_atb_path


MONETARY = {'capcost', 'capcost_energy', 'fom', 'fom_energy', 'vom'}
KEYS = ['scope', 'technology', 'series', 'scenario', 'identifiers', 'metric', 'year']


def load_history(settings, kind):
    cache = settings.setdefault('_prepared_history', {})
    if kind not in cache:
        config = settings['config']['historical_data']
        path = resolve_atb_path(config['directory']) / config[f'{kind}_filename']
        if not path.is_file():
            raise FileNotFoundError(f'Missing {path}; run scripts/historical_data_scraper.py.')
        data = pd.read_csv(path, keep_default_na=False)
        required = KEYS + ['value', 'unit', 'dollar_year', 'source_type', 'source_file',
                           'source_url', 'source_years', 'method']
        if set(required) - set(data):
            raise ValueError(f'Missing history columns in {path}: {set(required) - set(data)}')
        if data.duplicated(KEYS).any() or not np.isfinite(data.value).all():
            raise ValueError(f'Duplicate or nonfinite history values in {path}')
        if (data.value < 0).any():
            raise ValueError(f'Negative history values in {path}')
        if kind != 'manual':
            mapped = data.loc[data.scope.eq('reeds')]
            if mapped.duplicated(['technology', 'series', 'metric', 'year']).any():
                raise ValueError(f'Ambiguous mapped history in {path}')
            fractions = mapped.loc[mapped.metric.isin(['cf_improvement', 'rte'])]
            if not fractions.unit.eq('fraction').all() or not fractions.value.between(0, 1).all():
                raise ValueError(f'Invalid capacity-factor or efficiency fractions in {path}')
        monetary = data.unit.str.startswith('USD/')
        if pd.to_numeric(data.loc[monetary, 'dollar_year'], errors='coerce').isna().any():
            raise ValueError(f'Monetary history requires a dollar year in {path}')
        if data[['source_type', 'source_file', 'source_url', 'method']].eq('').any().any():
            raise ValueError(f'Incomplete history provenance in {path}')
        if kind == 'atb' and not data.year.eq(data.atb_year - 2).all():
            raise ValueError('Archived ATB anchors must use release year minus two.')
        cache[kind] = data
    return cache[kind]


def select_history(settings, kind, tech, series, metric):
    data = load_history(settings, kind)
    data = data.loc[data.scope.eq('reeds') & data.technology.eq(tech) & data.metric.eq(metric)]
    selected = data.loc[data.series.eq(str(series))]
    if selected.empty:
        selected = data.loc[data.series.eq('*')]
    return selected.sort_values('year')


def converted_values(data, dollar_year, deflator):
    values = data.value.astype(float).copy()
    monetary = data.unit.str.startswith('USD/')
    years = pd.to_numeric(data.loc[monetary, 'dollar_year']).astype(int)
    factors = years.map(deflator) / deflator[int(dollar_year)]
    if factors.isna().any():
        raise ValueError('A historical dollar year is absent from the ReEDS deflator.')
    values.loc[monetary] *= factors
    return values


def manual_history(settings, tech, scenario, deflator):
    data = load_history(settings, 'manual')
    data = data.loc[data.technology.eq(tech) & data.scenario.eq(str(scenario))].copy()
    if data.empty:
        raise ValueError(f'Missing prepared manual baseline for {tech}/{scenario}')
    data['value'] = converted_values(data, settings['history_dollar_year'], deflator)
    frame = data.pivot(index=['identifiers', 'year'], columns='metric', values='value').reset_index()
    identifiers = pd.DataFrame(frame.pop('identifiers').map(json.loads).tolist())
    frame = pd.concat([identifiers, frame.rename(columns={'year': 't'})], axis=1)
    return frame[settings['techs'][tech]['cols']]


def apply_history(frame, tech, settings, deflator):
    from generate_atb_files import _historical_mode_for_metric, _technology_smoothing_config

    smoothing = _technology_smoothing_config(tech, settings)
    if smoothing is None:
        return frame
    modes = smoothing['historical_data']
    tech_settings = settings['techs'][tech]
    ids = [c for c in tech_settings['indexcols'] if c not in ('Scenario', 't')]
    identity = next((c for c in ('i', 'type', 'turbine') if c in ids), None)
    result = frame.copy()
    for keys, group in frame.groupby(['Scenario', *ids], dropna=False):
        keys = keys if isinstance(keys, tuple) else (keys,)
        labels = dict(zip(['Scenario', *ids], keys))
        boundary = settings['atb_series_start'][tech].get(keys, smoothing['projection_start_year'])
        history = group.index[group.t.lt(boundary)]
        series = labels.get(identity, '*') if tech != 'wind-ons' else '*'
        for metric in modes:
            mode = _historical_mode_for_metric(modes, metric, labels.get(tech_settings.get('history_class_column')))
            if mode not in ('real', 'atb') or not len(history):
                continue
            data = select_history(settings, mode, tech, series, metric)
            if mode == 'atb':
                data = data.loc[data.year.lt(boundary) & data.atb_year.le(settings['atbyear'])]
            if data.empty:
                fallback = settings['config']['historical_atb']['missing_series'] if mode == 'atb' else 'error'
                if fallback == 'error':
                    raise ValueError(f'No prepared {mode} history for {tech}/{series}/{metric}')
                if fallback == 'broadcast':
                    anchor = group.loc[group.t.eq(boundary), metric]
                    if len(anchor) != 1:
                        raise ValueError(f'Missing broadcast anchor for {tech}/{series}/{metric}')
                    result.loc[history, metric] = float(anchor.iloc[0])
                continue
            values = converted_values(data, settings['dollaryear'], deflator).to_numpy()
            if metric == 'cf_improvement':
                reference = settings['cf_normalization_bases'][tech]
                reference = reference[series] if isinstance(reference, dict) else reference
                values /= reference
            anchor_years = data.year.to_numpy(dtype=float)
            anchor_values = np.asarray(values, dtype=float)
            order = np.argsort(anchor_years)
            anchor_years, anchor_values = anchor_years[order], anchor_values[order]
            # Bridge the years between the last history anchor and the first
            # projection year: ATB omits some technologies (nuclear, floating
            # offshore) until 2030, and clamping would hold the last anchor flat
            # for years and then step. Interpolate toward the projection instead.
            # One-sided gaps (years before the first anchor) still clamp.
            projection = group.loc[group.t.eq(boundary), metric]
            if len(projection) == 1 and anchor_years[-1] < boundary:
                anchor_years = np.append(anchor_years, float(boundary))
                anchor_values = np.append(anchor_values, float(projection.iloc[0]))
            result.loc[history, metric] = np.interp(
                group.loc[history, 't'], anchor_years, anchor_values
            )
    return result


def apply_real_overlap(frame, tech, settings, deflator):
    """Use available real anchors through the release year, after smoothing."""
    from generate_atb_files import _historical_mode_for_metric, _technology_smoothing_config

    smoothing = _technology_smoothing_config(tech, settings)
    if not smoothing or not smoothing.get('fill_atbstartyear2atbyear_with_real', False):
        return frame
    config = settings['techs'][tech]
    ids = [c for c in config['indexcols'] if c not in ('Scenario', 't')]
    identity = next((c for c in ('i', 'type', 'turbine') if c in ids), None)
    result = frame.copy()
    for keys, group in frame.groupby(['Scenario', *ids], dropna=False):
        keys = keys if isinstance(keys, tuple) else (keys,)
        labels = dict(zip(['Scenario', *ids], keys))
        boundary = settings['atb_series_start'][tech].get(keys, smoothing['projection_start_year'])
        series = labels.get(identity, '*') if tech != 'wind-ons' else '*'
        for metric in smoothing['historical_data']:
            if _historical_mode_for_metric(smoothing['historical_data'], metric,
                                           labels.get(config.get('history_class_column'))) != 'real':
                continue
            data = select_history(settings, 'real', tech, series, metric)
            data = data.loc[data.source_type.isin(['real', 'calculated'])
                            & data.year.between(boundary, settings['atbyear'])]
            if data.empty:
                continue
            values = converted_values(data, settings['dollaryear'], deflator)
            if metric == 'cf_improvement':
                reference = settings['cf_normalization_bases'][tech]
                values /= reference[series] if isinstance(reference, dict) else reference
            by_year = pd.Series(values.to_numpy(), index=data.year)
            index = group.index[group.t.isin(by_year.index)]
            result.loc[index, metric] = group.loc[index, 't'].map(by_year)
    return result
