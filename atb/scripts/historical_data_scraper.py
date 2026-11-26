"""Download historical sources and prepare the committed history tables."""

import argparse
import copy
import hashlib
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent / "helpers"))

from atb_config import load_processing_settings, raw_file_path, resolve_atb_path  # noqa: E402
from downloads import download_file  # noqa: E402
from generate_atb_files import _normalize_reeds_history, get_atb_file_path, load_csp_cost_ratios  # noqa: E402
from historical_data import COLUMNS, KEYS, MONETARY, UNITS, identity_column, load_history  # noqa: E402
from observed_sources import scrape as scrape_observations  # noqa: E402
from real_history_sources import _observed_values_by_year, _split_battery_history, battery_history_sources  # noqa: E402


def digest(path):
    """Return the SHA256 checksum of a file."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def reeds_blob_url(config, relative_path):
    """Return the GitHub blob URL for one ReEDS file at the pinned ref."""
    source = config['reeds_source']
    return f"https://github.com/{source['repo']}/blob/{source['ref']}/{relative_path}"


def reeds_source_file(config, relative_path, force=False, no_download=False, optional=False):
    """Fetch one ReEDS repository file at the pinned ref into the local cache.

    The cache path carries the ref, so changing `reeds_source.ref` fetches
    fresh copies instead of reusing another version's files. Returns None when
    an optional file is absent from that ref.
    """
    source = config['reeds_source']
    destination = (
        resolve_atb_path(source['cache_directory']) / source['ref'] / relative_path
    )
    if not no_download:
        url = (
            f"https://raw.githubusercontent.com/{source['repo']}/"
            f"{source['ref']}/{relative_path}"
        )
        try:
            download_file(
                url, destination, force=force,
                allow_insecure_ssl_fallback=config['raw_data'].get(
                    'allow_insecure_ssl_fallback', False),
            )
        except requests.exceptions.HTTPError as error:
            response = getattr(error, 'response', None)
            if optional and response is not None and response.status_code == 404:
                return None
            raise
    if not destination.is_file():
        if optional:
            return None
        raise FileNotFoundError(
            f"Missing cached ReEDS file {destination}. Rerun without --no-download."
        )
    return destination


def joined(values):
    """Join the distinct non-empty values with " | ", keeping their order."""
    return ' | '.join(dict.fromkeys(str(v) for v in values if str(v)))


def source_reference(path, url):
    """Describe one source file (name, URL, checksum) for sources.csv."""
    return dict(file=Path(path).name, url=url, sha256=digest(path))


def point(tech, series, metric, year, value, dollar_year, source_type, sources, method, **extra):
    """Build one history-table row for a ReEDS series; ``sources`` are kept to build sources.csv."""
    return dict(technology=tech, series=series, metric=metric, year=int(year), value=float(value),
                unit=UNITS.get(metric, 'multiplier'), dollar_year=dollar_year if metric in MONETARY else '',
                source_type=source_type, source_file=joined(s['file'] for s in sources),
                method=method, sources=sources, **extra)


def observed_references(rows, manifest):
    """Return the source record (file, URL, checksum from the download manifest) of each file behind the rows."""
    references = []
    for row in rows.drop_duplicates('source_file').to_dict('records'):
        source = manifest.loc[manifest.local_file.eq(row['source_file'])]
        if source.empty:
            raise ValueError(f"Missing raw-source manifest entry for {row['source_file']}")
        references.append(dict(file=row['source_file'], url=row['source_data_url'] or row['source_page_url'],
                               sha256=source.iloc[0].sha256))
    return references


def _scope_reference_name(tech_settings, target):
    """Return the ATB display name whose components represent one ReEDS series."""
    display = tech_settings['DisplayName']
    if isinstance(display, str):
        return display
    return {v: k for k, v in display.items()}.get(target, next(iter(display)))


def cost_scope_factor(settings, tech, target, scope):
    """Return OCC / (OCC + removed components) for one observed cost scope.

    Components come from cost_scope_adjustment.reference_year, falling back to a
    technology's earliest published year where ATB omits that year.
    """
    config = settings['config']['historical_cost_sources']['cost_scope_adjustment']
    components = config['scopes'].get(scope)
    if components is None:
        raise ValueError(f'Unknown cost scope {scope!r} for {tech}')
    if not components:
        return 1.0, '', None
    cache = settings.setdefault('_cost_scope_components', {})
    if not cache:
        flat = pd.read_csv(settings['reference_flat_file'], low_memory=False)
        flat = flat.loc[flat.core_metric_case.eq(config['reference_case'])
                        & flat.crpyears.eq(config['reference_crpyears'])
                        & flat.scenario.eq(config['reference_scenario'])
                        & flat.core_metric_parameter.isin(['OCC', 'GCC', 'CFC'])]
        if flat.empty:
            raise ValueError('No ATB cost components match cost_scope_adjustment; '
                             'check reference_case, reference_crpyears and reference_scenario.')
        cache['table'] = flat
    flat = cache['table']
    name = _scope_reference_name(settings['techs'][tech], target)
    rows = flat.loc[flat.display_name.eq(name)]
    if rows.empty:
        raise ValueError(f'ATB {settings["atbyear"]} has no cost components for {name!r}')
    year = int(config['reference_year'])
    if year not in set(rows.core_metric_variable):
        year = int(rows.core_metric_variable.min())
    rows = rows.loc[rows.core_metric_variable.eq(year)]
    values = rows.groupby('core_metric_parameter').value.mean()
    if 'OCC' not in values:
        raise ValueError(f'ATB {settings["atbyear"]} has no OCC for {name!r}')
    # ATB omits GCC or CFC for some technologies; a missing component is zero.
    removed = sum(float(values.get(component, 0.0)) for component in components)
    occ = float(values['OCC'])
    if not np.isfinite(occ) or occ <= 0 or removed < 0:
        raise ValueError(f'Invalid ATB cost components for {name!r}')
    factor = occ / (occ + removed)
    detail = ' '.join(f'{c}={float(values.get(c, 0.0)):.1f}' for c in components)
    method = (f'Cost scope {scope}: rescaled to ATB overnight capital cost using '
              f'ATB {settings["atbyear"]} {config["reference_scenario"]} {year} '
              f'{name}: OCC={occ:.1f} {detail} ($/kW); factor={factor:.4f}.')
    reference = source_reference(settings['reference_flat_file'], settings['reference_flat_url'])
    return factor, method, reference


def prepare_real(settings, deflator, deflator_source):
    """Build the real.csv table: the mapped, calculated, and filled ReEDS series."""
    settings = reference_settings(settings)
    config = settings['config']['historical_cost_sources']
    directory = resolve_atb_path(config['directory'])
    raw = pd.read_csv(directory / config['normalized_filename'], keep_default_na=False)
    manifest = pd.read_csv(directory / config['manifest_filename'], keep_default_na=False)
    scope_overrides = config['cost_scope_adjustment'].get('overrides') or {}
    rows = []
    for tech, metrics in config['reeds_mappings'].items():
        for metric, mapping in metrics.items():
            for entry in mapping.get('series') or [mapping]:
                entry = {**mapping, **entry, 'output_column': metric}
                selected = raw
                for column, value in entry['filters'].items():
                    selected = selected.loc[selected[column].eq(value)]
                duration_rows = pd.DataFrame()
                if tech == 'battery':
                    selected, duration_rows = battery_history_sources(raw, entry, settings)
                values = _observed_values_by_year(tech, entry, settings, deflator, observed=selected)
                targets = entry.get('technologies') or entry.get('turbine_classes') or ['*']
                ratios = dict.fromkeys(targets, 1.0)
                extras = [deflator_source] if metric in MONETARY else []
                method = 'Reviewed source mapping; monetary values converted with the ReEDS deflator.' if metric in MONETARY else 'Observed capacity factor fraction; normalized to the current ATB reference during formatting.'
                if metric == 'fom_index':
                    method = 'Early-age O&M median by vintage bin at the bin midpoint; an index only, scaled onto the ATB reference FOM during formatting.'
                calculated = False
                if tech == 'nuclear':
                    calculated = True
                    method = ('Provisional large-nuclear project cost mapping. The completion-cost '
                              'proxy and the reconstructed overnight cost are reported on different '
                              'boundaries and each is rescaled to the overnight basis from its own '
                              'declared scope; earlier sunk costs and the summer-net capacity basis of '
                              'the proxy remain unreconciled. Converted with the ReEDS deflator. '
                              'See sources for project qualifications.')
                if tech == 'battery':
                    durations = _observed_values_by_year(
                        tech, {'output_column': 'storage_duration'}, settings, deflator, observed=duration_rows)
                    units = selected.set_index('year').unit
                    totals = {year: value * durations[year] if units[year] == 'USD/kWh' else value
                              for year, value in values.items()}
                    values = _split_battery_history(totals, settings, deflator, durations)[metric]
                    split = config['battery_cost_split']
                    extras.append(source_reference(settings['workbook_path'], settings['config']['raw_data']['workbook']['url']))
                    method = (f"Estimated components using ATB {settings['atbyear']} {split['reference_scenario']} "
                              f"{split['reference_year']} power P0 and energy E0: s=observed_total/(P0+cohort_hours*E0); "
                              'power=s*P0; energy=s*E0. Total costs converted with the ReEDS deflator.')
                    if 'preferred_cost_filters' in split:
                        method += (' Preferred cost/duration sample where available; otherwise EIA. '
                                   'System USD/kWh converted to USD/kW using sum(MWh)/sum(MW) from the same sample.')
                    calculated = True
                if tech == 'csp':
                    ratios = load_csp_cost_ratios(settings).set_index('type').ratio.to_dict()
                    path = resolve_atb_path(f"inputs/manual/csp_cost_ratios_{settings['atbyear']}.csv")
                    extras.append(source_reference(path, 'https://github.com/ReEDS-Model/ReEDS'))
                    method = 'Crescent Dunes 10-hour project used as csp2 proxy; converted with the ReEDS deflator; configuration cost=proxy*ratio.'
                # Only capital costs carry a boundary, and it belongs to the
                # observation: the two nuclear anchors declare different scopes.
                scopes_by_year = {}
                if metric.startswith('capcost'):
                    override = scope_overrides.get(tech)
                    scopes_by_year = {int(row.year): override or row.cost_scope
                                      for row in selected.itertuples(index=False)}
                    missing = sorted(set(values) - set(scopes_by_year))
                    if missing:
                        raise ValueError(f'{tech} {metric} has no cost scope for {missing}')
                anchors = sorted(values)
                end = max(settings['config']['atb']['year'] - 2, max(anchors))
                years = sorted(set(anchors) | set(range(settings['reeds_start_year'], end + 1)))
                for target, ratio in ratios.items():
                    factors = {scope: cost_scope_factor(settings, tech, target, scope)
                               for scope in set(scopes_by_year.values())}
                    scaled = {year: value * factors[scopes_by_year[year]][0]
                              for year, value in values.items()} if factors else dict(values)
                    for year in years:
                        source_years = [year] if year in values else sorted({
                            max((y for y in anchors if y < year), default=anchors[0]),
                            min((y for y in anchors if y > year), default=anchors[-1]),
                        })
                        references = observed_references(selected.loc[selected.year.isin(source_years)], manifest)
                        if not duration_rows.empty:
                            references += observed_references(duration_rows.loc[duration_rows.year.isin(source_years)], manifest)
                        applied = [factors[scopes_by_year[y]] for y in source_years] if factors else []
                        rescaled = [f for f in applied if f[0] != 1]
                        source_type = 'calculated' if calculated or ratio != 1 or rescaled else 'real'
                        description = method + (f' Ratio={ratio}.' if tech == 'csp' else '')
                        for scope_method in dict.fromkeys(f[1] for f in rescaled):
                            description += ' ' + scope_method
                        references = references + [f[2] for f in rescaled if f[2] is not None]
                        if year not in values:
                            source_type = 'filled'
                            description += ' Linear interpolation between source years.' if len(source_years) == 2 else ' Nearest endpoint carried to this year.'
                        value = np.interp(year, anchors, [scaled[y] for y in anchors]) * ratio
                        rows.append(point(tech, target, metric, year, value, settings['dollaryear'], source_type,
                                          references + extras, description,
                                          cost_scope='|'.join(dict.fromkeys(scopes_by_year[y] for y in source_years))
                                          if scopes_by_year else ''))
    return pd.DataFrame(rows)


def reference_settings(settings):
    """Point history preparation at the pinned reference release."""
    settings = copy.deepcopy(settings)
    config = settings['config']
    release = config['historical_data']['reference_release']
    directory = resolve_atb_path(config['raw_data']['directory'])
    settings['atbyear'] = config['historical_data']['reference_atb_year']
    settings['workbook_path'] = str(directory / release['workbook']['filename'])
    config['raw_data']['workbook']['url'] = release['workbook']['url']
    settings['reference_flat_file'] = str(directory / release['flat_file']['filename'])
    settings['reference_flat_url'] = release['flat_file']['url']
    for path in (settings['workbook_path'], settings['reference_flat_file']):
        if not Path(path).is_file():
            raise FileNotFoundError(f'Missing reference release file {path}; rerun without --no-download.')
    return settings


def prepare_reeds_baseline(settings, deflator, deflator_source, force=False, no_download=False):
    """Build the reeds_baseline.csv table from the ReEDS plant_characteristics files at the pinned ref."""
    config_root = settings['config']
    vintage = config_root['historical_data']['reference_atb_year']
    dollars_relative = 'inputs/plant_characteristics/dollaryear.csv'
    dollars_path = reeds_source_file(config_root, dollars_relative, force, no_download)
    dollars = pd.read_csv(dollars_path, index_col='Scenario').squeeze()
    dollars_source = source_reference(dollars_path, reeds_blob_url(config_root, dollars_relative))
    rows = []
    for tech, config in settings['techs'].items():
        identity = identity_column(config)
        # A technology ReEDS never carried as an ATB file names its baseline
        # plant_characteristics file in settings.yaml (e.g. beccs_reference).
        baseline = config.get('baseline_file')
        for scenario in ('Advanced', 'Moderate', 'Conservative'):
            if baseline:
                key = baseline
                relative = f'inputs/plant_characteristics/{key}.csv'
                path = reeds_source_file(config_root, relative, force, no_download)
            else:
                key, _ = get_atb_file_path(tech, vintage, scenario, settings)
                relative = f'inputs/plant_characteristics/{key}.csv'
                path = reeds_source_file(config_root, relative, force, no_download, optional=True)
                if path is None:
                    key, _ = get_atb_file_path(tech, vintage, 'Moderate', settings)
                    relative = f'inputs/plant_characteristics/{key}.csv'
                    path = reeds_source_file(config_root, relative, force, no_download)
            data = _normalize_reeds_history(pd.read_csv(path), tech, settings)
            dollar_year = int(dollars[key])
            references = [source_reference(path, reeds_blob_url(config_root, relative)),
                          deflator_source, dollars_source]
            for record in data.to_dict('records'):
                series = record.get(identity, '*')
                # Every other column of the ReEDS file is stored as a metric row.
                for metric in config['cols']:
                    if metric in (identity, 't'):
                        continue
                    value = record[metric]
                    if metric in MONETARY:
                        value *= deflator[dollar_year] / deflator[settings['dollaryear']]
                    row = point(tech, series, metric, record['t'], value, settings['dollaryear'], '', references, '')
                    row.update(scenario=scenario, source_file=path.name)
                    if metric == 'cf_improvement':
                        row['unit'] = f'ATB {vintage} reference multiplier'
                    rows.append(row)
    return pd.DataFrame(rows)


def source_table(tables, manifest):
    """Return one row per source file (name, URL, SHA256) behind the history tables."""
    records = {row.local_file: (row.data_url or row.page_url, row.sha256) for row in manifest.itertuples()}
    for table in tables.values():
        for sources in table['sources']:
            for source in sources:
                records.setdefault(source['file'], (source['url'], source['sha256']))
    # Files kept in this repository are versioned by git, and their checksum
    # would change with the checkout's line endings, so it is left blank.
    manual = resolve_atb_path('inputs/manual')
    return pd.DataFrame([dict(source_file=name, url=url, sha256='' if (manual / name).is_file() else sha256)
                         for name, (url, sha256) in sorted(records.items())])


COVERAGE_START, COVERAGE_END = '<!-- coverage:start -->', '<!-- coverage:end -->'


def _collapse_years(files):
    """Shorten runs of yearly files: eia860_2016.zip, eia860_2017.zip -> eia860_2016-2017.zip."""
    groups = {}
    for name in files:
        stem, dot, ext = name.rpartition('.')
        head, sep, tail = stem.rpartition('_')
        if sep and tail.isdigit() and len(tail) == 4:
            groups.setdefault((head + sep, dot + ext), []).append(int(tail))
        else:
            groups[(name, '')] = None
    out = []
    for (head, ext), years in groups.items():
        if years is None:
            out.append(head)
        elif len(years) == 1:
            out.append(f'{head}{years[0]}{ext}')
        else:
            out.append(f'{head}{min(years)}-{max(years)}{ext}')
    return sorted(out)


def write_coverage(settings, real):
    """Rewrite the coverage table in inputs/historical/README.md from config and prepared history."""
    smoothing = settings['config']['processing']['smooth_cost_curves']['technologies']
    mapped = real
    lines = ['| technology | metric | series | history | observed source | anchor years | cost scope |',
             '| --- | --- | --- | --- | --- | --- | --- |']
    for tech, config in smoothing.items():
        for metric, mode in config['historical_data'].items():
            for series, series_mode in (mode.items() if isinstance(mode, dict) else [('*', mode)]):
                lookup = metric + '_index' if series_mode == 'indexed' else metric
                rows = mapped.loc[mapped.technology.eq(tech) & mapped.metric.eq(lookup)]
                if series != '*':
                    # Same fallback as select_history: a single modeled series is stored as '*'.
                    named = rows.loc[rows.series.eq(series)]
                    rows = named if not named.empty else rows.loc[rows.series.eq('*')]
                anchors = rows.loc[rows.source_type.isin(['real', 'calculated'])]
                if series_mode in ('real', 'indexed') and not anchors.empty:
                    files = sorted({f for cell in anchors.source_file for f in cell.split(' | ')
                                    if not f.startswith('atb_') and f != 'deflator.csv'
                                    and not f.startswith('csp_cost_ratios')})
                    years = f'{anchors.year.min()}-{anchors.year.max()} ({anchors.year.nunique()})'
                    scopes = ', '.join(sorted({c for cell in anchors.cost_scope for c in str(cell).split('|') if c}))
                    source = ', '.join(_collapse_years(files))
                else:
                    years, scopes, source = '-', '-', '-'
                lines.append(f'| {tech} | {metric} | {series} | {series_mode} | {source} | {years} | {scopes or "-"} |')
    path = resolve_atb_path(settings['config']['historical_data']['directory']) / 'README.md'
    text = path.read_text(encoding='utf-8')
    start, end = text.index(COVERAGE_START) + len(COVERAGE_START), text.index(COVERAGE_END)
    path.write_text(text[:start] + '\n' + '\n'.join(lines) + '\n' + text[end:],
                    encoding='utf-8', newline='\n')
    print(f'Updated coverage table in {path}')


def main():
    """Download the sources, rebuild both history tables, and refresh the coverage table."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config')
    parser.add_argument('--force', action='store_true', help='Replace cached downloads.')
    parser.add_argument('--no-download', action='store_true', help='Rebuild using cached raw files only.')
    args = parser.parse_args()
    settings = load_processing_settings(args.config)
    config = settings['config']
    if not args.no_download:
        for kind in ('flat_file', 'workbook'):
            download_file(config['raw_data'][kind]['url'], raw_file_path(config, kind), force=args.force,
                          allow_insecure_ssl_fallback=config['raw_data'].get('allow_insecure_ssl_fallback', False))
        release = config['historical_data']['reference_release']
        for kind in ('flat_file', 'workbook'):
            download_file(release[kind]['url'],
                          resolve_atb_path(config['raw_data']['directory']) / release[kind]['filename'],
                          force=args.force,
                          allow_insecure_ssl_fallback=config['raw_data'].get('allow_insecure_ssl_fallback', False))
    scrape_observations(config, force=args.force, no_download=args.no_download)
    settings = copy.deepcopy(settings)
    settings['dollaryear'] = settings['history_dollar_year']
    deflator_relative = 'inputs/financials/deflator.csv'
    path = reeds_source_file(config, deflator_relative, force=args.force, no_download=args.no_download)
    deflator = pd.read_csv(path, index_col='*Dollar.Year').squeeze()
    reference = source_reference(path, reeds_blob_url(config, deflator_relative))
    tables = {'real': prepare_real(settings, deflator, reference),
              'reeds_baseline': prepare_reeds_baseline(settings, deflator, reference,
                                       force=args.force, no_download=args.no_download)}
    directory = resolve_atb_path(config['historical_data']['directory'])
    directory.mkdir(parents=True, exist_ok=True)
    validation = copy.deepcopy(settings)
    validation.pop('_prepared_history', None)
    pending = []
    for kind, table in tables.items():
        if table.duplicated(KEYS[kind]).any() or not np.isfinite(table.value).all():
            raise ValueError(f'Duplicate or invalid {kind} historical points')
        path = directory / config['historical_data'][f'{kind}_filename']
        temporary = path.with_suffix('.csv.part')
        table.sort_values(KEYS[kind])[COLUMNS[kind]].to_csv(temporary, index=False, lineterminator='\n')
        validation['config']['historical_data'][f'{kind}_filename'] = temporary.name
        load_history(validation, kind)
        pending.append((temporary, path, len(table)))
    for temporary, path, count in pending:
        temporary.replace(path)
        print(f'Saved {count:,} points to {path}')
    # The scraped observations as reported, and where every source file came from.
    costs = config['historical_cost_sources']
    observed = pd.read_csv(resolve_atb_path(costs['directory']) / costs['normalized_filename'], keep_default_na=False)
    manifest = pd.read_csv(resolve_atb_path(costs['directory']) / costs['manifest_filename'], keep_default_na=False)
    for name, table in (('observed.csv', observed.drop(columns=['source_page_url', 'source_data_url'])),
                        ('sources.csv', source_table(tables, manifest))):
        table.to_csv(directory / name, index=False, lineterminator='\n')
        print(f'Saved {len(table):,} rows to {directory / name}')
    write_coverage(settings, tables['real'])


if __name__ == '__main__':
    main()
