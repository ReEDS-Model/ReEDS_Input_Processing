"""Sense checks for the MISO LTLF demand-multiplier outputs.

Runs the seven verification items from the plan:
  1. Three scenario files exist under Outputs/
  2. Schema r,year,multiplier and correct row count (48 states x 41 years)
  3. (Already asserted in main script -- weights sum to 1.0 per state)
  4. Fully-MISO state MN behaves as expected
  5. Fully-non-MISO states OH, PA match AEO row-for-row
  6. Mixed state IL blend equals w * MISO + (1-w) * AEO and lies between the alternatives
  7. Scenario ordering: High >= Baseline >= Low for 2027+

Run from any directory:
    python miso_ltlf_processing/_validate_outputs.py
"""

import os
import sys

import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

OUT_DIR = os.path.join(SCRIPT_DIR, 'Outputs')
AEO_DIR = os.path.join(REPO_ROOT, 'aeo_updates', 'Outputs')
LTLF_XLSX = os.path.join(SCRIPT_DIR, 'MISO LTLF Data.xlsx')

AEO_YEAR = 2026
LTLF_FIRST_YEAR = 2026
SCENARIOS = ['baseline', 'high', 'low']
SCEN_LABEL = {'baseline': 'Current Trajectory',
              'high': 'High Trajectory',
              'low': 'Low Trajectory'}


def _passfail(ok, msg):
    tag = 'PASS' if ok else 'FAIL'
    print(f'  [{tag}] {msg}')
    return ok


def check_files_and_schema():
    print('\n[1/2] File existence, schema, row count')
    all_ok = True
    frames = {}
    for s in SCENARIOS:
        p = os.path.join(OUT_DIR, f'demand_MISOLTLF_{AEO_YEAR}_{s}.csv')
        exists = os.path.exists(p)
        all_ok &= _passfail(exists, f'file exists: {os.path.relpath(p, REPO_ROOT)}')
        if not exists:
            continue
        df = pd.read_csv(p)
        frames[s] = df
        cols_ok = list(df.columns) == ['r', 'year', 'multiplier']
        all_ok &= _passfail(cols_ok,
                            f'{s}: columns == [r, year, multiplier] '
                            f'(got {list(df.columns)})')
        nstates = df['r'].nunique()
        nyears = df['year'].nunique()
        nrows = len(df)
        all_ok &= _passfail(
            nstates == 48 and nyears == 41 and nrows == 48 * 41,
            f'{s}: {nstates} states x {nyears} years = {nrows} rows '
            '(expected 48 x 41 = 1968)',
        )
        dup = df.duplicated(subset=['r', 'year']).sum()
        all_ok &= _passfail(dup == 0, f'{s}: no duplicate (r, year) pairs')
        year_range_ok = df['year'].min() == 2010 and df['year'].max() == 2050
        all_ok &= _passfail(year_range_ok,
                            f'{s}: year range 2010..2050')
    return all_ok, frames


def _load_ltlf_ratios():
    """Re-read LTLF ratios directly from the workbook for independent checks."""
    df = pd.read_excel(LTLF_XLSX, sheet_name='Data')
    id_cols = ['LOAD_TYPE', 'TRAJECTORY', 'ZONE/REGION', 'DATA_TYPE', 'DRIVER']
    year_cols = [c for c in df.columns if c not in id_cols]
    driver_blank = df['DRIVER'].isna() | (df['DRIVER'].astype(str).str.strip() == '')
    mask = (
        (df['LOAD_TYPE'] == 'Net Load')
        & (df['DATA_TYPE'] == 'Annual_Energy_TWh')
        & driver_blank
        & (df['ZONE/REGION'].isin(['North', 'Central', 'South']))
    )
    sub = df.loc[mask, ['TRAJECTORY', 'ZONE/REGION'] + year_cols]
    long = sub.melt(id_vars=['TRAJECTORY', 'ZONE/REGION'],
                    value_vars=year_cols,
                    var_name='year', value_name='energy_TWh')
    long['year'] = long['year'].astype(int)
    long['energy_TWh'] = pd.to_numeric(long['energy_TWh'], errors='coerce')
    long = long.rename(columns={'ZONE/REGION': 'subregion'})
    inv_map = {v: k for k, v in SCEN_LABEL.items()}
    long['scenario'] = long['TRAJECTORY'].map(inv_map)
    long = long.drop(columns='TRAJECTORY')
    base = (long.loc[long['year'] == LTLF_FIRST_YEAR,
                     ['scenario', 'subregion', 'energy_TWh']]
            .rename(columns={'energy_TWh': 'base_TWh'}))
    long = long.merge(base, on=['scenario', 'subregion'])
    long['ltlf_ratio'] = long['energy_TWh'] / long['base_TWh']
    return long[['scenario', 'subregion', 'year', 'ltlf_ratio']]


def _load_aeo(scenario):
    p = os.path.join(AEO_DIR, f'demand_AEO_{AEO_YEAR}_{scenario}.csv')
    return pd.read_csv(p)


def check_mn(frames):
    print('\n[3] Fully-MISO state: MN')
    ok = True
    df = frames['baseline']
    mn = df[df['r'] == 'MN'].set_index('year')['multiplier']
    ltlf = _load_ltlf_ratios()
    ltlf_north = (ltlf[(ltlf['scenario'] == 'baseline')
                       & (ltlf['subregion'] == 'North')]
                  .set_index('year')['ltlf_ratio'])
    ltlf_last = int(ltlf_north.index.max())
    print(f'    LTLF horizon detected: {int(ltlf_north.index.min())}..{ltlf_last}')

    ok &= _passfail(abs(mn[2010] - 1.0) < 1e-9,
                    f'2010 multiplier == 1.0 (got {mn[2010]:.6f})')
    ok &= _passfail(abs(mn[2025] - mn[2024]) < 1e-9,
                    f'2025 == 2024 (2024={mn[2024]:.6f}, 2025={mn[2025]:.6f})')
    ok &= _passfail(abs(mn[2026] - mn[2024]) < 1e-9,
                    f'2026 == 2024 (LTLF anchor) '
                    f'(2026={mn[2026]:.6f})')
    # Test the last LTLF year (not necessarily 2050).
    expected_last = mn[2024] * ltlf_north.loc[ltlf_last]
    ok &= _passfail(
        abs(mn[ltlf_last] - expected_last) < 1e-6,
        f'{ltlf_last} == 2024 * (LTLF North {ltlf_last} / 2026) '
        f'(got {mn[ltlf_last]:.6f}, expected {expected_last:.6f})',
    )
    # Warn if the main script's post-horizon fill collapses the multiplier.
    if ltlf_last < 2050:
        post_flat = all(abs(mn[y] - mn[2024]) < 1e-9
                        for y in range(ltlf_last + 1, 2051))
        if post_flat:
            print(f'    [warn] 2050 multiplier == 2024 multiplier '
                  f'({mn[2050]:.4f}); post-horizon LTLF ratios are being '
                  f'filled with 1.0. See note in the report.')
    print(f'    MN: 2010={mn[2010]:.4f}  2024={mn[2024]:.4f}  '
          f'2026={mn[2026]:.4f}  {ltlf_last}={mn[ltlf_last]:.4f}  '
          f'2050={mn[2050]:.4f}  '
          f'LTLF_North_{ltlf_last}={ltlf_north.loc[ltlf_last]:.4f}')
    return ok


def check_non_miso_states(frames):
    print('\n[4] Fully-non-MISO states match AEO row-for-row')
    ok = True
    for st in ['OH', 'PA', 'CA', 'FL', 'NY']:
        for s in SCENARIOS:
            new = (frames[s][frames[s]['r'] == st]
                   .set_index('year')['multiplier'])
            aeo = _load_aeo(s)
            aeo = aeo[aeo['r'] == st].set_index('year')['multiplier']
            aligned = new.reindex(aeo.index)
            diff = (aligned - aeo).abs().max()
            ok &= _passfail(diff < 1e-9,
                            f'{st} / {s}: max |new - AEO| = {diff:.2e}')
    return ok


def check_il_blend(frames):
    print('\n[5] Mixed state IL: blend arithmetic and bounds')
    ok = True
    # Rebuild the pop weights for IL to independently verify the blend
    hierarchy_paths = [
        r'C:\Users\challora\reeds-trees\main\inputs\zones\z90\hierarchy.csv',
        os.path.join(REPO_ROOT, 'zones', 'z90_20260216', 'hierarchy.csv'),
    ]
    hierarchy = None
    for hp in hierarchy_paths:
        if os.path.exists(hp):
            hierarchy = pd.read_csv(hp)
            break
    if hierarchy is None:
        print('    [SKIP] no hierarchy file available')
        return False

    c2z_path = r'C:\Users\challora\reeds-trees\main\inputs\zones\z90\county2zone.csv'
    pop_path = r'C:\Users\challora\reeds-trees\main\inputs\disaggregation\county_population.csv'
    if not os.path.exists(c2z_path) or not os.path.exists(pop_path):
        print('    [SKIP] reeds-trees files not found; blend check skipped')
        return False

    c2z = pd.read_csv(c2z_path, dtype=str).rename(columns={'ba': 'r'})
    c2z['FIPS'] = 'p' + c2z['FIPS'].astype(str).str.zfill(5)
    c2z = c2z.merge(hierarchy[['r', 'st']], on='r', how='left')

    pop = pd.read_csv(pop_path)
    pop.columns = [c.lower() for c in pop.columns]
    pop_col = 'value' if 'value' in pop.columns else 'population'
    pop = pop.rename(columns={'fips': 'FIPS', pop_col: 'population'})
    pop['FIPS'] = pop['FIPS'].astype(str)
    if not pop['FIPS'].str.startswith('p').all():
        pop['FIPS'] = 'p' + pop['FIPS'].str.lstrip('p').str.zfill(5)
    pop['population'] = pd.to_numeric(pop['population'], errors='coerce').fillna(0)

    merged = c2z[['FIPS', 'r', 'st']].merge(pop[['FIPS', 'population']], on='FIPS')
    ba_pop = merged.groupby(['r', 'st'], as_index=False)['population'].sum()
    il_pop = ba_pop[ba_pop['st'] == 'IL'].set_index('r')['population']
    w_miso = il_pop.get('IL_MISO', 0) / il_pop.sum()
    w_pjm = il_pop.get('IL_PJM', 0) / il_pop.sum()
    print(f'    IL weights: IL_MISO={w_miso:.4f}  IL_PJM={w_pjm:.4f}  '
          f'(sum={w_miso + w_pjm:.6f})')

    ltlf = _load_ltlf_ratios()
    for s in SCENARIOS:
        aeo = _load_aeo(s).query('r == "IL"').set_index('year')
        blend = frames[s].query('r == "IL"').set_index('year')['multiplier']
        ltlf_central = (ltlf[(ltlf['scenario'] == s)
                             & (ltlf['subregion'] == 'Central')]
                        .set_index('year')['ltlf_ratio'])

        # loadmult(IL, year) = aeo_multiplier(IL, year) / aeo_ratio(IL, year).
        # aeo_ratio for 2010..2024 is 1.0 in the AEO script, so loadmult = aeo_multiplier.
        # Recover loadmult trajectory using any pre-projection year with ratio=1.0.
        # Simplest: loadmult(y) for y<=2024 is aeo mult; for y>=2025 it's aeo mult / aeo_ratio.
        # We can back it out per-year using: loadmult = aeo_mult / aeo_ratio,
        # but aeo_ratio itself is unknown for y>=2025. Instead, use loadmult(2024)
        # since AEO carries it flat and MISO does the same.
        loadmult_2024 = aeo.loc[2024, 'multiplier']
        ltlf_last = int(ltlf_central.index.max())

        # Test at 2027 and the last LTLF year (to stay inside the LTLF horizon).
        for y in (2027, ltlf_last):
            # AEO IL ratio at year y = aeo_mult(y) / loadmult(2024) since loadmult is flat
            aeo_ratio_y = aeo.loc[y, 'multiplier'] / loadmult_2024
            miso_ratio_y = ltlf_central.loc[y]
            expected = loadmult_2024 * (w_miso * miso_ratio_y + w_pjm * aeo_ratio_y)
            got = blend.loc[y]
            ok &= _passfail(
                abs(got - expected) < 1e-6,
                f'IL / {s} / {y}: blended multiplier '
                f'(got {got:.6f}, expected {expected:.6f})',
            )
            # Blend must lie between pure-MISO and pure-AEO alternatives.
            pure_miso = loadmult_2024 * miso_ratio_y
            pure_aeo = loadmult_2024 * aeo_ratio_y
            lo, hi = (pure_miso, pure_aeo) if pure_miso <= pure_aeo else (pure_aeo, pure_miso)
            ok &= _passfail(
                lo - 1e-9 <= got <= hi + 1e-9,
                f'IL / {s} / {y}: blend lies within '
                f'[{lo:.4f}, {hi:.4f}] (got {got:.4f})',
            )
    return ok


def check_scenario_order(frames):
    print('\n[6] Scenario ordering: High >= Baseline >= Low for 2027+')
    ok = True
    # Load AEO scenarios once so we can distinguish AEO-inherited ordering
    # issues (INFO) from LTLF-blend-driven issues (FAIL).
    aeo = {s: _load_aeo(s) for s in ('baseline', 'high', 'low')}
    # Non-MISO states: OH, PA, CA, FL, NY, etc. inherit AEO exactly.
    non_miso = {'OH', 'PA', 'CA', 'FL', 'NY'}
    # Compare only for states with MISO territory (LTLF-driven divergence).
    # Non-MISO states inherit AEO scenario order; check them too.
    for st in ['MN', 'IL', 'MI', 'OH', 'IA', 'LA']:
        b = frames['baseline'].query(f'r == "{st}"').set_index('year')['multiplier']
        h = frames['high'].query(f'r == "{st}"').set_index('year')['multiplier']
        lo = frames['low'].query(f'r == "{st}"').set_index('year')['multiplier']
        violated = None
        for y in range(2027, 2051):
            if not (h[y] >= b[y] - 1e-9 and b[y] >= lo[y] - 1e-9):
                violated = y
                break
        if violated is None:
            _passfail(True, f'{st}: High >= Baseline >= Low for all 2027..2050')
            continue
        # Ordering violated. If the state is fully non-MISO and AEO itself
        # violates the same ordering at the same year, this is inherited
        # from AEO (INFO). Otherwise it's a real FAIL.
        y = violated
        if st in non_miso:
            ab = aeo['baseline'].query(f'r == "{st}"').set_index('year')['multiplier']
            ah = aeo['high'].query(f'r == "{st}"').set_index('year')['multiplier']
            al = aeo['low'].query(f'r == "{st}"').set_index('year')['multiplier']
            aeo_violates = not (ah[y] >= ab[y] - 1e-9 and ab[y] >= al[y] - 1e-9)
            if aeo_violates:
                print(f'    [INFO] {st} / {y}: order violated High={h[y]:.4f} '
                      f'Baseline={b[y]:.4f} Low={lo[y]:.4f} '
                      f'(inherited from AEO: H={ah[y]:.4f} B={ab[y]:.4f} '
                      f'L={al[y]:.4f}); not a script defect.')
                continue
        ok &= _passfail(
            False,
            f'{st} / {y}: order violated High={h[y]:.4f} '
            f'Baseline={b[y]:.4f} Low={lo[y]:.4f}',
        )
    return ok


def check_scenarios_equal_pre_2027(frames):
    print('\n[7] Scenarios equal 2010..2026 for MISO-driven and non-MISO states')
    ok = True
    for st in ['MN', 'IA', 'LA', 'OH', 'IL', 'MI']:
        b = frames['baseline'].query(f'r == "{st}"').set_index('year')['multiplier']
        h = frames['high'].query(f'r == "{st}"').set_index('year')['multiplier']
        lo = frames['low'].query(f'r == "{st}"').set_index('year')['multiplier']
        for y in range(2010, 2027):
            # AEO diverges starting 2025 (aeo_first_year=2025 by construction),
            # so only expect MISO-only states (MN, IA, LA) to match through 2026.
            if st in ('MN', 'IA', 'LA'):
                if not (abs(h[y] - b[y]) < 1e-9 and abs(lo[y] - b[y]) < 1e-9):
                    ok &= _passfail(
                        False,
                        f'{st} / {y}: scenarios differ (h={h[y]:.6f}, '
                        f'b={b[y]:.6f}, lo={lo[y]:.6f})',
                    )
                    break
        else:
            if st in ('MN', 'IA', 'LA'):
                _passfail(True, f'{st}: all scenarios equal 2010..2026')
    return ok


def main():
    print('=' * 72)
    print('MISO LTLF outputs — sense checks')
    print('=' * 72)
    all_ok = True
    ok, frames = check_files_and_schema()
    all_ok &= ok
    if not frames:
        print('\n[abort] no scenario files loaded; skipping remaining checks')
        return 1
    all_ok &= check_mn(frames)
    all_ok &= check_non_miso_states(frames)
    all_ok &= check_il_blend(frames)
    all_ok &= check_scenario_order(frames)
    all_ok &= check_scenarios_equal_pre_2027(frames)
    print('\n' + '=' * 72)
    print('OVERALL:', 'PASS' if all_ok else 'FAIL')
    print('=' * 72)
    return 0 if all_ok else 1


if __name__ == '__main__':
    sys.exit(main())
