import os
import sys
import glob
import pandas as pd
from itertools import product
import altair as alt
reeds_path = os.environ.get('REEDS_PATH', os.path.expanduser('~/Documents/Github/ReEDS/ReEDS'))
sys.path.append(reeds_path)

'''
This script processes the raw LBNL's interconnection queues data (https://emp.lbl.gov/queues) into
the active queue capacity at FIPS level by technology, split by interconnection agreement (IA) status:
- ia_executed = 1: q_status = "active" and IA_phase_clean = "IA Executed"
- ia_executed = 0: q_status = "active" with any other IA_phase_clean, including missing
The two categories are mutually exclusive, so "All Active" capacity is the sum of both rows.
The annual capacity deployment limit trajectory built from these values is determined in ReEDS.
'''

dir = os.getcwd()

##################### INPUTS ######################
# Most updated version of interconnection queue
filename = 'LBNL_Ix_Queue_Data_File_thru2025.xlsx'
# LBNL supplement with the detailed types behind "Other"/"Other Storage"
filename_other = 'queues_other_forNLR_2025.xlsx'
version = 2025              # data year (matches the year in the LBNL filenames)
# Number of technology type (as specified in the queue data file)
type_no = 3

###################################################

# Technology groups (tg) in ReEDS that the queue data is mapped to
reeds_techset = ['battery', 'biomass', 'coal', 'gas', 'geothermal', 'h2', 'hydro',
                 'nuclear', 'pumped-hydro', 'pv', 'wind-ofs', 'wind-ons']
# Labels used in the diagnostics and figures for the two IA categories and their sum
ia_labels = {1: 'IA Executed', 0: 'IA Not Executed'}
all_active_label = 'All Active'

def add_detailed_types(queue_data, filename_other, type_cols, folded2aggregated):
    """Relabel the aggregated LBNL resource types using the detailed types from LBNL's supplement.

    Hybrid (co-located) requests name their detailed types in a different order than the public
    file's type columns, so each is matched to the aggregated category it was folded into rather
    than by position. A request with the same aggregated category in two type columns is skipped,
    since there is no way to tell which slot the detailed type belongs to. Capacities always come
    from the public file. folded2aggregated maps each detailed type (lowercase) to the aggregated
    category LBNL folded it into.
    """
    other = pd.read_excel(os.path.join(dir,'inputs',filename_other))
    detailed_by_request = {}
    for row in other.itertuples(index=False):
        # Requests with a single resource type leave type1-3 blank and only name it in type_raw
        types = [t.strip() for t in (row.type1, row.type2, row.type3) if isinstance(t, str)]
        types = types or [str(row.type_raw).strip()]
        detailed = {folded2aggregated[t.lower()]: t for t in types if t.lower() in folded2aggregated}
        if detailed:
            detailed_by_request[(str(row.q_id).strip(), str(row.entity).strip())] = detailed

    # A q_id is only unique within an interconnecting entity, so match on the pair
    request = list(zip(queue_data['q_id'].astype(str).str.strip(),
                       queue_data['entity'].astype(str).str.strip()))
    aggregated = queue_data[type_cols].apply(lambda c: c.astype(str).str.strip())
    relabeled_requests = set()
    for col in type_cols:
        # Number of type columns sharing this row's category; >1 means the slot is ambiguous
        shared = aggregated.eq(aggregated[col], axis=0).sum(axis=1)
        relabeled = pd.Series(
            [detailed_by_request.get(q, {}).get(t) if n == 1 else None
             for q, t, n in zip(request, aggregated[col], shared)],
            index=queue_data.index)
        relabeled_requests.update(q for q, t in zip(request, relabeled) if t is not None)
        queue_data[col] = relabeled.fillna(queue_data[col])

    print('Relabeled ' + str(len(relabeled_requests)) + ' of the ' + str(len(detailed_by_request))
          + ' requests in ' + filename_other + ' that report an aggregated resource type')
    # Requests missing from the public file have no capacity or location, so they can't contribute
    missing = [q for q in detailed_by_request if q not in relabeled_requests]
    if missing:
        print('  No matching request in ' + filename + ' for: '
              + ', '.join(q_id + ' (' + entity + ')' for q_id, entity in missing))
    return queue_data

def aggregate_active_queue(queue_data, county_state, type_no=3):
    """Sum the active queue capacity by ReEDS county, technology group, and IA category.

    Each of the type_1..type_N / mw_1..mw_N slots of a request is counted separately, so a hybrid
    request contributes its own capacity to each of its technologies. Requests are matched to
    ReEDS counties on their FIPS code first and on county name + state as a fallback; requests that
    match neither are out of scope. ia_executed is 1 only when IA_phase_clean is "IA Executed";
    any other or missing phase is 0, so the two categories never overlap.

    Returns a frame with columns r, tg, ia_executed, capacity_mw that covers every ReEDS county
    with at least one active request, every ReEDS technology group with active queue capacity, and
    both IA categories; a 0 row means no active queue capacity in that category, not missing data.
    """
    active_queue = pd.DataFrame()

    # Iterate over the tech types (3)
    for pt in list(range(type_no)):
        item = pt+1

        # Filter out tech type
        queue_data_temp = queue_data[['q_status', 'county', 'state', 'fips_code', 'IA_phase_clean', 'type_'+str(item),'mw_'+str(item)]]
        queue_data_temp = queue_data_temp.rename(columns={'county': 'county_name', 'fips_code': 'FIPS',
                                                          'IA_phase_clean': 'IA_status_clean',
                                                          'type_'+str(item): 'tech','mw_'+str(item):'cap'})

        # Capacities are read as objects because of the header offset, so cast them back to numbers
        queue_data_temp['cap'] = pd.to_numeric(queue_data_temp['cap'], errors='coerce')

        # Only consider queues that have active status
        queue_data_active_temp = queue_data_temp[queue_data_temp['q_status']=='active'].copy()

        # Flag queues with an executed IA; every other status (including missing) is not executed
        queue_data_active_temp['ia_executed'] = (queue_data_active_temp['IA_status_clean']=='IA Executed').astype(int)

        # Remove the queues with negative capacity and clean up
        queue_data_active_temp.loc[queue_data_active_temp['cap']<0,'cap'] = 0
        queue_data_active_temp['cap'] = queue_data_active_temp['cap'].fillna(0)
        queue_data_active_temp['type'] = 'type'+str(item)
        active_queue = pd.concat([active_queue, queue_data_active_temp], axis=0).reset_index(drop=True)

    # Match each queue to a ReEDS county on its FIPS code, falling back to county name + state
    fips_reported = 'p' + pd.to_numeric(active_queue['FIPS'], errors='coerce').map(
        lambda x: str(int(x)).zfill(5) if pd.notna(x) else '')
    name2fips = county_state.set_index(county_state['county_name']+'|'+county_state['state'])['FIPS']
    fips_byname = (active_queue['county_name'].str.lower()+'|'+active_queue['state']).map(name2fips)
    active_queue['FIPS'] = fips_reported.where(fips_reported.isin(county_state['FIPS']), fips_byname)

    # Rename tech to match with ReEDS symbols
    active_queue['tech'] = active_queue['tech'].apply(lambda x: x.lower() if isinstance(x, str) else x)
    active_queue.loc[active_queue['tech']=='wind','tech'] = 'wind-ons'
    active_queue.loc[active_queue['tech']=='offshore wind','tech'] = 'wind-ofs'
    active_queue.loc[active_queue['tech']=='solar','tech'] = 'pv'
    active_queue.loc[active_queue['tech']=='hydrogen','tech'] = 'h2'
    active_queue.loc[active_queue['tech']=='pumped storage','tech'] = 'pumped-hydro'
    active_queue.loc[active_queue['tech']=='biofuel','tech'] = 'biomass'
    active_queue.loc[active_queue['tech']=='biogas','tech'] = 'biomass'

    # Queues in a ReEDS county with a ReEDS technology group are in scope; report the rest
    in_county = active_queue['FIPS'].isin(county_state['FIPS'])
    in_techset = active_queue['tech'].isin(reeds_techset)
    active_queue_county = active_queue[in_county & in_techset]

    print('Active queue capacity (MW) in ReEDS counties and technology groups by IA phase:')
    by_phase = active_queue_county.groupby(
        active_queue_county['IA_status_clean'].fillna('(missing IA_phase_clean)'))['cap'].sum()
    for phase, cap in by_phase.sort_values(ascending=False).items():
        print('  ' + phase + ': ' + str(round(cap, 1)))
    print('  Missing IA_phase_clean is counted as ia_executed = 0')
    print('Active queue capacity (MW) excluded from the output:')
    print('  ' + str(round(active_queue.loc[~in_county, 'cap'].sum(), 1)) + ' in requests not matched to a ReEDS county')
    out_of_scope = active_queue[in_county & ~in_techset]
    print('  ' + str(round(out_of_scope['cap'].sum(), 1)) + ' in resource types outside the ReEDS technology groups ('
          + ', '.join(sorted(out_of_scope['tech'].dropna().unique())) + ')')

    # Sum up the queue capacities by county, tech, and IA category
    active_queue_agg = active_queue_county.groupby(['FIPS','tech','ia_executed'])['cap'].sum().reset_index()

    # Assign 0 queue cap value to county-tech-IA category with no value, covering every ReEDS county
    # with an active request and every technology group with active queue capacity
    counties = active_queue.loc[in_county, 'FIPS'].unique()
    techs = active_queue_county['tech'].unique()
    unique_FIPS_tech_ia = pd.DataFrame(product(counties, techs, [1, 0]), columns=['FIPS','tech','ia_executed'])
    active_queue_agg = unique_FIPS_tech_ia.merge(active_queue_agg, on=['FIPS','tech','ia_executed'], how='left')
    active_queue_agg['cap'] = active_queue_agg['cap'].fillna(0)

    ### Final dataset to use in ReEDS
    return active_queue_agg.rename(columns={'FIPS':'r', 'tech':'tg', 'cap':'capacity_mw'})

def main():
    queue_data = pd.read_excel(os.path.join(dir,'inputs',filename), sheet_name='03. Complete Queue Data')
    # First row is a title row; check the real header is in the second row before using it
    if 'q_id' in queue_data.columns:
        raise ValueError(f'{filename}: header is already in the first row; remove the row-skipping below')
    if 'q_id' not in queue_data.iloc[0].values:
        raise ValueError(f'{filename}: expected header row (q_id, ...) in the second row of the sheet')
    queue_data.columns = queue_data.iloc[0]
    queue_data = queue_data[1:]

    # The LBNL queue data aggregates biofuel and biomass into technology type 'Other' and pumped
    # storage into 'Other Storage'. Here we map these technologies to their aggregated categories
    # so the detailed types can be restored from LBNL's supplement.
    folded2aggregated = {
        'pumped storage': 'Other Storage',
        'biofuel': 'Other',
        'biomass': 'Other',
    }
    queue_data = add_detailed_types(
        queue_data, filename_other, ['type_'+str(item+1) for item in range(type_no)], folded2aggregated)

    # County-to-FIPS mapping from the ReEDS repo
    county_state = pd.read_csv(os.path.join(reeds_path,'inputs','zones','county_state.csv'))
    county_state['FIPS'] = 'p' + county_state['FIPS'].astype(str).str.zfill(5)

    active_queue_county = aggregate_active_queue(queue_data, county_state, type_no)

    # Assuming zero queue for csp (in both IA categories)
    csp_queue = pd.read_csv(os.path.join(dir,'inputs','csp_queues.csv'))
    csp_queue = csp_queue.merge(pd.DataFrame({'ia_executed': [1, 0]}), how='cross')
    active_queue_county = active_queue_county.merge(csp_queue, on=['r','tg','ia_executed'], how='outer').fillna(0)
    active_queue_county = active_queue_county.sort_values(['r','tg','ia_executed'], ascending=[True, True, False])
    active_queue_county = active_queue_county[['r','tg','ia_executed','capacity_mw']].reset_index(drop=True)

    ia_executed_total = active_queue_county.loc[active_queue_county['ia_executed']==1, 'capacity_mw'].sum()
    print('Output: ' + ia_labels[1] + ' ' + str(round(ia_executed_total, 1)) + ' MW; '
          + all_active_label + ' ' + str(round(active_queue_county['capacity_mw'].sum(), 1)) + ' MW')

    ##################### SAVE OUTPUTS ######################
    active_queue_county.to_csv(os.path.join(dir,'outputs','interconnection_queues_'+str(version)+'.csv'),index=False)
    active_queue_county.to_csv(os.path.join(dir,'outputs','interconnection_queues.csv'),index=False)
    #########################################################


    ############### QUEUE PLOT #######################
    # Sum each vintage by technology and IA category, and add the All Active (sum of both) category
    def sum_by_category(queue, vintage):
        by_ia = queue.groupby(['tg','ia_executed'])['capacity_mw'].sum().reset_index()
        by_ia['category'] = by_ia['ia_executed'].map(ia_labels)
        all_active = queue.groupby('tg')['capacity_mw'].sum().reset_index()
        all_active['category'] = all_active_label
        out = pd.concat([by_ia, all_active], axis=0)[['tg','category','capacity_mw']]
        out['vintage'] = str(vintage)
        return out

    queue_plot = sum_by_category(active_queue_county, version)

    category_order = [ia_labels[1], ia_labels[0], all_active_label]
    status_cat = ['pv','csp','wind-ons', 'wind-ofs', 'nuclear', 'battery', 'pumped-hydro',
                  'biomass', 'gas', 'coal','hydro', 'geothermal', 'h2']

    resource_order_idx = {
        resource: idx
        for idx, resource in enumerate(status_cat[::-1]) # Reverse list to align colors with legend order
    }

    # Create "idx" column with integer values indicating order in stacked bar
    queue_plot["idx"] = queue_plot["tg"].map(resource_order_idx)

    chart = alt.Chart(queue_plot).mark_bar(size=30).encode(
        x=alt.X('category:N', title=None, sort=category_order),
        y=alt.Y('sum(capacity_mw):Q', axis=alt.Axis(grid=False, title='Capacity (MW)'), scale=alt.Scale(domain=[0, 2200000]),
                sort=status_cat),
        color=alt.Color('tg',
                        scale=alt.Scale(range=['gold','goldenrod','skyblue','aqua','lightpink','darkseagreen','aquamarine',
                                               'saddlebrown','grey','black','lightblue','violet','turquoise']),
                        sort=status_cat),
        order=alt.Order('idx')).configure_axis(titleFontSize=15, labelFontSize=15, grid=False
                    ).configure_legend(labelFontSize=15, titleFontSize=15).properties(width=200, height=350).properties(
        width=500,
        height=300,
        title='Interconnection Queue Version ' + str(version)
    )

    chart.save(os.path.join(dir,'outputs','figures','queue_versions_'+str(version)+'.html'))

    ############### COMPARISON PLOT #######################
    # Compare every vintage in outputs/ (interconnection_queues_<vintage>.csv) by IA category
    queue_compare = []
    for path in sorted(glob.glob(os.path.join(dir,'outputs','interconnection_queues_*.csv'))):
        vintage = os.path.basename(path).split('_')[-1].split('.')[0]
        queue_compare.append(sum_by_category(pd.read_csv(path), vintage))
    queue_compare = pd.concat(queue_compare, axis=0)
    queue_compare['idx'] = queue_compare['tg'].map(resource_order_idx)

    chart_compare = alt.Chart(queue_compare).mark_bar(size=30).encode(
        x=alt.X('vintage:N', title='Data vintage'),
        y=alt.Y('sum(capacity_mw):Q', axis=alt.Axis(grid=False, title='Capacity (MW)'), scale=alt.Scale(domain=[0, 2200000]),
                sort=status_cat),
        color=alt.Color('tg',
                        scale=alt.Scale(range=['gold','goldenrod','skyblue','aqua','lightpink','darkseagreen','aquamarine',
                                               'saddlebrown','grey','black','lightblue','violet','turquoise']),
                        sort=status_cat),
        order=alt.Order('idx'),
        column=alt.Column('category:N', title=None, sort=category_order)
    ).properties(width=200, height=300, title='Interconnection Queue by Data Vintage and IA Category'
    ).configure_axis(titleFontSize=15, labelFontSize=15, grid=False
    ).configure_legend(labelFontSize=15, titleFontSize=15
    ).configure_header(labelFontSize=15)
    chart_compare.save(os.path.join(dir,'outputs','figures','compare_queue_versions.html'))


if __name__ == '__main__':
    main()
