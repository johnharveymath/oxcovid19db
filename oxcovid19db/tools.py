import pandas as pd
from .columnlists import ColumnLists

def trim(gid_list):
    ''' Remove an underscore and following characters from each element in a list of strings
    Add a terminal period for easy hierarchical comparison
    Intended to remove the version number from gids
    '''
    result = []
    for gid in gid_list:
        gid = gid.split("_")
        result.append(gid[0] + '.')
    return result


def is_match(left, right):
    ''' Test whether the list of strings (gids), left, is equal to or a child of the list right '''
    left_trimmed = trim(left)
    right_trimmed = trim(right)
    result = True
    for left_gid in left_trimmed:
        gid_matches = False
        for right_gid in right_trimmed:
            if left_gid.startswith(right_gid):
                gid_matches = True
                break
        result = result & gid_matches
    return result


def make_parent_dictionary(left_gid_list, right_gid_list):
    ''' For each gid in left or in right, find a parent gid which appears in both'''
    parent = dict()
    for left_gid in left_gid_list:
        for right_gid in right_gid_list:
            if is_match(left_gid, right_gid):
                parent[left_gid] = right_gid
                parent[right_gid] = right_gid
    for right_gid in right_gid_list:
        if not parent.get(right_gid):
            for left_gid in left_gid_list:
                if is_match(right_gid, left_gid):
                    parent[left_gid] = left_gid
                    parent[right_gid] = left_gid
        if not parent.get(right_gid):
            parent[right_gid] = right_gid
    for left_gid in left_gid_list:
        if not parent.get(left_gid):
            parent[left_gid] = left_gid
    return parent


def make_adm_area_dictionary(left, right):
    left_dict = left[geo_columns(left) + ['gid']].drop_duplicates().set_index('gid').T.to_dict()
    right_dict = right[geo_columns(right) + ['gid']].drop_duplicates().set_index('gid').T.to_dict()
    return {**left_dict, **right_dict}


def convert_and_list(df):
    newcols = ['parent1', 'parent2', 'parent3', 'parentgid']
    header_list = list(df.columns) + newcols
    df = df.reindex(columns=header_list)
    df_gid_list = df['gid'].unique()
    return df, df_gid_list


def find_common_regions(left, right):
    ''' Add geographic information to two dataframes, left and right, so that they can be joined on gid '''
    left, left_gid_list = convert_and_list(left)
    right, right_gid_list = convert_and_list(right)

    # make a dictionary to identify parents and fill gid column
    parent = make_parent_dictionary(left_gid_list, right_gid_list)
    left['parentgid'] = left['gid'].map(parent.get)
    right['parentgid'] = right['gid'].map(parent.get)

    adm_dict = make_adm_area_dictionary(left, right)
    converter = {'adm_area_1': 'parent1', 'adm_area_2': 'parent2', 'adm_area_3': 'parent3'}
    for col in geo_columns(left):
        left[converter[col]] = left['parentgid'].map(lambda x: adm_dict.get(x).get(col))
    for col in geo_columns(right):
        right[converter[col]] = right['parentgid'].map(lambda x: adm_dict.get(x).get(col))

    return left, right

def make_agg_rules():
    agg_rules = dict()
    wm = lambda x: np.average(x, weights=df.loc[x.index, "samplesize"])

    # collect column_lists (class should only initialise once)
    table_list = ['epidemiology', 'mobility', 'weather']
    column_lists = ColumnLists(table_list)
    for column in column_lists.epidemiology:
        agg_rules[column] = ['sum']
    for column in column_lists.mobility:
        agg_rules[column] = ['mean']
    for column in column_lists.weather:
        if column != 'samplesize':
            agg_rules[column] = [wm]
    return agg_rules

def group_by_parent(df):
    # Group by geographical columns and by date
    grouping_columns = [col for col in df.columns if (col.startswith('parent') or col == 'date')]
    # Set of rules for how to aggregate any possible column
    agg_rules = make_agg_rules()
    # Set up variables to hold aggregation rules for this particular DataFrame and new column names
    agg_dict = dict()
    agg_columns = []
    # If a column can be aggregated, check if it is in df, then add to agg_dict and agg_columns
    for key in agg_rules:
        if key in df.columns:
            agg_dict[key] = agg_rules[key]
            for val in agg_rules[key]:
                if isinstance(val, str):
                    agg_columns.append(key + '_' + val)
                else:
                    agg_columns.append(key + '_wtmean')

    # Group, aggregate by agg_dict, and rename by agg_columns
    grouped = df.groupby(grouping_columns, dropna=False).agg(agg_dict)
    grouped.columns = agg_columns
    return grouped.reset_index()


def geo_columns(df):
    potential_columns = ['adm_area_1', 'adm_area_2', 'adm_area_3']
    geo_columns = [col for col in potential_columns if col in df.columns]
    return geo_columns


def validate_input(df):
    if not geo_columns(df):
        return False
    return True


def merge(left, right, how='inner'):
    ''' join two dataframes of oxcovid19 data '''
    if not (validate_input(left) and validate_input(right)):
        raise Exception
    # Augment the dataframes with parent regions
    left, right = find_common_regions(left, right)
    # Carry out join
    group_on = ['date', 'parent1', 'parent2', 'parent3',
                'parentgid'] if 'date' in left.columns and 'date' in right.columns else ['parent1', 'parent2',
                                                                                         'parent3', 'parentgid']
    result = pd.merge(group_by_parent(left), group_by_parent(right), how=how, on=group_on)
    # Restore original column names and drop empty columns
    result.rename(
        columns={'parent1': 'adm_area_1', 'parent2': 'adm_area_2', 'parent3': 'adm_area_3', 'parentgid': 'gid'},
        inplace=True)
    drops = [col for col in ['adm_area_1', 'adm_area_2', 'adm_area_3'] if result[col].isnull().all()]
    result.drop(columns=drops, inplace=True)
    return result
