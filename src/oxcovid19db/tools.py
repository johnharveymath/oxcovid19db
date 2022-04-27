'''
   Copyright 2021 John Harvey

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
'''

import pandas as pd
from .columnlists import ColumnLists


def trim(gid_list):
    """
    Remove an underscore and any following characters from each element in a list of strings
    Add a terminal period for easy hierarchical comparison
    Intended to remove the version number from gids
    """
    result = []
    for gid in gid_list:
        gid = gid.split("_")
        result.append(gid[0] + '.')
    return result


def is_match(left, right):
    """ Test whether the list of strings (gids), left, is equal to or a child of the list right """
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
    """ For each gid in left or in right, find a parent gid which appears in both"""
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
    """ Creates a look-up dictionary providing administrative names for a gid """
    left_dict = left[geo_columns(left) + ['gid']].drop_duplicates().set_index('gid').T.to_dict()
    right_dict = right[geo_columns(right) + ['gid']].drop_duplicates().set_index('gid').T.to_dict()
    return {**left_dict, **right_dict}


def convert_and_list(df):
    """ Add the additional columns which will be needed and obtain a list of all gids in the df"""

    newcols = ['parent1', 'parent2', 'parent3', 'parentgid']
    header_list = list(df.columns) + newcols
    df = df.reindex(columns=header_list)
    try:
        df_gid_list = df['gid'].unique()
    except KeyError as e:
        raise ValueError("The argument must have a column named gid")
    return df, df_gid_list


def find_common_regions(left, right):
    """ Add geographic information to two dataframes, left and right, so that they can be joined on gid """

    # add extra columns and report a list of unique gids in each df
    left, left_gid_list = convert_and_list(left)
    right, right_gid_list = convert_and_list(right)

    # make a dictionary to identify parents and fill parentgid column
    parent = make_parent_dictionary(left_gid_list, right_gid_list)
    left['parentgid'] = left['gid'].map(parent.get)
    right['parentgid'] = right['gid'].map(parent.get)

    # fill in the names for the parentgid in parent1, parent2, parent3
    adm_dict = make_adm_area_dictionary(left, right)
    converter = {'adm_area_1': 'parent1', 'adm_area_2': 'parent2', 'adm_area_3': 'parent3'}
    for col in geo_columns(left):
        left[converter[col]] = left['parentgid'].map(lambda x: adm_dict.get(x).get(col))
    for col in geo_columns(right):
        right[converter[col]] = right['parentgid'].map(lambda x: adm_dict.get(x).get(col))

    return left, right


def make_agg_rules():
    """ Collects the columns for the database and assigns the correct aggregation rules to each """
    agg_rules = dict()
    wm = lambda x: np.average(x, weights=df.loc[x.index, "samplesize"])

    # collect column_lists (either from a cached state or from the database)
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
    """ Aggregates data with the same administrative area and date according to rules for that data type """
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
    """ Extract column names in df which are geographical names"""
    potential_columns = ['adm_area_1', 'adm_area_2', 'adm_area_3']
    geo_columns = [col for col in potential_columns if col in df.columns]
    return geo_columns


def validate_input(df):
    """ An input df must have one column of names and one column of gids """
    if not geo_columns(df):
        return False
    if 'gid' not in df.colummns:
        return False
    return True


def merge(left, right, how='inner'):
    """ join two dataframes of oxcovid19 data """

    # confirm that both dataframes have necessary geographical information
    if not validate_input(left):
        raise ValueError('left dataframe columns must include gid and at least one of adm_area_1, adm_area_2, '
                         'adm_area_3')
    if not validate_input(right):
        raise ValueError('right dataframe columns must include gid and at least one of adm_area_1, adm_area_2, '
                         'adm_area_3')

    # Augment the dataframes with parent regions
    left, right = find_common_regions(left, right)

    # Carry out join on the parent regions and on date, if present
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
