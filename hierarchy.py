#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import networkx as nx

def pd_concat(df_list):
    return pd.concat(df_list, ignore_index=True, sort=False)

PARENT_PREFIX       = 'parent_'
H_PREFIX            = 'h_'       # prefix to use for added columns
IX_COL              = H_PREFIX + 'ix'
LEVEL_COL           = H_PREFIX + 'level'
DEFAULT_LEVEL_SPEC  = {LEVEL_COL: 999}        # level used when wrapping non-def_df with def_df

class Hierarchy:

    # def_df: defines the hierarchy
    #   must have <name> & parent_<name> columns (e.g., 'dept' and 'parent_dept')
    #   may have 'h_ix' column, if so this is used to sort results (h_ix+h_level)
    #   may have additional columns adding info to that node (eg, manager)
    # name: column name for the hierarchy, used to join the source df to the hierarchy (e.g. 'dept')
    # root: default: first row, column parent_<name>// note: root value in def_df may not be None!
    # level: added by constructor (1-based)

    def __init__(self, def_df, name, root=None):
        self.name = name
        self.mandatory = True                       # pers without a valid link to this hierarchy are dropped
        #
        parent_name = PARENT_PREFIX + self.name
        self.root_node = def_df.at[0, parent_name] if root is None else root
        self.def_df = def_df.copy()
        self.def_df[IX_COL] = def_df[IX_COL] if IX_COL in def_df.keys() else def_df.index       # add 'h_ix'
        # set up empty .aggs_df with <name> as primary key
        self.aggs_df = self.def_df.copy()[[name]]

        if not set([self.name, parent_name]).issubset(def_df.keys()):
            raise(ValueError('Child or parent column missing'))

        if not def_df[def_df.duplicated([self.name], keep=False)].empty:
            raise(ValueError('%s column values must be unique'))

        def _get_paths():                 # paths_df: node -> L0_node, L1_node, L2_node, node
            paths = {}
            levels = {}
            G = nx.from_pandas_edgelist(self.def_df, parent_name, self.name, create_using=nx.DiGraph())
            for node in G:
                if node != self.root_node:
                    path = nx.shortest_path(G, self.root_node, node)
                    # paths[path[-1]] = path[:-1]    # path[-1] is the leaf node, path[0] is root node
                    paths[path[-1]] = path    # path[-1] is the leaf node, path[0] is root node
                    levels[path[-1]] = len(path) - 1
            return (paths, levels)

        paths, levels = _get_paths()  # dicts with <name> column as key

        # paths df: name -> L1_parent_name - L2_parent_name - L3_parent_name...
        self.paths_df = (pd.DataFrame
                .from_dict(paths, orient='index')
                .reset_index()
                .rename(columns={'index': self.name}))

        # add levels column to the def_df
        levels = (pd.DataFrame.from_dict([levels]).T
                .reset_index()
                .rename(columns={'index': self.name, 0: LEVEL_COL}))

        self.def_df = self.def_df.merge(levels, on=self.name, how='left').fillna(999)

    @classmethod
    # parent not yet available, derive from struc_field ('RvB|Sales & Marketing|Marketing|Marketing Support')
    #  struc_field must be delimited by '|' (or specify delim)
    #  if struc_field is a list of column names, we will build temporary struc_field from them
    def from_structure(cls, def_df, name, struc_field, root=None, delim='|'):
        df = def_df.fillna('')
        keys = list(def_df.keys())

        # if necessary, concatenate column values into single struc field deli with '|'
        if isinstance(struc_field, list):
            print(df)
            print(struc_field)
            df['struc'] = df.apply(
                lambda row: delim.join([row[col] for col in struc_field if row[col]]), 
                axis=1)
            struc_field = 'struc'

        # struc_to_name: struc -> name
        struc_to_name = (df.copy()[[name, struc_field]]
            .append({name: 'root', struc_field: 'root'}, ignore_index=True)
            .rename(columns={struc_field: 'parent_' + struc_field, name: PARENT_PREFIX + name})
        )

        # parent_struc = struc minus last/leaf node in |-delimited list 
        def strip_from_right(row):
            index = row[struc_field].rfind(delim)
            return row[struc_field][:index] if index != -1 else 'root'
        #
        df['parent_' + struc_field] = df.apply(strip_from_right, axis=1)

        df = (df.merge(struc_to_name, on='parent_' + struc_field, how='left')
                .replace({'root': root})
                .reindex(keys + [PARENT_PREFIX + name], axis=1))    # drop temp columns, keep only parent col

        errmask = df.parent_dept.isnull()
        if not df[errmask].empty:
            print('Error in source data, no parent found %s - skipping' % df[errmask])
        df = df[~errmask]

        return cls(df, name, root)

    # duplicate rows for all higher levels in the hierarchy  
    # assumes that df has column with the hierarchy name
    """
    org = pd.DataFrame(list(zip(['1','12','120','121','122','13','130','1301','2','21'], ['0','1','12','12','12','1','13','130','0','2'])),
        columns = ['oe','parent_oe'])
    pers = pd.DataFrame(list(zip(['1','121','121','121','12'], ['456','573','574','578','666'])), columns=['oe','persnr'])
    h = Hierarchy(org, 'oe','0')
    h.expand(pers, add_cols=['h_ix','h_level'])   # h_ix? see __init__
    """
    def expand(self, df, add_cols=[]):     # add_cols = list of col names to add back from def_df, eg h_ix, h_level, manager...
        cols = df.keys()
        if self.name not in cols:
            raise(ValueError('%s column missing' % self.name))
        val_name = H_PREFIX + self.name
        return(df.merge(self.paths_df, on=self.name, how='left')
                .melt(id_vars=cols, value_name=val_name, var_name=LEVEL_COL)
                .query('%s!="%s"' % (val_name, self.root_node))   # drop root_node from result
                .dropna(subset=[val_name])
                .drop(columns=[self.name])                 # replace source 'dept' with 'dept' for the corresponding level
                .rename(columns={val_name: self.name})
                .merge(self.def_df, on=self.name, how='left', suffixes=('', '_y'))
                .sort_values(by=[IX_COL, LEVEL_COL])
                .reindex(list(cols) + add_cols, axis=1))

    # aggregate over df.val_col with agg_fn_name ('count','sum')
    # add result to self.aggs_df.agg_col
    #
    def add_to_def_df(self, df, val_col, agg_col, agg_fn_name):
        tdf = df[[self.name, val_col]]
        tdf = (self.expand(tdf)
                .groupby(self.name)
                .agg(agg_fn_name)      # eg, 'count', 'sum'
                .reset_index()
                .rename(columns={val_col: agg_col}))
        self.def_df = (self.def_df
                .merge(tdf, on=self.name, how='left')
                .fillna(''))

    # wrap pers_type_df (one row per pers) in org context: widen with def_df columns & intersperse with def_df rows
    #   rename_spec is to align columns in def_df to source df
    def wrap_in_def_df(self, df, rename_spec={}):
        return (pd_concat([
                self.def_df.rename(rename_spec),
                (df.assign(**DEFAULT_LEVEL_SPEC)     # add 'h_level' column with value 999
                    .merge(self.def_df.drop(columns=LEVEL_COL), on=self.name, how='left')
                    .fillna(''))
                ])
                .fillna('')
                .sort_values(by=[IX_COL, LEVEL_COL]))


if __name__ == "__main__":
    """Small demonstration, counting employees in an organizational structure."""
    # get some source data
    org = pd.DataFrame(list(zip(['1','12','120','121','122','13','130','1301','2','21'],
        ['0','1','12','12','12','1','13','130','0','2'])),
        columns=['dept','parent_dept'])
    root_node = '0'
    org['manager'] = 'Mgr_' + org.dept.str.upper()
    pers = pd.DataFrame(list(zip( 
        ['1','12','121','121','130'], 
        ['456','573','574','578','666'],
        ['John','Peter','Paul','Mary','George'])), 
        columns=['dept','pnr','name'])

    h = Hierarchy(org, 'dept', '0')
    print('\nExample def_df frame in Hierarchy h:')
    print(h.def_df)
    print('\nExample expansion:')
    exp = h.expand(pers, add_cols=['h_ix', 'h_level'])
    print(exp)
    print('\nExample groupby agg on multiple levels:')
    aggcount = (exp
        .groupby('dept')[['pnr']]
        .count()
        .reset_index()
        .rename(columns={'ancestor_dept':'dept','pnr':'p_count'}))
    print(aggcount)

    print('\n\nSecond example, using from_structure\nsource df:')
    src_df = pd.DataFrame.from_dict({
        'labels':  {0: 'RvB',
                    1: 'RvB|Sales & Marketing',
                    2: 'RvB|Sales & Marketing|Sales',
                    3: 'RvB|Sales & Marketing|Marketing',
                    4: 'RvB|Sales & Marketing|Marketing|Marketing Support',
                    5: 'RvB|Finance & ICT',
                    6: 'RvB|Finance & ICT|Finance'},
        'dept':    {0: 10, 1: 100, 2: 110, 3: 120, 4: 121, 5: 200, 6: 210},
        'mgr':     {0: 'John',
                    1: 'Sally',
                    2: 'Ruben',
                    3: 'Holly',
                    4: 'Max',
                    5: 'Mimi',
                    6: 'Astrid'}})
    print(src_df)
    h2 = Hierarchy.from_structure(src_df, 'dept', 'labels', root='0')
    print('\ndef_df frame:')
    print(h2.def_df)

    print('\n\nThird example, using from_structure, separate fields\nsource df:')
    src_df = pd.DataFrame.from_dict(
        {'label1': {0: 'RvB',
        1: 'RvB',
        2: 'RvB',
        3: 'RvB',
        4: 'RvB',
        5: 'RvB',
        6: 'RvB'},
        'label2': {0: '',
        1: 'Sales & Marketing',
        2: 'Sales & Marketing',
        3: 'Sales & Marketing',
        4: 'Sales & Marketing',
        5: 'Finance & ICT',
        6: 'Finance & ICT'},
        'label3': {0: '',
        1: '',
        2: 'Sales',
        3: 'Marketing',
        4: 'Marketing',
        5: '',
        6: 'Finance'},
        'label4': {0: '',
        1: '',
        2: '',
        3: '',
        4: 'Marketing Support',
        5: '',
        6: ''},
        'dept': {0: 10, 1: 100, 2: 110, 3: 120, 4: 121, 5: 200, 6: 210},
        'mgr': {0: 'John',
        1: 'Sally',
        2: 'Ruben',
        3: 'Holly',
        4: 'Max',
        5: 'Mimi',
        6: 'Astrid'}})
    print(src_df)
    h2 = Hierarchy.from_structure(src_df, 'dept', ['label1','label2','label3','label4'], root='0')
    print('\ndef_df frame:')
    print(h2.def_df)
