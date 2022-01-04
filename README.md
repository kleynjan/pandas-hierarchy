# Wrapper class to support multi-level trees in Pandas

While Pandas has extensive *multi-index* capabilities, which can also be arranged in a hierarchical structure, the assumption is / seems to be that these indexes are for independent dimensions. Every index in a multi-index represents a *different* dimension.

This class is intended for cases where a *single* dimension fits a tree structure, and you want to aggregate across that dimension ('rollups', etc). 

Possible applications:
* org chart, where you have multiple employees per dept, and departments organized into a tree
* taxonomies, where you'd have animal -> mammal -> dog -> dobermann

In all of these cases, you'd want to be able to run different aggregations, and want those at all available levels of the hierarchy: how many *mammals*? how many *dogs*? (as well as: how many *dobermanns*, but you probably have that one already by agg(count) on your base data).

### Background

Previously, for these cases I used to repeat the groupby on the dimension identifier for each level of the hierarchy, concatenating the results into a single dataframe. With larger hierarchies this became quite unwieldy and slow. 

This class uses an entirely different approach: in the constructor, all paths in the hierarchy are retrieved using a networkx DiGraph. This way, we map each item in the hierarchy to all of its ancestors, using pandas' melt to create a flat normalized form. 

Since subclassing DataFrames is quite challenging, the class simply wraps a DataFrame that defines the hierarchy (self.def_df). Composition over inheritance, anyway.

### Example

Given some source data:

```
# pers: our employees
#
  dept  pnr    name   #
0    1  456    John   # top level manager
1   12  573   Peter   # parent dept -> 1
2  121  574    Paul   # parent dept -> 12
3  121  578    Mary   # parent dept -> 12
4  130  666  George   # parent dept -> 13

# org: our org tree 
#
   dept parent_dept
0     1           0
1    12           1
2   120          12
3   121          12
4   122          12
5    13           1
6   130          13
7  1301         130
8     2           0
9    21           2
```
we want to make a roll-up head count for all layers of the organizational tree:
```
  dept p_count
0    1      5
1   12      3
2  121      2
3   13      1
4  130      1
```
1. Initialize the hierarchy object:
```
h = Hierarchy(org, 'dept', '0')
# where:
#   org:     source df
#   'dept':  identifier for our department hierarchy
#   '0':     root_node (see parent_dept in the org above, first row)
#
# h.def_df now holds the 'hierarchy definition', along with any other attributes you might want
# h_ix is the index provided as a helper, to sort output based on the original hierarchy source
# h_level is the level of the hierarchy

print(h.def_df)
   dept parent_dept  h_ix  h_level
0     1           0     0        1
1    12           1     1        2
2   120          12     2        3
3   121          12     3        3
4   122          12     4        3
5    13           1     5        2
6   130          13     6        3
7  1301         130     7        4
8     2           0     8        1
9    21           2     9        2
```
2. Expand a given source df (in our case, employees) to create a normalized/melted table that we can run our aggregation on: 
```
exp = h.expand(pers, add_cols=['h_ix', 'h_level'])
print(exp)

   dept  pnr    name  h_ix h_level
0     1  456    John     0       1
1     1  573   Peter     0       1
2     1  574    Paul     0       1
3     1  578    Mary     0       1
4     1  666  George     0       1
5    12  573   Peter     1       2
6    12  574    Paul     1       2
7    12  578    Mary     1       2
9   121  574    Paul     3       3
10  121  578    Mary     3       3
8    13  666  George     5       2
11  130  666  George     6       3
```
As you can see, an employee that works for dept=121 is now also included for dept=12 and dept=1.

3. Finally, QED:
```
qed = exp.groupby('dept')[['pnr']].count()
print(qed)   # =rollup employee counts

      pnr
dept     
1       5
12      3
121     2
13      1
130     1

```

### TODO

The from_structure class method is a convenience function if your source dataframe for the hierarchy doesn't have a nice identifier -> parent_identifier structure. It will construct the hierarchy from a struc_field in the form 'animal|mammal|dog' (or a list of columns that can be used in this same manner). This method needs some work, it doesn't fit all logical use cases at the moment.

