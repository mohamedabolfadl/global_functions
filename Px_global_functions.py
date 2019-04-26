# -*- coding: utf-8 -*-
"""
CREATED: Sat Apr 20 21:23:54 2019

AUTHOR: Mohamed Ibrahim (mohamed_ibrahim@mckinsey.com)

SCRIPT NAME: P.py

Purpose:

Input:
    1.
    2.
    3.

Output:
    1.
    2.

NOTES:
[NA]

ISSUES:
[NA]


"""




#################################
#%     LIBRARIES             --#
#################################
#import numpy as np
#import pandas as pd
#import seaborn as sns
#import pyarrow as pa


#################################
#%        FUNCTIONS           --#
#################################


#-- Purpose:
#   save file as parquet
def save_parquet(dt,filename):
    import pyarrow as pa
    table = pa.Table.from_pandas(dt)
    import pyarrow.parquet as pq
    if ".parquet" in filename:
        pq.write_table(table, filename)
    else:
        pq.write_table(table, filename+'.parquet')



#-- Purpose:
#   read parquet file to dataframe
def read_parquet(filename):
    import pyarrow.parquet as pq
    table = pq.read_table(filename)
    dt = table.to_pandas()
    return dt



#-- Purpose:
#   Print all rows
def disp(x):
    import pandas as pd
#    pd.set_option('display.max_rows', len(x))
    pd.set_option('display.max_columns', None)  # or None
    pd.set_option('display.max_rows', 20)  # or None
    pd.set_option('display.max_colwidth', 20)  # or -1
    #pd.set_option('display.width',None)
    pd.set_option('display.expand_frame_repr', False)
    print(x)
    pd.reset_option('display.max_rows')
    pd.reset_option('display.max_columns')
    pd.reset_option('display.max_colwidth')
    pd.reset_option('display.width')
    pd.reset_option('display.expand_frame_repr')
#    pd.describe_option('display')


#-- Purpose:
#   flatten nested lists
def flatten_list(dt):
    import numpy as np
    return np.array([item for sublist in dt  for item in sublist])



def print_it(s):
    print(s)

