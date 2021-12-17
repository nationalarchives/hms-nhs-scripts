#!/usr/bin/env python3

import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('reduction', nargs = '+')
parser.add_argument('--suffix', '-s', default = '.new')
args = parser.parse_args()

for reduction in args.reduction:
  #Read in the reductions and replace everything that has reduced to an empty field with an explicit BLANK
  reduction_df = pd.read_csv(reduction, na_filter = False, index_col = False, dtype = {'data.consensus_text': str, 'data.aligned_text': str})
  reduction_cols = list(reduction_df.columns)
  if 'data.consensus_text' in reduction_cols:
    reduction_df['data.consensus_text'].replace('^\s*$', 'BLANK', regex = True, inplace = True)

  #Restore to original format
  reduction_df = reduction_df.reset_index()
  reduction_df['tmp'] = reduction_df.apply(lambda x: int(x['task'][1:]), axis = 'columns')
  reduction_df = reduction_df.sort_values(by = ['subject_id', 'tmp'])
  reduction_df = reduction_df.reindex(columns = reduction_cols)

  reduction_df.to_csv(path_or_buf = f'{reduction}{args.suffix}', index = False)
