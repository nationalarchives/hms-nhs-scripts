#!/usr/bin/env python3

import pandas as pd
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument('extraction', nargs = '+')
parser.add_argument('--tranche', '-t')
parser.add_argument('--suffix', '-s', default = '.new')
parser.add_argument('--no_sort', action = 'store_true')
args = parser.parse_args()

tranche_df = pd.read_csv(args.tranche, index_col = ['subject_id', 'task'],
                         usecols = ['subject_id', 'task', 'complete'])

for extraction in args.extraction:
  #Read in the extractions and drop all classifications relating to completed tasks
  extraction_df = pd.read_csv(extraction, na_filter = False, index_col = False, dtype = {'data.text': str, 'data.value': str},
                              converters = {'task': lambda x: x[1:]} ) #Here the task has leading T, but in tranche_df it does not
  extraction_df['task'] = extraction_df['task'].astype('int32')
  extraction_cols = list(extraction_df.columns)
  extraction_df = extraction_df.set_index(['subject_id', 'task'])
  extraction_df = extraction_df.join(tranche_df['complete'], how = 'left')
  extraction_df = extraction_df[extraction_df['complete'] != True] #odd logic to handle NaN correctly

  #Restore to original format.
  #If no classifications were complete in previous tranche(s) and we do not sort (or if the
  #sorting happens to be a nop, which it seems that it is when we have not concatenated extractions)
  #then this leaves us with an identify transform.
  extraction_df = extraction_df.reset_index()
  if not args.no_sort:
    extraction_df = extraction_df.sort_values(by = ['classification_id', 'task']) #TODO: Would it make more sense to sort by subject_id and task?
  extraction_df = extraction_df.reindex(columns = extraction_cols)

  extraction_df['task'] = 'T' + extraction_df['task'].astype(str)
  extraction_df.to_csv(path_or_buf = f'{extraction}{args.suffix}', float_format = '%.99g', index = False)
