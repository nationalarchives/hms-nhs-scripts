#!/usr/bin/env python3

import pandas as pd
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument('extraction', nargs = '+')
parser.add_argument('--tranche', '-t')
parser.add_argument('--suffix', '-s', default = '.new')
args = parser.parse_args()

tranche_df = pd.read_csv(args.tranche, index_col = [0, 1])

for extraction in args.extraction:
  #Read in the extractions and drop all classifications relating to completed tasks
  extraction_df = pd.read_csv(extraction, na_filter = False, index_col = False, dtype = {'data.text': str, 'data.value': str})
  extraction_cols = list(extraction_df.columns)
  extraction_df = extraction_df.set_index(['subject_id', 'task'])
  extraction_df = extraction_df.join(tranche_df['complete'], how = 'left')
  extraction_df = extraction_df[extraction_df['complete'] != True] #odd logic to handle NaN correctly

  #Restore to original format.
  #If no classifications were complete in previous tranche(s) then this leaves us with an identify transform.
  extraction_df = extraction_df.reset_index()
  extraction_df['tmp'] = extraction_df.apply(lambda x: int(x['task'][1:]), axis = 'columns')
  extraction_df = extraction_df.sort_values(by = ['classification_id', 'tmp'])
  extraction_df = extraction_df.reindex(columns = extraction_cols)

  extraction_df.to_csv(path_or_buf = f'{extraction}{args.suffix}', float_format = '%.99g', index = False)
