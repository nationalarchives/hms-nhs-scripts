#!/usr/bin/env python3

import pandas as pd
import argparse
import os

parser = argparse.ArgumentParser(description = 'This script removed previously-processed data from the extractions file, saving us from regenerating it.')
parser.add_argument('extraction', nargs = '+', help = 'Extractions file as produced by "panoptes_aggregation extract"')
parser.add_argument('--tranche', '-t', help = 'File containing record of views for each row in each subject')
parser.add_argument('--suffix', '-s', default = '.stripped.csv', help = 'Suffix to put on output extractions file: the output file will be named as the input file, but with this as its name extension. Default: ".stripped.csv".')
parser.add_argument('--no_sort', action = 'store_true', help = 'By default, this script sorts the output extractions file by classification_id and task number. Set this option to output the extractions file in the same order as the input file. If -t specifies no previously complete rows and --no_sort is set, then the input and output files are identical.')
args = parser.parse_args()

tranche_df = pd.read_csv(args.tranche, index_col = ['subject_id', 'task'],
                         usecols = ['subject_id', 'task', 'complete'],
                         dtype = {'subject_id': int, 'task': int, 'complete': bool})

for extraction in args.extraction:
  #Read in the extractions and drop all classifications relating to completed tasks
  extraction_df = pd.read_csv(extraction, na_filter = False, index_col = False, dtype = {
      'classification_id': int,
      'user_name': str,
      'user_id': str,
      'workflow_id': int,
      'created_at': str,
      'subject_id': int,
      'extractor': str,
      'data.text': str,
      'data.gold_standard': str,
      'data.value': str,
      'data.aggregation_version': str
    },
    converters = {'task': lambda x: str(x)[1:]} ) #Here the task has leading T, but in tranche_df it does not
  extraction_df['task'] = extraction_df['task'].astype('int32')
  extraction_cols = list(extraction_df.columns)
  extraction_df = extraction_df.set_index(['subject_id', 'task'])
  extraction_df = extraction_df.join(tranche_df['complete'], how = 'left')
  full_len = len(extraction_df)
  extraction_df = extraction_df[extraction_df['complete'] != True] #odd logic to handle NaN correctly
  stripped_len = len(extraction_df)

  #Restore to original format.
  #If no classifications were complete in previous tranche(s) and we do not sort (or if the
  #sorting happens to be a nop, which it seems that it is when we have not concatenated extractions)
  #then this leaves us with an identify transform.
  extraction_df = extraction_df.reset_index()
  if not args.no_sort:
    extraction_df = extraction_df.sort_values(by = ['classification_id', 'task']) #TODO: Would it make more sense to sort by subject_id and task?
  extraction_df = extraction_df.reindex(columns = extraction_cols)

  extraction_df['task'] = 'T' + extraction_df['task'].astype(str)
  outname = extraction.split(".", 1)[0] + args.suffix
  extraction_df.to_csv(outname, float_format = '%.99g', index = False)

  print(f"Removed {full_len - stripped_len} rows from {extraction}. Output ({'unsorted' if args.no_sort else 'sorted'}) in {outname}.")
