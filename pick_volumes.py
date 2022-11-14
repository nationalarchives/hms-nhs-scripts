#!/usr/bin/env python3

import os
import sys
import argparse
import pandas as pd
import subjects

parser = argparse.ArgumentParser(description = 'This script removes subjects outside of a given volume range')
parser.add_argument('extraction', nargs = '+', help = 'Extractions file as produced by "panoptes_aggregation extract" (and possibly processed by other extraction scripts)')
parser.add_argument('--subjects', help = 'File containing record of views for each row in each subject')
parser.add_argument('--first_volume', required = True, type = int, help = 'Lowest volume number to include in the output')
parser.add_argument('--final_volume', required = True, type = int, help = 'Highest volume number to include in the output')
parser.add_argument('--suffix', '-s', default = '.vols.csv', help = 'Suffix to put on output extractions file: the output file will have the same name as the input extractions file, with this suffix appended. Default: ".vols".')
parser.add_argument('--subjects_cache', default = 'extraction/subjects_metadata.csv', help = 'Location of subject metadata cache. Default: "extraction/subjects_metadata.csv"')
args = parser.parse_args()

for extraction in args.extraction:
  #Read in the extractions and drop all classifications relating to completed tasks
  extraction_df = pd.read_csv(extraction, na_filter = False, index_col = None, dtype = {
      'classification_id': int,
      'user_name': str,
      'user_id': str,
      'workflow_id': int,
      'task': str,
      'created_at': str,
      'subject_id': int,
      'extractor': str,
      'data.text': str,
      'data.gold_standard': str,
      'data.value': str,
      'data.aggregation_version': str
    }
  )
  full_len = len(extraction_df)
  extraction_df = extraction_df.reset_index() #save the original index, so that we can preserve output order
  extraction_df = extraction_df.set_index('subject_id', drop = False)
  extraction_df = extraction_df.join(subjects.get_subjects_df(args.subjects_cache)['volume'])
  
  volumes = extraction_df['volume']
  if volumes.isna().any():
    raise Exception(f'''Null values in volume column for {extraction}
Implies that the following subject ids are not in the metadata: ''' + 
      ', '.join([f'{x}' for x in extraction_df[extraction_df.volume.isna()].index.drop_duplicates().values]))
  if not args.first_volume in set(volumes): print(f'Warning: Start volume {args.first_volume} not in volumes', file = sys.stderr)
  if not args.final_volume  in set(volumes): print(f'Warning: Stop volume {args.final_volume} not in volumes', file = sys.stderr)
  print('Volumes in dataset:', ', '.join(map(lambda x: str(x), sorted(volumes.unique()))))
  print('Volumes taken:     ', ', '.join([str(x) for x in filter(lambda x: x in volumes.unique(), range(args.first_volume, args.final_volume + 1))]))

  extraction_df = extraction_df[extraction_df['volume'].between(args.first_volume, args.final_volume)]
  extraction_df = extraction_df.drop('volume', axis = 1)
  extraction_df = extraction_df.set_index('index').sort_index() #return to original order (handy for diff-comparison)
  extraction_df.to_csv(path_or_buf = f'{extraction.split(".", 1)[0]}{args.suffix}', float_format = '%.99g', index = False)
  final_len = len(extraction_df)

  print(f'Removed {full_len - final_len} wrong-volume rows from {extraction} to create {extraction}{args.suffix} with {final_len} rows.')
