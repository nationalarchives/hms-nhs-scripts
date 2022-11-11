#!/usr/bin/env python3
import os
import re
import sys
import json
import pandas as pd
from collections import defaultdict

#For debugging
#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)
#pd.set_option('display.expand_frame_repr', None)

def get_subjects_df(cache_file):
  return pd.read_csv(cache_file, index_col = 'subject_id', dtype = {'subject_id': int, 'volume': int, 'page': int, 'location': str})

def create_subjects_df(exports_subj_file, cache_file, supplements_dict = None, drop_raw = True):
  def translate_metadata(x):
    fnam = json.loads(x)['Filename']
    match = re.fullmatch('.*_(\d+)-(\d+)(?: \d)?\.jpg', fnam)
    if match:
      (vol, page) = [int(x) for x in match.groups()]
      if   vol ==  1: page -= 21
      elif vol ==  2: page -= 28
      elif vol ==  6: raise Exception('Surprisingly met volume 6')
      elif vol == 20: page += 24
      elif vol  > 30: page -=  2
      else: page -= 3
    else: raise Exception(f'"{fnam}" does not match regular expression')
    return pd.Series([vol, page])

  def translate_location(x):
    location = json.loads(x)
    assert len(location) == 1
    location  = next(iter(location.values()))
    assert isinstance(location, str)
    return location

  def dump_missing(base_df, gaps_message, no_gaps_message):
    vol_page_series = base_df.set_index('volume')['page']
    messages = []
    for v in vol_page_series.index.unique():
      low = vol_page_series.loc[v].min()
      high = vol_page_series.loc[v].max()
      gaps = set(range(low, high + 1)) - set(vol_page_series.loc[v])
      if gaps != set():
        messages.append(f'Missing pages in vol {v}: ' + ', '.join([str(x) for x in sorted(gaps)]))
    if len(messages):
      print(gaps_message)
      print('\n'.join(messages))
    else:
      print(no_gaps_message)
    print()


  subjects = pd.read_csv(exports_subj_file,
                         usecols = ['subject_id', 'metadata', 'locations'],
                         dtype = {'subject_id': int, 'metadata': str, 'locations': str})
  #Drop all duplicates
  subjects = subjects.drop_duplicates().set_index('subject_id')
  if not subjects.index.is_unique: raise Exception('Non-unique subjects')
  subjects[['volume','page']] = subjects['metadata'].apply(translate_metadata)
  subjects['location'] = subjects['locations'].apply(translate_location)
  if drop_raw:
    subjects = subjects.drop(['metadata', 'locations'], axis = 1)

  dump_missing(subjects, 'Missing pages *before* applying supplements', 'No missing pages *before* applying supplements')
  if supplements_dict:
    values = defaultdict(list)
    for subj_id, metadata in supplements_dict.items():
      if subj_id in subjects.index: raise Exception(f'Supplementary subject {subj_id} already present in subjects')
      for k, v in metadata.items(): values[k].append(v)
    supplements_df = pd.DataFrame(values, index = list(supplements_dict.keys()))
    subjects = pd.concat([subjects, supplements_df])
  else:
    supplements_df = pd.DataFrame(data = {'volume': [], 'page': [], 'location': []}, index = pd.Index([], dtype = int, name = 'subject_id'))
  dump_missing(subjects, 'Missing pages *after* applying supplements', 'No missing pages *after* applying supplements')

  for c in ('volume', 'page'):
    if subjects[c].lt(1).any():
      print(f'Warning: {c} numbers below 1', file = sys.stderr)
      print(subjects[subjects[c].lt(1)], file = sys.stderr)

  vol_page_df = subjects.reset_index().rename({'index': 'subject_id'}, axis = 1).set_index(['volume', 'page']).sort_index()
  if not vol_page_df.index.is_unique:
    dups = vol_page_df[vol_page_df.index.duplicated(keep = False)]
    print('Warning: Found multiple subject_ids for the following pages', file = sys.stderr)
    print(dups[['subject_id', 'location']], file = sys.stderr)
    #raise Exception() #FIXME: Confirm that only one of each set of duplicates actually has annotations.
    #                          I have done this by hand at the point of spotting the problem, but the script
    #                          should be making sure.
    #                          Need completed exports to do this, so do it at either the end of extract.py, or at the beginning of aggregate.py
    #                          Just before reduction would be a good place -- the final extraction file tells us
    #                          what we need to know, and reduction is costly.

  subjects = subjects.sort_index()
  subjects.to_csv(cache_file, index_label = 'subject_id')

  #If I ever need the "other" dfs outside of a context where I am calling this function, then I can always dump them to CSV and write a function here to recover them
  return (subjects, supplements_df, dups)

#Usage example:
#Just do the auto-checks -- no manual checks
#./subjects.py exports/hms-nhs-the-nautical-health-service-subjects.csv

#Manually check the final page in each volume from 30 to 34
#./subjects.py exports/hms-nhs-the-nautical-health-service-subjects.csv 30 34 -1

#Manually check pages 1, 50 and 100 in every volume
#./subjects.py exports/hms-nhs-the-nautical-health-service-subjects.csv -1 -1 1 50 100
def main():
  import yaml
  from tempfile import TemporaryDirectory

  with open('workflow.yaml') as f:
    supplements = yaml.load(f, Loader = yaml.Loader)['subjects']['supplements']
  with TemporaryDirectory() as td:
    fnam = f'{td}/subjects_metadata.csv'
    create_subjects_df(sys.argv[1], fnam, supplements, False)
    if len(sys.argv) == 2: return

    subjects = get_subjects_df(fnam) #Note: this will fail if I have not already created the file!

  if len(sys.argv) >= 3:
    min_vol = max_vol = int(sys.argv[2])
  if len(sys.argv) >= 4:
    max_vol = int(sys.argv[3])
  if len(sys.argv) >= 5:
    offsets = [int(x) for x in sys.argv[4:]]
  else:
    offsets = [0, -1] #first and final row

  if min_vol < 0:
    min_vol = subjects.volume.min()
    max_vol = subjects.volume.max()

  df = subjects.reset_index().set_index(['volume', 'page']).sort_index()
  import pprint
  import webbrowser
  for volume in range(min_vol, max_vol + 1):
    if volume == 6: continue #there should be no volume 6
    bad = []
    for index in offsets:
      row = df.loc[volume].iloc[index]
      page = subjects.loc[row.subject_id].page
      webbrowser.open(row.location, new=0, autoraise=False)
      print('Page  Volume  Location  Metadata')
      print(page, volume, row.location, row.metadata)
      print(f'Page number should be {page}')
      i = None
      while i == None:
        i = input('Enter the actual page number to log a bad page. Just press enter to continue. ')
        try:
          if i == '': break
          if not int(i) == page:
            neighbour = 1 if index == 0 else -2
            bad.append({'volume': volume,
                        'computed page number': page,
                        'actual page number': i,
                        'url': row.location,
                        'raw metadata': row.metadata,
                        'neighbour': df.loc[volume].iloc[neighbour].location
                       })
        except ValueError: i = None
    for x in bad: pprint.pp(x)

if __name__ == "__main__":
  main()
