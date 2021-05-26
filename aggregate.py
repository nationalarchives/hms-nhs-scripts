#!/usr/bin/env python3
import pprint
import yaml
import json
import ast
import re
import sys
import numpy as np
import pandas as pd

#For debugging
#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)
#pd.set_option('display.expand_frame_repr', None)

PREFIX='hms-nhs-the-nautical-health-service'
DIR='doctored'
KEYS = ['subject_id', 'task']
TEXT_CONSENSUS_THRESHOLD = 0.9
DROPDOWN_CONSENSUS_THRESHOLD = 0.66
DROP_T = {
  'type': 'dropdown',
  'name': 'data.value',
}
TEXT_T = {
  'type': 'text',
  'name': 'data.consensus_text',
}
workflow = {
  18109: {
    'ztype': TEXT_T,
    'nptype': pd.Int64Dtype(),
    'name': 'admission number',
  },
  18110: {
    'ztype': TEXT_T,
    'nptype': str,
    'name': 'date admitted',
  },
  18111: {
    'ztype': TEXT_T,
    'nptype': str,
    'name': 'name',
  },
  18112: {
    'ztype': DROP_T,
    'nptype': str,
    'name': 'rank/rating',
    'major': 5,
    'minor': 8,
  },
  18113: {
    'ztype': TEXT_T,
    'nptype': pd.Int64Dtype(),
    'name': 'age',
  },
  18114: {
    'ztype': TEXT_T,
    'nptype': str,
    'name': 'place of birth',
  },
  18115: {
    'ztype': TEXT_T,
    'nptype': str,
    'name': 'port sailed out of'
  },
  18116: {
    'ztype': TEXT_T,
    'nptype': pd.Int64Dtype(),
    'name': 'years at sea',
  },
  18117: {
    'ztype': TEXT_T,
    'nptype': str,
    'name': 'last ship',
  },
  #18118: {
  #  'ztype': TEXT_T,
  #  'nptype': str,
  #  'name': admission circumstances',
  #},
  18119: {
    'ztype': TEXT_T,
    'nptype': str,
    'name': 'date discharged',
  },
  18120: {
    'ztype': DROP_T,
    'nptype': str,
    'name': 'how disposed',
    'major': 1,
    'minor': 1,
  },
  18121: {
    'ztype': TEXT_T,
    'nptype': pd.Int64Dtype(),
    'name': 'days victualled/in hospital',
  },
}

#Read in the reduced data.
columns = []
bad = {}
for wid, data in workflow.items():
  datacol = data['ztype']['name']
  #print(f'aggregation/{data["ztype"]["type"]}_reducer_{wid}.csv')
  conflict_keys = []
  if data['ztype'] == TEXT_T:
    conflict_keys = ['data.aligned_text', 'data.number_views', 'data.consensus_score']
  df = pd.read_csv(f'{DIR}/{data["ztype"]["type"]}_reducer_{wid}.csv',
                   index_col = KEYS,
                   usecols   = KEYS + [datacol] + conflict_keys,
                   converters = {'task': lambda x: x[1:]}, #Could replace this with something that returns 1 through 25 over and over
                   dtype     = {datacol: data['nptype']})

  #Handle conflicts
  if(data['ztype'] == TEXT_T):
    #Levenshtein distance approach, IIRC
    def resolver(x):
      if x['data.consensus_score'] / x['data.number_views'] < TEXT_CONSENSUS_THRESHOLD:
        bad[x.name] = '*'
        return x['data.aligned_text']
      else: return x[datacol]
    df[datacol] = df.apply(resolver, axis = 'columns')
    #TODO: For these kinds of strings, may well be better to treat them like dropdowns and just take two thirds identical as permitting auto-resolve
  elif(data['ztype'] == DROP_T):
    def resolver(x):
      #x is a single-element array, containing one dictionary
      selections = ast.literal_eval(x[datacol])
      if len(selections) != 1: raise Exception()
      selections = selections[0]

      total_votes = sum(selections.values())
      for selection, votes in selections.items():
        if votes / total_votes >= DROPDOWN_CONSENSUS_THRESHOLD:
          return str([{selection: votes}])
      bad[x.name] = '*'
      return x
    df[datacol] = df.apply(resolver, axis = 'columns')
  else: raise Exception()

  #Tidy up columns
  df.drop(conflict_keys, axis = 'columns', inplace = True) #Drop columns that we just brought in for conflict handling
  df.rename(columns={datacol: data['name']}, inplace = True) #Rename the data column to something meaninful

  #Convert dropdowns to their values
  if(data['ztype'] == DROP_T):
    def decode_dropdown(selection_json):
      with open(f'{DIR}/Task_labels_workflow_{wid}_V{data["major"]}.{data["minor"]}.yaml') as f:
        labels = yaml.full_load(f)
        selections = ast.literal_eval(selection_json)
        if len(selections) != 1: raise Exception()
        selections = selections[0]
        result = {}
        for selection, votes in selections.items():
          if selection == 'None': result[None] = votes
          else: result[list(labels[f'T1.selects.0.options.*.{selection}.label'].values())[0]] = votes
        if len(result) == 1:
          return list(result.keys())[0]
        else: return str(result)
    df[data['name']] = df[data['name']].map(decode_dropdown)

  columns.append(df)

#TODO: Deal with conflicts


#Combine the separate workflows into a single dataframe
#Assumption: Task numbers always refer to the same row in each workflow
#            If this assumption does not hold, we can perform a mapping
#            on the dataframes at the point that we read them in, above.
#Quick test shows that this assumption does hold for now.
first = columns.pop(0)
joined = first.join(columns, how='outer')


#Tag the rows with badness
joined.insert(0, 'Problems', '')
joined['Problems'] = joined.apply(lambda x: 'Blank(s)' if x.isnull().values.any() else '', axis = 'columns')

#TODO This part does not feel like the Pandas way
for b in bad.keys():
  x = joined.at[b, 'Problems']
  if len(x):
    joined.at[b, 'Problems'] = f'{x} & disagreements'
  else:
    joined.at[b, 'Problems'] = 'Disagreements'


#Translate subjects ids into original filenames
#Assumption: the metadata is invariant across all of the entries for each subject_id
subjects = pd.read_csv(f'{PREFIX}-subjects.csv',
                       usecols   = ['subject_id', 'metadata'])
joined.insert(0, 'volume', '')
joined.insert(1, 'page', '')
for sid in joined.index.get_level_values('subject_id').unique():
  metadata = subjects.query(f'subject_id == {sid}').iloc[0]['metadata']
  fnam = json.loads(metadata)['Filename']
  match = re.fullmatch('.*_(\d+)-(\d+)(?: \d)?\.jpg', fnam)
  if match:
    (vol, page) = map(lambda x: int(x), match.groups())
    if   vol == 1: page -= 21
    elif vol == 2: page -= 28
    elif vol == 6:
      print('Surprisingly met volume 6', file = sys.stderr)
      (vol, page) = ('6', '?')
    else: page -= 3
  else:
    print(f'"{fnam}" does not match regular expression', file = sys.stderr)
    (vol, page) = '?', '?'
  joined.loc[[sid], 'volume'] = [vol]  * 25
  joined.loc[[sid], 'page']   = [page] * 25

#Dump output
#print(joined)
joined.to_csv(path_or_buf = f'{DIR}/joined.csv', float_format = '%.0f')
