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
for wid, data in workflow.items():
  datacol = data['ztype']['name']
  #print(f'aggregation/{data["ztype"]["type"]}_reducer_{wid}.csv')
  df = pd.read_csv(f'{DIR}/{data["ztype"]["type"]}_reducer_{wid}.csv',
                   index_col = KEYS,
                   usecols   = KEYS + [datacol],
                   converters = {'task': lambda x: x[1:]}, #Could replace this with something that returns 1 through 25 over and over
                   dtype     = {datacol: data['nptype']})
  df.rename(columns={datacol: data['name']}, inplace = True)

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
joined.to_csv(path_or_buf = f'{DIR}/joined.csv')
