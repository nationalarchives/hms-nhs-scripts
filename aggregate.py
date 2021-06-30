#!/usr/bin/env python3
import yaml
import json
import ast
import re
import sys
import pandas as pd
import argparse

#For debugging
#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)
#pd.set_option('display.expand_frame_repr', None)

KEYS = ['subject_id', 'task']

parser = argparse.ArgumentParser()
parser.add_argument('--reduced', '-r',
                    default = 'aggregation',
                    dest = 'dir',
                    help = 'Directory containing data reduced by Panoptes scripts.\nShould be "doctored" for basic testing.\nShould be "aggregation" for real processing.')
parser.add_argument('--workflows', '-w',
                    default = 'launch_workflows',
                    help = 'Label for workflows to process (see workflows.yaml). Set to "development_workflows" for basic testing.')
parser.add_argument('--text_threshold', '-t',
                    type = float,
                    default = 0.9,
                    help = 'Text consensus threshold')
parser.add_argument('--dropdown_threshold', '-d',
                    type = float,
                    default = 0.66,
                    help = 'Dropdown consensus threshold')
parser.add_argument('--exports', '-e',
                    default = 'exports',
                    help = 'Directory of exports from the Zooniverse project')
args = parser.parse_args()
PREFIX=f'{args.exports}/hms-nhs-the-nautical-health-service'

with open('workflow.yaml') as f:
  workflow = yaml.load(f, Loader = yaml.Loader)

#Read in the reduced data.
columns = []
bad = {}
autoresolved = {}
TEXT_T = workflow['definitions']['TEXT_T']
DROP_T = workflow['definitions']['DROP_T']
for wid, data in workflow[args.workflows].items():
  datacol = data['ztype']['name']
  conflict_keys = []
  reduced_file = f'{args.dir}/{data["ztype"]["type"]}_reducer_{wid}.csv'
  print(f'*** Processing {reduced_file}')
  if data['ztype'] == TEXT_T:
    conflict_keys = ['data.aligned_text', 'data.number_views', 'data.consensus_score']
  try:
    df = pd.read_csv(reduced_file,
                     index_col = KEYS,
                     usecols   = KEYS + [datacol] + conflict_keys,
                     converters = {'task': lambda x: x[1:]}, #Could replace this with something that returns 1 through 25 over and over
                     dtype     = {datacol: str})
  except:
    print(f'Error while reading {reduced_file}')
    raise

  #Handle conflicts
  if(data['ztype'] == TEXT_T):
    #Levenshtein distance approach, IIRC
    def resolver(x):
      if pd.isnull(x['data.consensus_score']) or pd.isnull(x['data.number_views']): return ''

      if x['data.consensus_score'] / x['data.number_views'] < args.text_threshold:
        bad[x.name] = '*'
        return x['data.aligned_text']
      else:
        if x['data.consensus_score'] != x['data.number_views']: #data has been autoresolved
          if x.name in autoresolved: autoresolved[x.name].append(data['name'])
          else: autoresolved[x.name] = [data['name']]
        return x[datacol]
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
        if votes == total_votes:
          return str([{selection: votes}])
        if votes / total_votes >= args.dropdown_threshold: #data has been autoresolved
          if x.name in autoresolved: autoresolved[x.name].append(data['name'])
          else: autoresolved[x.name] = [data['name']]
          return str([{selection: votes}])
      bad[x.name] = '*'
      return str([selections])
    df[datacol] = df.apply(resolver, axis = 'columns')
  else: raise Exception()

  #Tidy up columns
  df.drop(conflict_keys, axis = 'columns', inplace = True) #Drop columns that we just brought in for conflict handling
  df.rename(columns={datacol: data['name']}, inplace = True) #Rename the data column to something meaninful

  #Convert dropdowns to their values
  if(data['ztype'] == DROP_T):
    labelfile = f'{args.dir}/Task_labels_workflow_{wid}_V{data["major"]}.{data["minor"]}.yaml'
    with open(labelfile) as f:
      labels = yaml.full_load(f)
    def decode_dropdown(selection_json):
        selections = ast.literal_eval(selection_json)
        if len(selections) != 1: raise Exception()
        selections = selections[0]
        result = {}
        for selection, votes in selections.items():
          if selection == 'None': result[None] = votes
          else:
            label = list(labels[f'T1.selects.0.options.*.{selection}.label'].values())[0]
            label = label[label.find('=') + 1:].strip()
            result[label] = votes
        if len(result) == 1:
          return list(result.keys())[0]
        else: return str(result)
    df[data['name']] = df[data['name']].map(decode_dropdown)

  columns.append(df)

print('*** Generating output')
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

#Record where there was autoresolution
joined.insert(0, 'Autoresolved', '')
#TODO: Again, doesn't feel like Pandas
for index, value in autoresolved.items():
  joined.at[index, 'Autoresolved'] = '; '.join(value)


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


#This feels ridiculous, but works in conjunction with maxcolwidth.sh to check for columns too wide for Excel
joined.to_csv(path_or_buf = f'output/lenchecker.csv', float_format = '%.0f', sep = '@')

#Dump output
joined.to_csv(path_or_buf = f'output/joined.csv', float_format = '%.0f')
