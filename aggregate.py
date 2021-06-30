#!/usr/bin/env python3
import yaml
import json
import ast
import re
import sys
import pandas as pd
import argparse
import collections
import datetime
import dateutil

#For debugging
#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)
#pd.set_option('display.expand_frame_repr', None)

KEYS = ['subject_id', 'task']
RETIREMENT_COUNT = 3 #I believe that this is the same for all workflows at all times. Can be parameterised in workflows.yaml if need be.

parser = argparse.ArgumentParser()
parser.add_argument('workflows',
                    nargs = '?',
                    default = 'development_workflows',
                    help = 'Label for workflows to process (see workflows.yaml).')
parser.add_argument('--text_threshold', '-t',
                    type = float,
                    default = 0.9,
                    help = 'Text consensus threshold')
parser.add_argument('--dropdown_threshold', '-d',
                    type = float,
                    default = 0.66,
                    help = 'Dropdown consensus threshold')
parser.add_argument('--unfinished', '-u',
                    action = 'store_true',
                    help = 'Include cases with insufficient number of classifications')
parser.add_argument('--verbose', '-v',
                    type = int,
                    default = 0,
                    help = 'Set to higher numbers for increasing verbosity')
parser.add_argument('--output', '-o',
                    default = 'joined.csv',
                    help = 'Set name of output file (will always go in output/ dir)')
parser.add_argument('--exports', '-e',
                    default = 'exports',
                    help = 'Directory of exports from the Zooniverse project')
parser.add_argument('--reduced', '-r',
                    default = 'aggregation',
                    dest = 'dir',
                    help = 'Directory containing data reduced by Panoptes scripts.')
args = parser.parse_args()
if args.workflows == 'development_workflows':
  print('*** TEST MODE')
  args.dir = 'doctored'
  args.unfinished = True
PREFIX=f'{args.exports}/hms-nhs-the-nautical-health-service'

with open('workflow.yaml') as f:
  workflow = yaml.load(f, Loader = yaml.Loader)

#Read in the reduced data.
columns = []
bad = {}
autoresolved = {}
TEXT_T = workflow['definitions']['TEXT_T']
DROP_T = workflow['definitions']['DROP_T']


#Recieve:
#'candidates': A dict of category names and votes for that category
#'threshold': A threshold for winning
#'subject_task': Key for logging
#'workflow_name': Value for logging
#If there is a winner, return the winning category and its number of votes as a single-entry dict
#If the winner was not unanimous, also log this in 'autoresolved'
#If there is not a winner, return the input dict and log this in 'bad'
#The way this is written, the reference we return will be the same as the first parameter iff there is no winner.
#We can also detect success by checking that the length of the returned dict is 1 (if the original input contained only 1 key, then that key is by definition the winner; otherwise, the dict of the winner is a single-entry dict)
def category_resolver(candidates, threshold, subject_task, workflow_name):
  total_votes = sum(candidates.values())
  for selection, votes in candidates.items():
    if votes == total_votes:
      return {selection: votes}
    if votes / total_votes >= threshold: #data has been autoresolved
      if subject_task in autoresolved: autoresolved[subject_task].append(workflow_name)
      else: autoresolved[subject_task] = [workflow_name]
      return {selection: votes}
  bad[subject_task] = '*'
  return candidates


for wid, data in workflow[args.workflows].items():
  datacol = data['ztype']['name']
  conflict_keys = []
  reduced_file = f'{args.dir}/{data["ztype"]["type"]}_reducer_{wid}.csv'
  if args.verbose >= 0: print(f'*** Processing {reduced_file}')
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
    if not args.unfinished:
      #Drop all classifications that are based on an insufficient number of views
      df.drop(df[df['data.number_views'] < RETIREMENT_COUNT].index)

      #Report on rows with different counts
      if args.verbose >= 1:
        overcount = df[df['data.number_views'] > RETIREMENT_COUNT]
        print(f'  Completed rows: {len(df.index)} (of which {len(overcount.index)} overcounted)')
        if args.verbose >= 2 and not overcount.empty: print(overcount)
        undercount = df[df['data.number_views'] < RETIREMENT_COUNT]
        print(f'  Undercounted rows: {len(undercount.index)}')
        if args.verbose >= 2 and not undercount.empty: print(undercount)

    #Process data for output
    #Strings use Levenshtein distance approach, IIRC
    #Take a different approach for non-string data
    def resolver(x):
      if pd.isnull(x['data.consensus_score']) or pd.isnull(x['data.number_views']): return ''

      if data['nptype'] == str:
        if x['data.consensus_score'] / x['data.number_views'] < args.text_threshold:
          bad[x.name] = '*'
          return x['data.aligned_text']
        else:
          if x['data.consensus_score'] != x['data.number_views']: #data has been autoresolved
            if x.name in autoresolved: autoresolved[x.name].append(data['name'])
            else: autoresolved[x.name] = [data['name']]
          return x[datacol]
      elif data['nptype'] == pd.Int64Dtype:
        candidates = ast.literal_eval(x['data.aligned_text'])

        if(len(candidates) != 1): #Not a conventional case, resolve manually
          return x['data.aligned_text']

        candidates = candidates[0]

        #If there are any non-numerals in the input, just return it to resolve manually
        if any(map(lambda x: not x.isdigit(), candidates)):
          return x['data.aligned_text']
        candidates = map(lambda x: int(x), candidates)
        candidates = category_resolver(collections.Counter(candidates), args.dropdown_threshold, x.name, data['name'])
        if len(candidates) == 1: return next(iter(candidates)) #First key, efficiently (see https://www.geeksforgeeks.org/python-get-the-first-key-in-dictionary/)
        else: return x['data.aligned_text']
      elif data['nptype'] == datetime.date:
        candidates = ast.literal_eval(x['data.aligned_text'])

        if(len(candidates) != 1): #Not a conventional case, resolve manually
          return x['data.aligned_text']

        candidates = candidates[0]
        #https://stackoverflow.com/a/18029112 has a trick for reading arbitrary date formats while rejecting ambiguous cases
        #We just need to use the documented format, but we can be a bit forgiving
        try:
          candidates = [dateutil.parser.parse(d, dayfirst = True) for d in candidates] #yearfirst defaults to False
        except (TypeError, ValueError): #Something is wrong, resolve manually
          return x['data.aligned_text']
        candidates = category_resolver(collections.Counter(candidates), args.dropdown_threshold, x.name, data['name'])
        if len(candidates) == 1:
          date = next(iter(candidates))
          return date.strftime('%d-%m-%Y')
        else: return x['data.aligned_text']
      else: raise Exception()
    df[datacol] = df.apply(resolver, axis = 'columns')
    #TODO: For these kinds of strings, may well be better to treat them like dropdowns and just take two thirds identical as permitting auto-resolve
  elif(data['ztype'] == DROP_T):
    #Drop all classifications that are based on an insufficient number of views
    if not args.unfinished:
      def votecounter(x):
        selections = ast.literal_eval(x)
        if len(selections) != 1: raise Exception()
        return(sum(selections[0].values()))
      df = df[df[datacol].apply(votecounter) >= RETIREMENT_COUNT]

      #Report on rows with different counts
      if args.verbose >= 1:
        overcount = df[df[datacol].apply(votecounter) > RETIREMENT_COUNT]
        print(f'  Completed rows: {len(df.index)} (of which {len(overcount.index)} overcounted)')
        if args.verbose >= 2 and not overcount.empty: print(overcount)
        undercount = df[df[datacol].apply(votecounter) < RETIREMENT_COUNT]
        print(f'  Undercounted rows: {len(undercount.index)}')
        if args.verbose >= 2 and not undercount.empty: print(undercount)

    #Process classifications for output
    def resolver(x):
      #x is a single-element array, containing one dictionary
      selections = ast.literal_eval(x[datacol])
      if len(selections) != 1: raise Exception()
      return str([category_resolver(selections[0], args.dropdown_threshold, x.name, data['name'])])
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

if args.verbose >= 0: print('*** Generating output')
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
joined.to_csv(path_or_buf = f'output/{args.output}', float_format = '%.0f')
