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
import subprocess
import inspect
from collections import defaultdict

#For debugging
#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)
#pd.set_option('display.expand_frame_repr', None)

bad = defaultdict(int) #Keys of bad are indices of rows in the final dataframe
autoresolved = {} #Keys of autoresolved are indices of rows in the final dataframe
subjects = None #Initialised after arg parsing

parser = argparse.ArgumentParser()
parser.add_argument('workflows',
                    nargs = '?',
                    default = 'launch_workflows',
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
parser.add_argument('--blanks', '-b',
                    action = 'store_true',
                    help = 'Include pages with missing values')
parser.add_argument('--no_stamp', '-S',
                    action = 'store_true',
                    help = 'Do not stamp the output with information about the script used to generate it')
parser.add_argument('--flow_report', '-f',
                    action = 'store_true',
                    help = ('Show information about paths taken through program.\n'
                            'Used in conjunction with coverage.sh to make sure that test inputs are testing all paths.'
                           )
                   )
args = parser.parse_args()
subjects = pd.read_csv(f'{args.exports}/hms-nhs-the-nautical-health-service-subjects.csv',
                         usecols   = ['subject_id', 'metadata', 'locations'])


def flow_report(msg, row_id, value):
  if not args.flow_report: return

  caller = inspect.stack()[1]
  try: report_id = f'{caller.function} {msg}'
  finally: del caller
  if args.verbose >= 1:
    print(f'FR: {report_id} {row_id} {value}')
  else:
    if report_id not in flow_report.reported:
      flow_report.reported.add(report_id)
      print(f'FR: {report_id}')
flow_report.reported = set()

#Translate subject ids into (vol, page) tuple
#Assumption: the metadata is invariant across all of the entries for each subject_id
def get_subject_reference(subject):
  global subjects
  metadata = subjects.query(f'subject_id == {subject}').iloc[0]['metadata']
  fnam = json.loads(metadata)['Filename']
  match = re.fullmatch('.*_(\d+)-(\d+)(?: \d)?\.jpg', fnam)
  if match:
    (vol, page) = [int(x) for x in match.groups()]
    if   vol == 1: page -= 21
    elif vol == 2: page -= 28
    elif vol == 6: raise Exception('Surprisingly met volume 6')
    else: page -= 3
    return (vol, page)
  else: raise Exception(f'"{fnam}" does not match regular expression')


#"Port sailed out of" is not in volume 1, so doesn't count as a blank there
def has_blanks(row):
  without_port_sailed = row.drop('port sailed out of')
  if without_port_sailed.isnull().values.any(): return 'Blank(s)' #Something other than 'port sailed out of' is blank, so there are definitely blanks
  if pd.isnull(row['port sailed out of']): #if this is volume 1, this doesn't count as a blank. otherwise, it does.
    volume = get_subject_reference(row.name[0])[0]
    if volume != 1: return 'Blank(s)'
  return ''


#We can find these by looking for square brackets and for misplaced zeros
#But square brackets will show up in every cell that was already flagged as bad
#So we give up on counting the bad cells, and just make sure that we flag all
#rows that contain at least one of them
def has_transcriptionisms(row):
  if row.name in bad: return
  if row.str.contains('[\[\]]').any():
    bad[row.name] = 1
  #TODO: Should use the info in workflows.yaml to drop the number columns, rather than listing them all by name:
  if row.drop(['admission number', 'age', 'years at sea', 'number of days victualled']).str.contains('^0+$').any():
    bad[row.name] += 1


#Recieve:
#'candidates': A dict of category names and votes for that category
#'threshold': A threshold for winning
#'subject_task': Key for logging
#'workflow_name': Value for logging
#If there is a winner, return the winning category and its number of votes as a single-entry dict
#If the winner was not unanimous, also log this in 'autoresolved'
#If there is not a winner, return the input dict
#The way this is written, the reference we return will be the same as the first parameter iff there is no winner.
#We can also detect success by checking that the length of the returned dict is 1 (if the original input contained only 1 key, then that key is by definition the winner; otherwise, the dict of the winner is a single-entry dict)
def category_resolver(candidates, threshold, subject_task, workflow_name):
  total_votes = sum(candidates.values())
  for selection, votes in candidates.items():
    if votes == total_votes:
      return {selection: votes}
    if votes / total_votes >= threshold: #data has been autoresolved
      if subject_task in autoresolved: autoresolved[subject_task][workflow_name] = None
      else: autoresolved[subject_task] = { workflow_name: None }
      return {selection: votes}
  return candidates


def years_at_sea_resolver(candidates, row, data, datacol):
  #Reconsitute the original transcriptions
  originals = [''.join(x) for x in zip(*candidates)]
  navies = []
  merchants = []
  for numbers in [x.split(';') for x in originals]:
    if len(numbers) != 2:
      flow_report('Wrong number of "years at sea" entries (or bad separator)', row.name, originals)
      bad[row.name] += 1
      return originals
    try:
      (navy, merchant) = [float(x) for x in numbers]
    except ValueError:
      flow_report('Non-float argument in "years at sea"', row.name, originals)
      bad[row.name] += 1
      return originals
    navies.append(navy)
    merchants.append(merchant)
  navy_results     = category_resolver(collections.Counter(navies),    args.dropdown_threshold, row.name, data['name'])
  merchant_results = category_resolver(collections.Counter(merchants), args.dropdown_threshold, row.name, data['name'])
  if len(navy_results) == 1 and len(merchant_results) == 1:
    if row.name in autoresolved and data['name'] in autoresolved[row.name]: flow_report('Autoresolved', row.name, originals)
    else: flow_report('Unanimous', row.name, originals)
    navy_result = "%02g" % next(iter(navy_results))
    merchant_result = "%02g" % next(iter(merchant_results))
    navy_result=re.sub('^\d\.', '0\g<0>', navy_result)
    merchant_result=re.sub('^\d\.', '0\g<0>', merchant_result)
    return f'{navy_result}; {merchant_result}'
  else:
    #Because we resolve the two sides independently, we might both autoresolve and fail for the field.
    #This is a bit confusing, so if we failed for either side, remove the autoresolved.
    if row.name in autoresolved and data['name'] in autoresolved[row.name]: del autoresolved[row.name][data['name']]

    if len(navy_results) != 1 and len(merchant_results) != 1: flow_report('Unresolvable (both sides)', row.name, originals)
    elif len(navy_results) != 1: flow_report('Unresolvable (navy side)', row.name, originals)
    else: flow_report('Unresolvable (merchant side)', row.name, originals)
    bad[row.name] += 1
    return originals

def string_resolver(row, data, datacol):
  #Start with a special case -- if port sailed out of is set and we are volume 1, autoresolve to blank
  if data['name'] == 'port sailed out of' and get_subject_reference(row.name[0])[0] == 1:
    if row['data.number_views'] > 0:
      if row.name in autoresolved:
        flow_report('port sailed out of in volume 1 (later)', row.name, row['data.aligned_text'])
        autoresolved[row.name][data['name']] = None
        return ''
      else:
        flow_report('port sailed out of in volume 1 (first)', row.name, row['data.aligned_text'])
        autoresolved[row.name] = { data['name']: None }
        return ''
  if row['data.consensus_score'] / row['data.number_views'] < args.text_threshold:
    flow_report('Did not pass threshold', row.name, row['data.aligned_text'])
    bad[row.name] += 1
    return row['data.aligned_text']
  else:
    if row['data.consensus_score'] != row['data.number_views']: #data has been autoresolved
      if row.name in autoresolved:
        flow_report('Later autoresolve', row.name, row['data.aligned_text'])
        autoresolved[row.name][data['name']] = None
      else:
        flow_report('First autoresolve', row.name, row['data.aligned_text'])
        autoresolved[row.name] = { data['name']: None }
    else: flow_report('Unambiguous', row.name, row['data.aligned_text'])
    return row[datacol]

def date_resolver(row, data):
    candidates = ast.literal_eval(row['data.aligned_text'])

    if(len(candidates) != 1): #Not a conventional case, resolve manually
      flow_report('Surprising input', row.name, row['data.aligned_text'])
      bad[row.name] += 1
      return row['data.aligned_text']

    candidates = candidates[0]
    #https://stackoverflow.com/a/18029112 has a trick for reading arbitrary date formats while rejecting ambiguous cases
    #We just need to use the documented format, but we can be a bit forgiving
    try:
      candidates = [dateutil.parser.parse(d, dayfirst = True) for d in candidates] #yearfirst defaults to False
    except (TypeError, ValueError): #Something is wrong, resolve manually
      flow_report('Unparseable', row.name, row['data.aligned_text'])
      bad[row.name] += 1
      return row['data.aligned_text']
    candidates = category_resolver(collections.Counter(candidates), args.dropdown_threshold, row.name, data['name'])
    if len(candidates) == 1:
      if row.name in autoresolved and data['name'] in autoresolved[row.name]: flow_report('Autoresolved', row.name, row['data.aligned_text'])
      else: flow_report('Unanimous', row.name, row['data.aligned_text'])
      date = next(iter(candidates))
      return date.strftime('%d-%m-%Y')
    else:
      flow_report('Unresolvable', row.name, row['data.aligned_text'])
      bad[row.name] += 1
      return row['data.aligned_text']

def number_resolver(row, data, datacol):
  candidates = ast.literal_eval(row['data.aligned_text'])

  #years at sea needs some special handling
  #it contains two floating point numbers, separated by a semicolon
  #we can improve autoresolution by normalising these and comparing them individually
  if data['name'] == 'years at sea':
    return years_at_sea_resolver(candidates, row, data, datacol)

  if(len(candidates) != 1): #Not a conventional case, resolve manually
    flow_report('Surprising input', row.name, row['data.aligned_text'])
    bad[row.name] += 1
    return row['data.aligned_text']

  candidates = candidates[0]

  #If there are any non-numerals in the input, just return it to resolve manually
  try:
    candidates = [float(x) for x in candidates]
  except ValueError:
    flow_report('Non-float input', row.name, row['data.aligned_text'])
    bad[row.name] += 1
    return row['data.aligned_text']
  if not all([x.is_integer() for x in candidates]):
    flow_report('Non-integer input', row.name, row['data.aligned_text'])
    bad[row.name] += 1
    return row['data.aligned_text']
  candidates = [int(x) for x in candidates]
  candidates = category_resolver(collections.Counter(candidates), args.dropdown_threshold, row.name, data['name'])
  if len(candidates) == 1:
    if row.name in autoresolved and data['name'] in autoresolved[row.name]: flow_report('Autoresolved', row.name, row['data.aligned_text'])
    else: flow_report('Unanimous', row.name, row['data.aligned_text'])
    return next(iter(candidates)) #First key, efficiently (see https://www.geeksforgeeks.org/python-get-the-first-key-in-dictionary/)
  else:
    flow_report('Unresolvable', row.name, row['data.aligned_text'])
    bad[row.name] += 1
    return row['data.aligned_text']

#Process data for output
#Strings use Levenshtein distance approach, IIRC
#Take a different approach for non-string data
def text_resolver(row, **kwargs):
  global args, bad, autoresolved #Just being explicit that these are global
  data = kwargs['data']
  datacol = kwargs['datacol']

  if pd.isnull(row['data.consensus_score']) or pd.isnull(row['data.number_views']):
    assert args.unfinished #This condition should only come up if -u is set
    return ''

  if data['nptype'] == str: return string_resolver(row, data, datacol)
  elif data['nptype'] == pd.Int64Dtype: return number_resolver(row, data, datacol)
  elif data['nptype'] == datetime.date: return date_resolver(row, data)
  else: raise Exception()

def main():
  global args, bad, autoresolved #Be explicit that these are global

  KEYS = ['subject_id', 'task']
  RETIREMENT_COUNT = 3 #I believe that this is the same for all workflows at all times. Can be parameterised in workflows.yaml if need be.

  with open('workflow.yaml') as f:
    workflow = yaml.load(f, Loader = yaml.Loader)

  #Read in the reduced data.
  columns = []
  TEXT_T = workflow['definitions']['TEXT_T']
  DROP_T = workflow['definitions']['DROP_T']

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
                       dtype     = {datacol: str},
                       skip_blank_lines = False)
    except:
      print(f'Error while reading {reduced_file}')
      raise

    #Handle conflicts
    if(data['ztype'] == TEXT_T):
      if not args.unfinished:
        #Drop all classifications that are based on an insufficient number of views
        df.drop(df[df['data.number_views'] < RETIREMENT_COUNT].index, inplace = True)
        df.dropna(subset = ['data.number_views'], inplace = True)

      #Report on rows with different counts
      if args.verbose >= 1:
        overcount = df[df['data.number_views'] > RETIREMENT_COUNT]
        print(f'  Completed rows: {len(df.index)} (of which {len(overcount.index)} overcounted)')
        if args.verbose >= 3 and not overcount.empty: print(overcount)
        undercount = df[df['data.number_views'] < RETIREMENT_COUNT]
        print(f'  Undercounted rows: {len(undercount.index)}')
        if args.verbose >= 3 and not undercount.empty: print(undercount)

      df[datacol] = df.apply(text_resolver, axis = 'columns', data = data, datacol = datacol)
      #TODO: For these kinds of strings, may well be better to treat them like dropdowns and just take two thirds identical as permitting auto-resolve
    elif(data['ztype'] == DROP_T):
      #Drop all classifications that are based on an insufficient number of views
      if not args.unfinished:
        def votecounter(votes):
          selections = ast.literal_eval(votes)
          if len(selections) != 1: raise Exception()
          return(sum(selections[0].values()))
        df = df[df[datacol].apply(votecounter) >= RETIREMENT_COUNT]

      #Report on rows with different counts
      if args.verbose >= 1:
        overcount = df[df[datacol].apply(votecounter) > RETIREMENT_COUNT]
        print(f'  Completed rows: {len(df.index)} (of which {len(overcount.index)} overcounted)')
        if args.verbose >= 3 and not overcount.empty: print(overcount)
        undercount = df[df[datacol].apply(votecounter) < RETIREMENT_COUNT]
        print(f'  Undercounted rows: {len(undercount.index)}')
        if args.verbose >= 3 and not undercount.empty: print(undercount)

      #Process classifications for output
      def drop_resolver(row):
        #row is a single-element array, containing one dictionary
        selections = ast.literal_eval(row[datacol])
        if len(selections) != 1: raise Exception()
        result = category_resolver(selections[0], args.dropdown_threshold, row.name, data['name'])
        if len(result) == 1:
          if row.name in autoresolved and data['name'] in autoresolved[row.name]:
            flow_report('Autoresolved', row.name, row['data.value'])
          else: flow_report('Unanimous', row.name, row['data.value'])
        else:
          flow_report('Unresolvable', row.name, row['data.value'])
          bad[row.name] += 1
        return str([result])
      df[datacol] = df.apply(drop_resolver, axis = 'columns')
    else: raise Exception()

    #Tidy up columns
    df.drop(conflict_keys, axis = 'columns', inplace = True) #Drop columns that we just brought in for conflict handling
    df.rename(columns={datacol: data['name']}, inplace = True) #Rename the data column to something meaninful

    #Convert dropdowns to their values
    if(data['ztype'] == DROP_T):
      labelfile = f'{args.dir}/Task_labels_workflow_{wid}_V{data["major"]}.{data["minor"]}.yaml'
      with open(labelfile) as f:
        labels = yaml.full_load(f)
      def decode_dropdown(row):
          selections = ast.literal_eval(row[data['name']])
          if len(selections) != 1: raise Exception()
          selections = selections[0]
          result = {}
          for selection, votes in selections.items():
            if selection == 'None': result[None] = votes
            else:
              label = list(labels[f'T{row.name[1]}.selects.0.options.*.{selection}.label'].values())[0]
              label = label[label.find('=') + 1:].strip()
              result[label] = votes
          if len(result) == 1:
            return list(result.keys())[0]
          else: return str(result)
      df[data['name']] = df.apply(decode_dropdown, axis = 'columns')

    columns.append(df)

  if args.verbose >= 0: print('*** Generating output')
  #Combine the separate workflows into a single dataframe
  #Assumption: Task numbers always refer to the same row in each workflow
  #            If this assumption does not hold, we can perform a mapping
  #            on the dataframes at the point that we read them in, above.
  #Quick test shows that this assumption does hold for now.
  first = columns.pop(0)
  joined = first.join(columns, how='outer')

  #Search for transcription problmes
  joined.apply(has_transcriptionisms, axis = 'columns')

  #Tag or remove the rows with badness
  joined.insert(0, 'Problems', '')
  joined['Problems'] = joined.apply(has_blanks, axis = 'columns')
  if not args.blanks:
    incomplete_subjects = list(joined[joined.Problems != ''].index.get_level_values('subject_id').unique())
    removed = joined.query(f'subject_id in @incomplete_subjects')
    for key in removed.index.to_numpy():
      bad.pop(key, None)
      autoresolved.pop(key, None)
    joined = joined.query(f'subject_id not in @incomplete_subjects')

  #Tag unresolved unresolved fields
  #TODO This part does not feel like the Pandas way
  for b in bad.keys():
    problems = joined.at[b, 'Problems']
    if len(problems):
      joined.at[b, 'Problems'] = f'{problems} & at least {bad[b]} unresolved fields'
    else:
      joined.at[b, 'Problems'] = f'At least {bad[b]} unresolved fields'

  #Translate subjects ids into original filenames
  #Assumption: the metadata is invariant across all of the entries for each subject_id
  joined.insert(0, 'subject', '')
  joined.insert(1, 'volume', '')
  joined.insert(2, 'page', '')
  for sid in joined.index.get_level_values('subject_id').unique():
    #This is what I should do here. But enabling it produces a peculiar warning,
    #so sticking with duplicate vol/page calculation code for now.
    #(vol, page) = get_subject_reference(sid)
    metadata = subjects.query(f'subject_id == {sid}').iloc[0]['metadata']
    fnam = json.loads(metadata)['Filename']
    match = re.fullmatch('.*_(\d+)-(\d+)(?: \d)?\.jpg', fnam)
    if match:
      (vol, page) = [int(x) for x in match.groups()]
      if   vol == 1: page -= 21
      elif vol == 2: page -= 28
      elif vol == 6: raise Exception('Surprisingly met volume 6')
      else: page -= 3
    else: raise Exception(f'"{fnam}" does not match regular expression')

    #Figure out URL of page image
    location = subjects.query(f'subject_id == {sid}').iloc[0]['locations']
    location = json.loads(location)
    assert len(location) == 1
    location = location.values()
    location = next(iter(location))
    assert isinstance(location, str)

    joined.loc[[sid], 'subject'] = f'=HYPERLINK("{location}"; "{sid}")'
    joined.loc[[sid], 'volume'] = vol
    joined.loc[[sid], 'page']   = page


  #Record where there was autoresolution
  joined.insert(len(joined.columns), 'Autoresolved', '')
  #TODO: Again, doesn't feel like Pandas
  for index, value in autoresolved.items():
    joined.at[index, 'Autoresolved'] = '; '.join(value.keys())


  #This feels ridiculous, but works in conjunction with maxcolwidth.sh to check for columns too wide for Excel
  joined.to_csv(path_or_buf = f'output/lenchecker.csv', index = False, sep = '@')

  #Dump output
  if not args.no_stamp:
    remote = subprocess.run(['git', 'remote', '-v'], capture_output = True, check = True).stdout
    joined['Repo'] = [remote] * len(joined.index)
    commit = subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output = True, check = True).stdout
    joined['Commit'] =[commit] * len(joined.index)
    joined['Args'] = [' '.join(sys.argv)] * len(joined.index)
  joined.to_csv(path_or_buf = f'output/{args.output}', index = False)

main()
