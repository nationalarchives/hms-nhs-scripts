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
import os
import time
import csv
from collections import defaultdict, Counter

#For debugging
#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)
#pd.set_option('display.expand_frame_repr', None)

bad = defaultdict(int) #Keys of bad are indices of rows in the final dataframe
autoresolved = {} #Keys of autoresolved are indices of rows in the final dataframe
subjects = None #Initialised after arg parsing
KEYS = ['subject_id', 'task'] #Columns to use in all cases

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
parser.add_argument('--timing',
                    action = 'store_true',
                    help = 'Give timing information for phases in the program')
parser.add_argument('--output_dir',
                    default = 'output',
                    help = 'Set output dir (must already exists, defaults to "output")')
parser.add_argument('--output', '-o',
                    default = 'joined.csv',
                    help = 'Set name of output file (use --output_dir to change the output directory)')
parser.add_argument('--exports', '-e',
                    default = 'exports',
                    help = 'Directory of exports from the Zooniverse project')
parser.add_argument('--reduced', '-r',
                    default = 'aggregation',
                    dest = 'dir',
                    help = 'Directory containing data reduced by Panoptes scripts.')
#parser.add_argument('--blanks', '-b',
#                    action = 'store_true',
#                    help = 'Include pages with missing values')
parser.add_argument('--uncertainty',
                    action = 'store_true',
                    help = 'Treat certain patterns as indicating presence of an uncertain transcription and requiring manual review')
parser.add_argument('--no_stamp', '-S',
                    action = 'store_true',
                    help = 'Do not stamp the output with information about the script used to generate it')
parser.add_argument('--flow_report', '-f',
                    action = 'store_true',
                    help = ('Show information about paths taken through program.\n'
                            'Used in conjunction with coverage.sh to make sure that test inputs are testing all paths.'
                           )
                   )
parser.add_argument('--dump_interims', action = 'store_true')
parser.add_argument('--row_factor',
                    type = int,
                    help = 'Percentage of total rows to read. Repeatable across runs, for faster testing cycles.'
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

def track(msg, **kwargs):
  if args.timing:
    now = time.time()
    if track.last == 0: track.last = now
    print(f'[{int(now - track.last):>5n}] {msg}')
    track.last = now
  elif 'regardless' in kwargs and kwargs['regardless']:
    print(msg)
track.last = 0

#Candidates is a list of strings
#Each string shoud correspond to a single text box from the workflows
def uncertainty(candidates):
  if not args.uncertainty: return False

  assert isinstance(candidates, list), candidates
  patterns = [
    r'\[.*\]',
    r'\(.*[\.?].*\)',
    r'\?+',
    r'^[^\d]*\.[^ $]'
  ]
  for candidate in candidates:
    assert isinstance(candidate, str)
    for pattern in patterns:
      if re.search(pattern, candidate):
        if args.verbose >= 1:
          print(f'U: {pattern}: {candidate} ({"::".join(candidates)})')
        return True
  return False


def unaligned(aligned_text):
  return list(map(lambda x: ' '.join(x).strip(), zip(*aligned_text)))


def pretty_candidates(candidates, best_guess = None):
  container = candidates
  if isinstance(candidates, str):
    container = ast.literal_eval(candidates)
    assert isinstance(container, list)

  if isinstance(container, list):
    container = unaligned(container)
  elif isinstance(container, dict):
    container = [f'{k}: {v}' for k, v in candidates.items()]
  else:
    raise Exception(f"Unexpected data type {type(container)} while prettifying candidates")

  retval = [ best_guess, '----------' ] if best_guess else []
  for k, v in Counter(container).items():
    if re.search(r'@\d+$', k) or k == '----------':
      raise Exception(f'Reserved pattern in input: {k}')
    if v == 1: retval.append(k)
    else: retval.append(f'{k} @{v}')
  return '\n'.join(retval)


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


def count_text_views(wid):
  #The text reducer does not count blank entries as viewed.
  #This can mess up our calculation for including something in the output.
  #So we count rows in the extractor instead.
  #(It would make sense to use the retirement count in ...-subjects.csv, but that doesn't seem to have all of the information (perhaps because it is a snapshot of the subjects now, so any removed subjects become unavailable?)
  #(Alternative implementation: we could identify finished subjects by looking at 'retired' in the subject metadata logged in the exports file, and then perhaps the 'already_seen' flag in there can be used to catch repeat classifications -- depending upon exactly what that flag means. That is likely to be more simple and more efficient.)
  #This is only needed for TEXT_T, as the dropdown reducer does give us a count of all votes, even where the volunteer did not vote (logged as a vote for None)
  #We also take the opportunity to log cases where a logged-in user has classified the same subject more than once (we could try to do this for anonymous users as well, but I'm not sure about the IP hashes)
  extractor_df = pd.read_csv(args.dir + '/' + f'text_extractor_{wid}.csv',
                          index_col = KEYS, #i.e. subject_id and task, so that we are indexed the same way as the data
                          usecols = KEYS + ['classification_id', 'user_id'], #classification_id MUST be present, so we can use to count the total. user_id needed for counting logged-in users.
                          converters = {'task': lambda x: x[1:]} #drop the T, so that index matches
                         )

  #Sanity check -- the uncleaned (but tranche-processed) extraction file should contain the same classification ids
  extractor_new_series = pd.read_csv(args.dir + '/' + f'text_extractor_{wid}.csv.new', index_col = KEYS, usecols = KEYS + ['classification_id'], converters = {'task': lambda x: x[1:]})['classification_id']
  assert len(extractor_new_series) == len(extractor_df['classification_id'])
  extractor_new_series_comparison = extractor_new_series.reset_index(drop = True).eq(extractor_df['classification_id'].reset_index(drop = True))
  assert extractor_new_series_comparison.all(), extractor_new_series_comparison

  #First work out whether logged in users have performed repeat classifications on any subjects, so that we can log that this has happened
  nonunique_views = None
  id_group = extractor_df['user_id'].groupby(KEYS) #for user_id-aware counting
  repeat_classifications = id_group.count() - id_group.nunique() #entirely ignoring nans (these functions ignore them), as we can't necessarily rely on the IP address so we want to set aside anonymous users here
  repeat_classifications = repeat_classifications[repeat_classifications != 0]
  if len(repeat_classifications) != 0:
    subj_group = repeat_classifications.groupby('subject_id')
    assert subj_group.nunique().eq(1).all() #Each task within a given subject should have the same number of classifications
    nonunique_views = subj_group.first() #This is storing the raw count minus the unique count for every repeat-classified subject in the current field

  raw_count = extractor_df['classification_id'].groupby(KEYS).count() #counts all rows
  return (raw_count, nonunique_views)


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

  if uncertainty(originals): return pretty_candidates(candidates, row['data.consensus_text'])

  for numbers in [x.split(';') for x in originals]:
    if len(numbers) != 2:
      flow_report('Wrong number of "years at sea" entries (or bad separator)', row.name, originals)
      bad[row.name] += 1
      return pretty_candidates(candidates, row['data.consensus_text'])
    try:
      (navy, merchant) = [float(x) for x in numbers]
    except ValueError:
      flow_report('Non-float argument in "years at sea"', row.name, originals)
      bad[row.name] += 1
      return pretty_candidates(candidates, row['data.consensus_text'])
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
    return pretty_candidates(candidates, row['data.consensus_text'])

def string_resolver(row, data, datacol):
  if uncertainty(unaligned(ast.literal_eval(row['data.aligned_text']))) or row['data.consensus_score'] / row['data.number_views'] < args.text_threshold:
    flow_report('Did not pass threshold', row.name, row['data.aligned_text'])
    bad[row.name] += 1
    return pretty_candidates(row['data.aligned_text'], row['data.consensus_text'])
  else:
    if row['data.consensus_score'] != row['data.number_views']: #data has been autoresolved (ignoring any empty strings)
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
      return pretty_candidates(row['data.aligned_text'], row['data.consensus_text'])

    candidates = candidates[0]

    if uncertainty(candidates): return pretty_candidates(row['data.aligned_text'], row['data.consensus_text'])

    #https://stackoverflow.com/a/18029112 has a trick for reading arbitrary date formats while rejecting ambiguous cases
    #We just need to use the documented format, but we can be a bit forgiving
    try:
      candidates = [dateutil.parser.parse(d, dayfirst = True) for d in candidates] #yearfirst defaults to False
    except (TypeError, ValueError): #Something is wrong, resolve manually
      flow_report('Unparseable', row.name, row['data.aligned_text'])
      bad[row.name] += 1
      return pretty_candidates(row['data.aligned_text'], row['data.consensus_text'])
    candidates = category_resolver(collections.Counter(candidates), args.dropdown_threshold, row.name, data['name'])
    if len(candidates) == 1:
      if row.name in autoresolved and data['name'] in autoresolved[row.name]: flow_report('Autoresolved', row.name, row['data.aligned_text'])
      else: flow_report('Unanimous', row.name, row['data.aligned_text'])
      date = next(iter(candidates))
      return date.strftime('%d-%m-%Y')
    else:
      flow_report('Unresolvable', row.name, row['data.aligned_text'])
      bad[row.name] += 1
      return pretty_candidates(row['data.aligned_text'], row['data.consensus_text'])

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
    return pretty_candidates(row['data.aligned_text'], row['data.consensus_text'])

  candidates = candidates[0]

  if uncertainty(candidates): return pretty_candidates(row['data.aligned_text'], row['data.consensus_text'])

  #If there are any non-numerals in the input, just return it to resolve manually
  try:
    candidates = [float(x) for x in candidates]
  except ValueError:
    flow_report('Non-float input', row.name, row['data.aligned_text'])
    bad[row.name] += 1
    return pretty_candidates(row['data.aligned_text'], row['data.consensus_text'])
  if not all([x.is_integer() for x in candidates]):
    flow_report('Non-integer input', row.name, row['data.aligned_text'])
    bad[row.name] += 1
    return pretty_candidates(row['data.aligned_text'], row['data.consensus_text'])
  candidates = [int(x) for x in candidates]
  candidates = category_resolver(collections.Counter(candidates), args.dropdown_threshold, row.name, data['name'])
  if len(candidates) == 1:
    if row.name in autoresolved and data['name'] in autoresolved[row.name]: flow_report('Autoresolved', row.name, row['data.aligned_text'])
    else: flow_report('Unanimous', row.name, row['data.aligned_text'])
    return next(iter(candidates)) #First key, efficiently (see https://www.geeksforgeeks.org/python-get-the-first-key-in-dictionary/)
  else:
    flow_report('Unresolvable', row.name, row['data.aligned_text'])
    bad[row.name] += 1
    return pretty_candidates(row['data.aligned_text'], row['data.consensus_text'])

#Process data for output
#Strings use Levenshtein distance approach, IIRC
#Take a different approach for non-string data
def text_resolver(row, **kwargs):
  global args, bad, autoresolved #Just being explicit that these are global
  data = kwargs['data']
  datacol = kwargs['datacol']

  #This means either that no-one has classified it, or that all classifications were empty strings
  #Either way, return a blank
  if pd.isnull(row['data.number_views']):
    if not pd.isnull(row['data.consensus_score']): raise Exception('Broken assumption')
    return ''

  if data['nptype'] == str: return string_resolver(row, data, datacol)
  elif data['nptype'] == pd.Int64Dtype: return number_resolver(row, data, datacol)
  elif data['nptype'] == datetime.date: return date_resolver(row, data)
  else: raise Exception()

def main():
  global args, bad, autoresolved #Be explicit that these are global

  RETIREMENT_COUNT = 3 #I believe that this is the same for all workflows at all times. Can be parameterised in workflows.yaml if need be.

  try: os.mkdir(args.output_dir)
  except FileExistsError:
    print(f"Output directory '{args.output_dir}' already exists.\nPlease delete it before running this script, or use --output_dir to output to a different directory.", file = sys.stderr)
    sys.exit(1)

  with open('workflow.yaml') as f:
    workflow = yaml.load(f, Loader = yaml.Loader)

  #Read in the reduced data.
  columns = []
  TEXT_T = workflow['definitions']['TEXT_T']
  DROP_T = workflow['definitions']['DROP_T']

  #Declare array to store record of what we have processed, and another to store records with repeat views by a given user
  workflow_columns = []
  views = []
  nonunique_views = []

  track('Processing workflows')
  for wid, data in workflow[args.workflows].items():
    workflow_columns.append(data['name'])
    datacol = data['ztype']['name']
    conflict_keys = []
    reduced_file = f'{args.dir}/{data["ztype"]["type"]}_reducer_{wid}.csv'
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

    #count views
    if data['ztype'] == TEXT_T:
      current_views, nonunique_counts = count_text_views(wid)
      if nonunique_counts is not None:
        nonunique_views.append(nonunique_counts.rename(data['name']))
    elif data['ztype'] == DROP_T:
      #Blank entries for dropdowns appear to come out as type 'None' with n votes --
      #so they are counted and we do not need to do the work in count_text_views to count everything
      #TODO: Add a unique users check to this one, similar to the one in count_text_views
      def votecounter(votes):
        selections = ast.literal_eval(votes)
        if len(selections) != 1: raise Exception()
        return(sum(selections[0].values()))
      current_views = df[datacol].apply(votecounter)
    current_views = current_views.rename(data['name'])
    views.append(current_views)

    #drop all classifications based on insufficient number of views
    if not args.unfinished:
      #Drop all classifications that are based on an insufficient number of views
      df.drop(current_views[current_views < RETIREMENT_COUNT].index, inplace = True)

    #User can shrink the number of rows to be read, for faster runs.
    #This will be used for run-to-run output comparison, so must be repeatable.
    #Taking every nth row might give a better overall sample of the data than just taking head or tail.
    #But if people tend to classify the same records at around the same time, every nth row might result in few complete classifications.
    #Note that this does not affect the views count for TEXT_T, which is always based on the entire text_exporter file. This means the results are spurious in that rows are admitted even if they no longer have enough views to be considered complete, but the point of this feature is just to be able to compare for unexpected output changes.
    #Must do this *after* dropping rows, because of the separation of this from the views file
    if args.row_factor:
      df = df.iloc[::int(100 / args.row_factor)]

    #Report on rows with different counts
    if args.verbose >= 1:
      overcount = df.loc[current_views[current_views > RETIREMENT_COUNT].index]
      print(f'  Completed rows: {len(df.index)} (of which {len(overcount.index)} overcounted)')
      if args.verbose >= 3 and not overcount.empty: print(overcount)
      undercount = df.loc[current_views[current_views < RETIREMENT_COUNT].index]
      print(f'  Undercounted rows: {len(undercount.index)}')
      if args.verbose >= 3 and not undercount.empty: print(undercount)

    #Handle conflicts
    if(data['ztype'] == TEXT_T):
      df[datacol] = df.apply(text_resolver, axis = 'columns', data = data, datacol = datacol)
      #TODO: For these kinds of strings, may well be better to treat them like dropdowns and just take two thirds identical as permitting auto-resolve
    elif(data['ztype'] == DROP_T):
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

    df = df[datacol].rename(data['name']).to_frame() #Keep just the data column, renaming it to something meaningful and keeping it a DF rather than a Series

    #Convert dropdowns to their values
    if(data['ztype'] == DROP_T):
      if type(data['version']) is list: #Assume that labels are the same in all versions, just use the first file.
                                        #TODO: Add some code to confirm that the labels are the same in all versions.
                                        #      At time of writing, extract.py will ensure this for phase 1 exports from HMS NHS.
        labelfile = f'{args.dir}/Task_labels_workflow_{wid}_V{data["version"][0]}.yaml'
      else:                             labelfile = f'{args.dir}/Task_labels_workflow_{wid}_V{data["version"]}.yaml'
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
              label_list = list(labels[f'T{row.name[1]}.selects.0.options.*.{selection}.label'].values())
              if len(label_list) != 1: raise Exception('\n'.join(['Assumption that label_list always contains 1 element is broken'] + label_list))
              label = label_list[0]
              label = label[label.find('=') + 1:].strip()
              result[label] = votes
          if len(result) == 1:
            return list(result.keys())[0]
          else: return pretty_candidates(result)
      df[data['name']] = df.apply(decode_dropdown, axis = 'columns')

    columns.append(df)
    track(f'* {reduced_file} done', regardless = True)

  track('Generating output', regardless = True)
  if args.dump_interims:
    for i, c in enumerate(columns): c.to_csv(f'{args.output_dir}/col_{i}.csv')
  #Combine the separate workflows into a single dataframe
  #Assumption: Task numbers always refer to the same row in each workflow
  #            If this assumption does not hold, we can perform a mapping
  #            on the dataframes at the point that we read them in, above.
  #Quick test shows that this assumption does hold for now.
  first = columns.pop(0)
  joined = first.join(columns, how='outer')
  track('* Data joined')
  if args.dump_interims: joined.to_csv(f'{args.output_dir}/initial_joined.csv')

  if args.dump_interims:
    for i, v in enumerate(views): v.to_csv(f'{args.output_dir}/views_col_{i}.csv')
  first = views.pop(0).to_frame()
  joined_views = first.join(views, how='outer')
  track('* Views joined')
  if args.dump_interims: joined_views.to_csv(f'{args.output_dir}/initial_joined_views.csv')

  #This just gives us some record of whether the same user repeat-classified.
  #Potentially important data, but not a requested feature.
  #At time of writing, only implemented for text workflows.
  #Might not be tranche-safe.
  #In principle could be established later by running for the full dataset... though
  #identifying the affected pages becomes difficult if the relevant data is missing from
  #the subjects file.
  if len(nonunique_views):
    first = nonunique_views.pop(0).to_frame()
    first.join(nonunique_views, how='outer').to_csv(f'{args.output_dir}/nonunique.csv')

  #Search for transcription problmes
  joined.apply(has_transcriptionisms, axis = 'columns')
  track('* Transcriptionisms identified')
  if args.dump_interims: joined.to_csv(f'{args.output_dir}/joined_has_transcriptionisms.csv')

  #Translate subjects ids into original filenames
  #Assumption: the metadata is invariant across all of the entries for each subject_id
  subj_info_cols = {
    'subject': {},
    'volume': {},
    'page': {},
    'raw_subject': {},
  }
  joined.insert(0, 'subject', '')
  joined.insert(1, 'volume', '')
  joined.insert(2, 'page', '')
  joined.insert(len(joined.columns), 'raw_subject', '')
  for sid in joined.index.get_level_values('subject_id').unique():
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

    subj_info_cols['subject'][sid] = f'=HYPERLINK("{location}"; "{sid}")'
    subj_info_cols['volume'][sid] = vol
    subj_info_cols['page'][sid] = page
    subj_info_cols['raw_subject'][sid] = sid
  for level_name, value in subj_info_cols.items():
    for sid, subj_info in value.items():
      joined.loc[[sid], level_name] = subj_info

  #Identify volume 1's subject ids, for special handling around 'port sailed out of'
  vol_1_subj_ids = list(set(joined[joined['volume'] == 1].index.get_level_values(0)))

  track('* Subjects identified')
  if args.dump_interims: joined.to_csv(f'{args.output_dir}/joined_subjects_identified.csv')

  #Handle the 'port sailed out of' special case
  bad_ports = joined.loc[vol_1_subj_ids]['port sailed out of'][joined.loc[vol_1_subj_ids]['port sailed out of'].notnull()].index
  for bad_port in bad_ports:
    if bad_port in autoresolved:
      flow_report('port sailed out of in volume 1 (later)', bad_port, joined.loc[bad_port])
      autoresolved[bad_port]['port sailed out of'] = None
    else:
      flow_report('port sailed out of in volume 1 (first)', bad_port, joined.loc[bad_port])
      autoresolved[bad_port] = { 'port sailed out of': None }
  joined.loc[bad_ports,['port sailed out of']] = ''
  track('* Years at sea fixed up')

  joined_views['complete'] = joined_views[workflow_columns].ge(RETIREMENT_COUNT).all(axis = 1)
  joined_views.loc[vol_1_subj_ids,['complete']] = joined_views.loc[vol_1_subj_ids][workflow_columns].drop('port sailed out of', axis = 1).ge(RETIREMENT_COUNT).all(axis = 1)
  track('* Complete views identified')
  if args.dump_interims: joined_views.to_csv(f'{args.output_dir}/joined_views_complete.csv')

  #Tag or remove the rows with badness
  joined.insert(3, 'Problems', '')
  joined['Problems'] = joined[workflow_columns].isnull().values.any(axis = 1)
  joined.loc[vol_1_subj_ids,['Problems']] = joined.loc[vol_1_subj_ids][workflow_columns].drop('port sailed out of', axis = 1).isnull().values.any(axis = 1)
  joined['Problems'] = joined['Problems'].map({True: 'Blank(s)', False: ''})
  if args.dump_interims: joined.to_csv(f'{args.output_dir}/joined_problems.csv')

  #At the moment I get to a complete page by looking at subject_id level.
  #If anything under that subject_id is blank, I discard that subject id.
  #However, sometimes fields are actually input as blank, so this leads me to
  #discard pages that are actually complete.
  #So, instead of this, use the information in joined_views to identify complete pages.
  #Output them whether or not they have blanks, tagging the blanks in Problems.
  #if not args.blanks:
  #  incomplete_subjects = list(joined[joined.Problems != ''].index.get_level_values('subject_id').unique())
  #  removed = joined.query(f'subject_id in @incomplete_subjects')
 #   for key in removed.index.to_numpy():
 #     bad.pop(key, None)
 #     autoresolved.pop(key, None)
 #   joined = joined.query(f'subject_id not in @incomplete_subjects')
 #   joined_views = joined_views.query(f'subject_id not in @incomplete_subjects')
 # track('* Badness identified')

  if not args.unfinished:
    incomplete_subjects = []
    complete_subjects = []
    for sid in joined_views.index.get_level_values('subject_id').unique():
      if joined_views.loc[[sid]]['complete'].all():
        complete_subjects.append(sid)
      else:
        incomplete_subjects.append(sid)
    removed = joined.query(f'subject_id in @incomplete_subjects')
    removed.to_csv(f'{args.output_dir}/removed.csv')
    for key in removed.index.to_numpy():
      bad.pop(key, None)
      autoresolved.pop(key, None)
    joined = joined.query(f'subject_id in @complete_subjects')
    joined_views = joined_views.query(f'subject_id in @complete_subjects')
    track('* Badness identified')
    if args.dump_interims:
      joined.to_csv(f'{args.output_dir}/joined_unfinished.csv')
      joined_views.to_csv(f'{args.output_dir}/joined_views_unfinished.csv')


  #Tag unresolved unresolved fields
  #TODO This part does not feel like the Pandas way
  for b in bad.keys():
    problems = joined.at[b, 'Problems']
    if len(problems):
      joined.at[b, 'Problems'] = f'{problems} & at least {bad[b]} unresolved fields'
    else:
      joined.at[b, 'Problems'] = f'At least {bad[b]} unresolved fields'
  track('* Unresolved identified')
  if args.dump_interims: joined.to_csv(f'{args.output_dir}/joined_unresolved.csv')


  #Record where there was autoresolution
  joined.insert(len(joined.columns), 'Autoresolved', '')
  #TODO: Again, doesn't feel like Pandas
  for index, value in autoresolved.items():
    joined.at[index, 'Autoresolved'] = '; '.join(filter(lambda x: x in value.keys(), workflow_columns))
  track('* Autos identified')
  if args.dump_interims: joined.to_csv(f'{args.output_dir}/joined_autos.csv')


  #This feels ridiculous, but works in conjunction with maxcolwidth.sh to check for columns too wide for Excel
  joined.replace(to_replace = '\n', value = 'N', regex = True).to_csv(path_or_buf = f'{args.output_dir}/lenchecker.csv', index = False, sep = '~')

  #Dump output
  if not args.no_stamp:
    remote = subprocess.run(['git', 'remote', '-v'], capture_output = True, check = True).stdout
    joined['Repo'] = [remote] * len(joined.index)
    commit = subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output = True, check = True).stdout
    joined['Commit'] =[commit] * len(joined.index)
    joined['Args'] = [' '.join(sys.argv)] * len(joined.index)
  joined.to_csv(path_or_buf = f'{args.output_dir}/{args.output}', index = False, quoting = csv.QUOTE_NONNUMERIC)

  #Update views file
  #A row that is complete in the old views file cannot be in the new views data because any data
  #pertaining to that row will have been removed during extraction. This includes any new data
  #pertaining to that row if late classifications have come in.
  #But a row that is incomplete in the old views file will be in the new views data as well. The
  #new data by itself contains the full updated status of that row (old views will have been re-read).
  #So we:
  # * Keep rows in the old file that have complete is True
  # * Replace rows from the old file that have complete is False
  # * Add rows that do not exist in the old file
  views_file = f'{args.output_dir}/views_{args.output}'
  if os.path.exists(views_file):
    old_views = pd.read_csv(views_file, index_col = [0, 1])
    try:
      joined_views = joined_views.append(old_views[old_views['complete']], verify_integrity = True).sort_index()
    except ValueError as e:
      print("Caught a ValueError. If this is overlapping values in the index, probably", file = sys.stderr)
      print("means that you have rerun an aggregation for which you already had a views file.", file = sys.stderr)
      raise e
  joined_views.to_csv(path_or_buf = views_file)

  track('* All done')

main()
