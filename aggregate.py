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

#Columns to use in all cases, with the rules for reading them in
KEYS = ['subject_id', 'task']
#Use these converters/dtypes when reading from aggregation
KEYS_CONVERTERS = {'task': lambda x: int(str(x)[1:])} #drop the T, so that index matches
KEYS_DTYPES = {'subject_id': int}
#Use these converters/dtypes when reading from a previous run (i.e. the old_views file)
KEYS_CONVERTERS_2 = {}
KEYS_DTYPES_2 = {'subject_id': int, 'task': int}

parser = argparse.ArgumentParser()
parser.add_argument('workflows',
                    nargs = '?',
                    default = 'launch_workflows',
                    help = 'Label for workflows to process (see workflow.yaml).')
parser.add_argument('--output_dir',
                    default = 'output',
                    help = 'Set output dir (default: "output"). Must not already exist.')
parser.add_argument('--output', '-o',
                    default = 'joined.csv',
                    help = 'Set name of output file (default: "joined.csv"). Use --output_dir to change the output directory.')
parser.add_argument('--exports', '-e',
                    default = 'exports',
                    help = 'Directory of exports from the Zooniverse project (default: "exports")')
parser.add_argument('--reduced', '-r',
                    default = 'aggregation',
                    dest = 'dir',
                    help = 'Directory containing data reduced by Panoptes scripts (default: "aggregation")')
parser.add_argument('--text_threshold', '-t',
                    type = float,
                    default = 0.9,
                    help = 'Text consensus threshold, from 0 to 1. This is only used for "true text" fields -- so not for dropdowns, numbers or dates. (Default: 0.9)')
parser.add_argument('--dropdown_threshold', '-d',
                    type = float,
                    default = 0.66,
                    help = 'Dropdown consensus threshold, from 0 to 1. This is used for all "non-text" fields, so applies to dates and numbers as well as dropdowns. (Default: 0.66)')
parser.add_argument('--unfinished', '-u',
                    action = 'store_true',
                    help = 'Include classifications with insufficient number of views and pages with incomplete or missing rows')
parser.add_argument('--uncertainty',
                    action = 'store_true',
                    help = 'Treat certain patterns as indicating presence of an uncertain transcription and requiring manual review. May result in a lot of additional manual work as this is a pre-reconciliation check: reconciled algorithms may reconcile-out the uncertainty markers. The script always flags similar patterns in reconciled strings: this is less work to clean up but relies upon believing that auto-reconciliation has coped OK with uncertainty markers in the original transcriptions.')
parser.add_argument('--no_transcriptionisms',
                    action = 'store_true',
                    help = 'Skip "always on" post-reconciliation check for patterns indicating transcription uncertainty. This saves a lot of time so can be helpful in development.')
parser.add_argument('--no_stamp', '-S',
                    action = 'store_true',
                    help = 'Do not stamp the output with information about the script used to generate it')
parser.add_argument('--verbose', '-v',
                    type = int,
                    default = 0,
                    help = 'Set to higher numbers for increasing verbosity')
parser.add_argument('--timing',
                    action = 'store_true',
                    help = 'Give timing information for phases in the program')
parser.add_argument('--flow_report', '-f',
                    action = 'store_true',
                    help = ('Show information about paths taken through program.\n'
                            'Used in conjunction with coverage.sh to make sure that test inputs are testing all paths.'
                           )
                   )
parser.add_argument('--dump_interims',
                    action = 'store_true',
                    help = 'Dump out CSV files at intermediate stages of processing. Helpful for testing and debugging.')
parser.add_argument('--row_factor',
                    type = float,
                    help = 'Percentage of total rows to read. Repeatable across runs, for faster testing cycles.'
                   )
args = parser.parse_args()
subjects = pd.read_csv(f'{args.exports}/hms-nhs-the-nautical-health-service-subjects.csv',
                         usecols   = ['subject_id', 'metadata', 'locations'],
                         dtype = {'subject_id': int, 'metadata': str, 'locations': str})

def dump_interim(pandas_thing, fnam):
  if args.dump_interims:
    fnam = re.compile(r'[ \(\)]').sub('_', fnam)
    pandas_thing.to_csv(f'{args.output_dir}/interims/{fnam}.csv')

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


def pretty_candidates(candidates, best_guess = '<No best guess>'):
  container = candidates
  if isinstance(candidates, str):
    container = ast.literal_eval(candidates)
    assert isinstance(container, list)

  if isinstance(container, list):
    container_count = Counter(unaligned(container))
  elif isinstance(container, dict):
    container_count = {str(k): v for k, v in candidates.items()}
  else:
    raise Exception(f"Unexpected data type {type(container)} while prettifying candidates")

  retval = [ best_guess, '----------' ]
  for k, v in container_count.items():
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
  #Note that it may be that only part of the uncertainty identifier has survived autoresolution.
  #For this reason, we cannot use the exact same patterns as in the pre-resultion function 'uncertainty'.
  for pattern in [
    r'[\[\]\{\}]', #Any sort of bracket (apart from round, which come up too much in correct contexts)
    r'\.\.', #More than one '.' in succession
    r'\?' #A question mark
    #r'[^\d]*\.[^ \d$]', #A single dot that (a) does not appear to be part of a number and (b) does not appear to be a full stop. Did away with this one as it matches dots in abbreviations and initials -- far too noisy.
  ]:
    if row.str.contains(pattern).any():
      bad[row.name] = 1
      return
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
                          converters = KEYS_CONVERTERS,
                          dtype = { **KEYS_DTYPES, 'classification_id': int, 'user_id': float } #user_id is float so that blanks can be NaN
                         )

  #Sanity check -- the uncleaned (but tranche-processed) extraction file should contain the same classification ids
  extractor_new_series = pd.read_csv(args.dir + '/' + f'text_extractor_{wid}.csv.new', index_col = KEYS, usecols = KEYS + ['classification_id'], converters = KEYS_CONVERTERS, dtype = {**KEYS_DTYPES, 'classification_id': int})['classification_id']
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

  if uncertainty(originals):
    flow_report('Uncertain transcriber', row.name, row['data.aligned_text'])
    bad[row.name] += 1
    return pretty_candidates(candidates, row['data.consensus_text'])

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
  if uncertainty(unaligned(ast.literal_eval(row['data.aligned_text']))):
    flow_report('Uncertain transcriber', row.name, row['data.aligned_text'])
    bad[row.name] += 1
    return pretty_candidates(row['data.aligned_text'], row['data.consensus_text'])
  if row['data.consensus_score'] / row['data.number_views'] < args.text_threshold:
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

    if uncertainty(candidates):
      flow_report('Uncertain transcriber', row.name, row['data.aligned_text'])
      bad[row.name] += 1
      return pretty_candidates(row['data.aligned_text'], row['data.consensus_text'])

    #Check the candidates for any with a zero-field. Pass through for manual check if this happens.
    for x in candidates:
      parts = [int(y) for y in re.split(r'[-/\.]', x)]
      if len(parts) != 3 or 0 in parts:
        flow_report('Zero-field (or bad field count)', row.name, row['data.aligned_text'])
        bad[row.name] += 1
        return pretty_candidates(row['data.aligned_text'], row['data.consensus_text'])

    #https://stackoverflow.com/a/18029112 has a trick for reading arbitrary date formats while rejecting ambiguous cases
    #We just need to use the documented format, but we can be a bit forgiving
    #TODO: Improve date handling, see https://github.com/nationalarchives/hms-nhs-scripts/issues/11
    #TODO: This seems a bit redundant with data cleaning, might be able to make this bit faster by skipping the date parsing.
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
      return date.strftime('%b %d %Y')
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

  if uncertainty(candidates):
    flow_report('Uncertain transcriber', row.name, row['data.aligned_text'])
    bad[row.name] += 1
    return pretty_candidates(row['data.aligned_text'], row['data.consensus_text'])

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
  if args.dump_interims: os.mkdir(f'{args.output_dir}/interims')

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
  removed = []

  track('Processing workflows')
  for wid, data in workflow[args.workflows].items():
    workflow_columns.append(data['name'])
    datacol = data['ztype']['name']
    conflict_keys = {}
    reduced_file = f'{args.dir}/{data["ztype"]["type"]}_reducer_{wid}.csv'
    if data['ztype'] == TEXT_T: #data that we use to make decisions about how well reconciliation worked
      conflict_keys = {
        'data.aligned_text': str,
        'data.number_views': float, #TODO: I cannot see how this would be other than an int, but Pandas insists that it must be treated as float -- maybe due to NaNs??
        'data.consensus_score': float
      }
    try:
      df = pd.read_csv(reduced_file,
                       index_col = KEYS,
                       usecols   = KEYS + [datacol] + list(conflict_keys.keys()),
                       converters = KEYS_CONVERTERS,
                       dtype = {**KEYS_DTYPES, datacol: str, **conflict_keys},
                       skip_blank_lines = False)
    except:
      print(f'Error while reading {reduced_file}')
      raise

    #count views
    if data['ztype'] == TEXT_T:
      #For Zooniverse-text fields, we must count rows in the extractor to get an accurate number of views.
      #This is because the reducer discards empty entries, but we consider these to be legitimate views.
      current_views, nonunique_counts = count_text_views(wid)
      if nonunique_counts is not None: #We also count repeat classifications by the same (logged-in) user
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
    dump_interim(current_views, f'current_views_{data["name"]}')

    if not args.unfinished:
      #Drop all classifications that are based on an insufficient number of views
      unfinished_idx = current_views[current_views < RETIREMENT_COUNT].index
      removed.append(df.loc[unfinished_idx][datacol].rename(data['name'])) #We never write to this, so don't really mind if this is a reference or a copy (though copy would be less fragile!)
      df = df.drop(unfinished_idx)
      current_views = current_views.drop(unfinished_idx)
      dump_interim(removed[-1], f'removed_{data["name"]}')
      if args.verbose >= 1 and len(removed[-1]) != 0: print(f'  Removed {len(removed[-1])} classifications from {data["name"]} due to unreached retirement count')
      if args.verbose >= 3: print(removed[-1])

    #User can shrink the number of rows to be read, for faster runs.
    #This will be used for run-to-run output comparison, so must be repeatable.
    #Taking every nth row might give a better overall sample of the data than just taking head or tail.
    #But if people tend to classify the same records at around the same time, every nth row might result in few complete classifications.
    #Note that this does not affect the views count for TEXT_T, which is always based on the entire text_exporter file. This means the results are spurious in that rows are admitted even if they no longer have enough views to be considered complete, but the point of this feature is just to be able to compare for unexpected output changes.
    if args.row_factor:
      assert(len(df.index.symmetric_difference(current_views.index)) == 0)
      df = df.iloc[::int(100 / args.row_factor)]
      current_views = current_views.loc[df.index]

    #Report on rows with different counts
    if args.verbose >= 1:
      overcount = df.loc[current_views[current_views > RETIREMENT_COUNT].index]
      print(f'  Completed rows: {len(df.index)} (of which {len(overcount.index)} overcounted)')
      if args.verbose >= 3 and not overcount.empty: print(overcount)
      if args.unfinished:
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
    views.append(current_views)
    track(f'* {reduced_file} ({data["name"]}) done', regardless = True)

  track('Generating output', regardless = True)
  for c in columns: dump_interim(c, c.columns[0])
  #Combine the separate workflows into a single dataframe
  #Assumption: Task numbers always refer to the same row in each workflow
  #            If this assumption does not hold, we can perform a mapping
  #            on the dataframes at the point that we read them in, above.
  #Quick test shows that this assumption does hold for now.
  first = columns.pop(0)
  joined = first.join(columns, how='outer')
  track('* Data joined')
  dump_interim(joined, 'initial_joined')

  for v in views: dump_interim(v, f'views_{v.name}')
  first = views.pop(0).to_frame()
  joined_views = first.join(views, how='outer')
  track('* Views joined')
  dump_interim(joined_views, 'initial_joined_views')

  first = removed.pop(0).to_frame()
  first.join(removed, how='outer').to_csv(f'{args.output_dir}/incomplete_rows.csv')
  track('* Removed fields logged')

  if not joined.index.equals(joined_views.index):
    print('Indexes of joined and joined_views are not equal. The indexes may have a different order. The following entries are in only one index:', file = sys.stderr)
    print(joined.index.symmetric_difference(joined_views.index), file = sys.stderr)
    raise Exception('joined index differed from joined_views index\n')
  if not joined.index.unique:
    raise Exception('joined and joined_views have non-unique index')
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
  if not args.no_transcriptionisms:
    joined.apply(has_transcriptionisms, axis = 'columns')
    track('* Transcriptionisms identified')
    dump_interim(joined, 'joined_has_transcriptionisms')

  #Translate subjects ids into original filenames
  #Assumption: the metadata is invariant across all of the entries for each subject_id
  subj_info_cols = {
    'original': {},
    'subject': {},
    'volume': {},
    'page': {},
  }
  joined.insert(0, 'original', '')
  joined.insert(1, 'subject', '')
  joined.insert(2, 'volume', '')
  joined.insert(3, 'page', '')
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

    subj_info_cols['original'][sid] = location
    subj_info_cols['subject'][sid] = sid
    subj_info_cols['volume'][sid] = vol
    subj_info_cols['page'][sid] = page
  for level_name, value in subj_info_cols.items():
    for sid, subj_info in value.items():
      joined.loc[[sid], level_name] = subj_info

  #Identify volume 1's subject ids, for special handling around 'port sailed out of'
  vol_1_subj_ids = list(set(joined[joined['volume'] == 1].index.get_level_values(0)))

  track('* Subjects identified')
  dump_interim(joined, 'joined_subjects_identified')

  #Handle the 'port sailed out of' special case
  bad_ports = joined.loc[vol_1_subj_ids]['port sailed out of'][joined.loc[vol_1_subj_ids]['port sailed out of'].notnull()].index
  for bad_port in bad_ports:
    if bad_port in autoresolved:
      flow_report('port sailed out of in volume 1 (later)', bad_port, joined.loc[bad_port])
      autoresolved[bad_port]['port sailed out of'] = None
    else:
      flow_report('port sailed out of in volume 1 (first)', bad_port, joined.loc[bad_port])
      autoresolved[bad_port] = { 'port sailed out of': None }
  if args.verbose >= 1 and len(bad_ports) != 0: print(f'  {len(bad_ports)} rows in volume 1 incorrectly had a port')
  joined.loc[bad_ports,['original','volume','page','port sailed out of']].to_csv(f'{args.output_dir}/ports_removed.csv')
  joined.loc[bad_ports,['port sailed out of']] = ''
  track('* "Port sailed out of" fixed up')

  joined_views['complete'] = joined_views[workflow_columns].ge(RETIREMENT_COUNT).all(axis = 1)
  joined_views.loc[vol_1_subj_ids,['complete']] = joined_views.loc[vol_1_subj_ids][workflow_columns].drop('port sailed out of', axis = 1).ge(RETIREMENT_COUNT).all(axis = 1)
  track('* Complete views identified')
  dump_interim(joined_views, 'joined_views_complete')

  #Tag or remove the rows with badness
  joined.insert(len(joined.columns), 'Problems', '')
  joined['Problems'] = joined[workflow_columns].isnull().values.any(axis = 1)
  joined.loc[vol_1_subj_ids,['Problems']] = joined.loc[vol_1_subj_ids][workflow_columns].drop('port sailed out of', axis = 1).isnull().values.any(axis = 1)
  joined['Problems'] = joined['Problems'].map({True: 'Blank(s)', False: ''})
  dump_interim(joined, 'joined_problems')
  track('* Badness identified')

  #The following code assumes equal indices, so confirm that this is still the case.
  #(We earlier checked for equality and uniqueness. We do not do anything that should
  #change uniqueness, but it is worth checking that any removals have happened in both
  #dataframes.
  if not joined.index.equals(joined_views.index): raise Exception('joined index differs from joined_views index (equals)')
  if not args.unfinished:
    incomplete_subjects = []
    for sid in joined_views.index.get_level_values('subject_id').unique():
      page_completes = joined_views.loc[[sid]]['complete']
      if (not page_completes.all()) or not(len(page_completes) == 25):
        incomplete_subjects.append(sid)

    incomplete_joined = joined.query(f'subject_id in @incomplete_subjects')
    incomplete_joined.to_csv(f'{args.output_dir}/incomplete_pages.csv')
    for key in incomplete_joined.index.unique().to_numpy():
      bad.pop(key, None)
      autoresolved.pop(key, None)

    joined = joined.drop(incomplete_joined.index)
    dump_interim(joined, 'joined_unfinished')
    dump_interim(joined_views, 'joined_views_unfinished')
    track('* Incompletes removed')


  #Tag unresolved unresolved fields
  #TODO This part does not feel like the Pandas way
  for b in bad.keys():
    problems = joined.at[b, 'Problems']
    if len(problems):
      joined.at[b, 'Problems'] = f'{problems} & at least {bad[b]} unresolved fields'
    else:
      joined.at[b, 'Problems'] = f'At least {bad[b]} unresolved fields'
  track('* Unresolved identified')
  dump_interim(joined, 'joined_unresolved')


  #Record where there was autoresolution
  joined.insert(len(joined.columns), 'Autoresolved', '')
  #TODO: Again, doesn't feel like Pandas
  for index, value in autoresolved.items():
    joined.at[index, 'Autoresolved'] = '; '.join(filter(lambda x: x in value.keys(), workflow_columns))
  track('* Autos identified')
  dump_interim(joined, 'joined_autos')

  #joined.csv is complete: now sort it
  joined = joined.sort_values(['volume', 'page'], kind = 'stable') #stable so that we maintain the row order within the page

  #This feels ridiculous, but works in conjunction with maxcolwidth.sh to check for columns too wide for Excel or Sheets. We use ^ as the separator because it happens to work -- a non-printing char would be better, but to_csv does not permit them.
  joined.replace(to_replace = '\n', value = 'N', regex = True).to_csv(path_or_buf = f'{args.output_dir}/lenchecker.csv', index = False, sep = '^')

  #Dump output
  if not args.no_stamp:
    remote = subprocess.run(['git', 'remote', '-v'], capture_output = True, check = True).stdout
    joined['Repo'] = [remote] * len(joined.index)
    commit = subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output = True, check = True).stdout
    joined['Commit'] =[commit] * len(joined.index)
    joined['Args'] = [' '.join(sys.argv)] * len(joined.index)
  joined['§°—’“”…；£ªéºöœü'] = '' #TODO: Find a better way to force Google Sheets to recognise the character encoding as UTF-8
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
    old_views = pd.read_csv(views_file, index_col = KEYS, converters = KEYS_CONVERTERS_2, dtype = {**{k: int for k in workflow_columns}, **KEYS_DTYPES_2, 'complete': bool})
    try:
      joined_views = joined_views.append(old_views[old_views['complete']], verify_integrity = True).sort_index()
    except ValueError as e:
      print("Caught a ValueError. If this is overlapping values in the index, probably", file = sys.stderr)
      print("means that you have rerun an aggregation for which you already had a views file.", file = sys.stderr)
      raise e
  joined_views.to_csv(path_or_buf = views_file)

  track('* All done')

main()
