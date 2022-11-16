#!/usr/bin/env python3

import os
import re
import csv
import git
import sys
import math
import yaml
import pandas as pd
import shutil
import filecmp
import argparse
import subprocess
from collections import Counter
from datetime import datetime, timezone
from multiprocessing import Process
from enum import Enum
import subjects

#globals
args = None
workflow_defs = None
class Phase(Enum):
  SUBJECTS = 'subjects'
  CONFIG = 'config'
  EXTRACT = 'extract'
  STRIP_PROCESSED = 'strip'
  PICK_VOLUMES = 'pick'
  CLEAN = 'clean'
  POST_EXTRACT = 'post_extract'
  REDUCE = 'reduce'
DEFAULT_PHASES = [x.value for x in Phase]

def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('workflow_set',
                      help = 'Label for set of workflows to process. See workflow.yaml. "phase1" and "phase2" are good values.')
  parser.add_argument('--workflow_defs',
                      default = 'workflow.yaml',
                      help = 'File defining the workflows (default: workflow.yaml)')
  parser.add_argument('--exports', '-e',
                      default = 'exports',
                      help = 'Directory of exports from the Zooniverse project (default: exports)')
  parser.add_argument('--output_dir',
                      default = 'extraction',
                      help = 'Set output dir (default: extraction)')
  parser.add_argument('--verbose', '-v',
                      action = 'store_true',
                      help = 'Verbose output')
  parser.add_argument('--no_tranche',
                      action = 'store_true',
                      help = 'Do not generate tranche info. This does not prevent use of existing tranche info to eliminate previously-completed rows.')
  parser.add_argument('--phase',
                      nargs = '*',
                      choices = DEFAULT_PHASES,
                      default = DEFAULT_PHASES,
                      help = 'Run only certain phases of the extractor. This has a high risk of producing poor-quality data and is a developer-only option. Particularly note that only certain phase orderings will work, and that \'subjects\' is a "split" phase, governing two different blocks of code at different points in the sequence. This option only selects the phases to run, it does not affect phase order or the number of times that each phase is run. If all phases are specified then that is equivalent to not specifying this option at all. If this option is specified then the extraction/ dir is expected to already exist.')

  global args
  args = parser.parse_args()

def get_version(v):
  #We pick out the parts with string operations, rather than converting to float, because of versions like "19.60"
  m = re.fullmatch(r'(\d+)\.(\d+)', v)
  if not m:
    raise Exception(f'Version {v} is not in major.minor format')
  return (int(m[1]), int(m[2]))

#The base name of the concatenation of all extractions for this workflow id
def get_extraction_name(w_id, w_data):
  return f'{args.output_dir}/{w_data["ztype"]["type"]}_extractor_{w_id}'

def runit(subproc_args, logfile):
  stringified_args = list(map(lambda x: str(x) if type(x) is int else x, subproc_args))
  if args.verbose:
    print(logfile + ':', ' '.join(stringified_args))
  try:
    with open(logfile, 'w') as f:
      subprocess.run(stringified_args, stdout = f, stderr = subprocess.STDOUT, check = True)
  except subprocess.CalledProcessError as e:
    print(f'*** The following command failed with exit code {e.returncode}:', file = sys.stderr)
    print('   ', ' '.join(e.cmd), file = sys.stderr)
    with open(logfile, 'r') as f: print(''.join(['    ' + x for x in f.readlines()]), file = sys.stderr)
    raise e

def tranche_info():
  #Log number of lines in input files. This should allow me to recreate the exact same result by slicing the end off future downloads.
  tranchedir=f'tranches/{datetime.now(timezone.utc).strftime("%Y%m%d%H%M_GMT")}'
  os.mkdir(tranchedir)
  with open(f'{tranchedir}/lines.txt', 'w') as f:
    all_lines = 0
    for export in sorted(os.listdir(f'{args.exports}')):
      with open(f'exports/{export}') as g:
        lines = len(g.readlines())
        print(f'{lines:9} exports/{export}', file = f)
        all_lines += lines
    print(f'{all_lines:9} total', file = f)

  #Grab the last few classification ids as well, just in case
  with open(f'{tranchedir}/last_classifications.txt', 'w') as f:
    for export in sorted(os.listdir(f'{args.exports}')):
      with open(f'exports/{export}') as g:
        print(f'==> exports/{export} <==', file = f)
        print('\n'.join(map(lambda x: x[:x.find(',')], g.readlines()[-10:])), file = f)
        print(file = f)

  #Record git info about the script used to do the extraction
  #In the normal run of things I would expect this to be the same as that used for aggregate.py
  #But it is possible for the two to diverge, so let's record both
  with open(f'{tranchedir}/generated_by', 'w') as f:
    g = git.Repo('.').git
    print(g.remote('-v'), file = f)
    print('origin/main', g.rev_parse('origin/main'), file = f)
    print('HEAD       ', g.rev_parse('HEAD'), file = f)
    print(g.status(), file = f)

  return tranchedir

def panoptes_config(w_id, versions):
  for major, minor in versions:
    runit([
      'panoptes_aggregation', 'config',
      f'{args.exports}/{workflow_defs["export"]}', w_id,
      '-v', f'{major}.{minor}',
      '-d', args.output_dir
      ],
      f'{args.output_dir}/config_{w_id}_V{major}.{minor}.log'
    )

def config_fixups(w_id, versions):
  for major, minor in versions:
    if w_id == 18624 and major == 3 and minor == 1:
      with open(f'{args.output_dir}/Task_labels_workflow_{w_id}_V{major}.{minor}.yaml') as f:
        lines = f.readlines()
      with open(f'{args.output_dir}/Task_labels_workflow_{w_id}_V{major}.{minor}.yaml', 'w') as f:
        for line in lines:
          print(re.sub('To a Ship Cured', 'To a/his Ship Cured', line), file = f, end = '')

#Check that outputs are identical. Saves thinking if at least reductions and tasks are.
# FIXME: I would like to confirm that the differences in the task files do not affect the reduction script.
#        However, as the only difference is in a hex string used as a dictionary key, and as those dictionary keys do
#        not appear anywhere in the extracted CSV file, hopefully I am OK. There could still be a problem if, say, the
#        hex string is a hash of the label that is used in some way.
def config_check_identity(w_id, versions, ztype):
  #Functions to transform-out parts of files that we do not want to compare
  def dropdown_label_transformer(config_fnam):
    with open(config_fnam) as f: config_dict = yaml.load(f, Loader = yaml.Loader)
    for key in list(config_dict): #list-ing a dictionary gives a list of the keys
      if key.endswith('.label'):
        sub_dict = config_dict[key]
        if len(sub_dict) != 1: print(f'Error: Unexpected file shape in {config}: selection options are expected to have exactly 1 label', file = sys.stderr)
        sub_key = next(iter(sub_dict.keys()))
        if not re.fullmatch('[a-f0-9]+', sub_key):
          print(f'Error: Unexpected file shape in {config}: keys for labels are expected to be a hex number, got {sub_key}', file = sys.stderr)
        sub_value = next(iter(sub_dict.values()))
        config_dict[key] = sub_value
    return yaml.dump(config_dict)

  def extraction_transformer(config_fnam):
    with open(config_fnam) as f:
      config_dict = yaml.load(f, Loader = yaml.Loader)
      del config_dict['workflow_version']
      return yaml.dump(config_dict)

  #The actual comparison code
  bad_comparisons = []
  for config_type, configs in (
    ('reduction',  [f'{args.output_dir}/Reducer_config_workflow_{w_id}_V{x[0]}.{x[1]}_{ztype}_extractor.yaml' for x in versions]),
    ('task label', [f'{args.output_dir}/Task_labels_workflow_{w_id}_V{x[0]}.{x[1]}.yaml' for x in versions]),
    ('extraction', [f'{args.output_dir}/Extractor_config_workflow_{w_id}_V{x[0]}.{x[1]}.yaml' for x in versions])
  ):
    if len(configs) == 1: continue

    base_config = configs.pop()
    if config_type == 'reduction' or \
       (config_type == 'task label' and workflow_defs[args.workflow_set]['workflows'][w_id]['ztype'] == workflow_defs['definitions']['TEXT_T']):
      for config in configs:
        if not filecmp.cmp(base_config, config, shallow = False): bad_comparisons.append((config_type, w_id, config, base_config))
        elif args.verbose: print(f'{config_type} configuration file {config} for workflow {w_id} is identical to {base_config}.')
    else:
      if config_type == 'task label': #Dropdown. Eliminate the variable hex numbers, compare the rest of the file
        assert workflow_defs[args.workflow_set]['workflows'][w_id]['ztype'] == workflow_defs['definitions']['DROP_T']
        transformer = dropdown_label_transformer
      elif config_type == 'extraction': #It is possible that differences in extraction files are unimportant. Figure this out if it ever comes up.
        transformer = extraction_transformer
      else: assert False #unreachable

      base_config_transformed = transformer(base_config)
      for config in configs:
        if base_config_transformed != transformer(config): bad_comparisons.append((config_type, w_id, config, base_config))
        elif args.verbose: print(f'{config_type} configuration file {config} for workflow {w_id} is identical to {base_config}.')

  if len(bad_comparisons) != 0:
    for x in bad_comparisons: print(f'{x[0]} configuration files for different versions of workflow {x[1]} differ: {x[2]} differs from base {x[3]}.', file = sys.stderr)
    print('We rely upon these being the same to allow us to concatenate the extractions and reduce them together.', file = sys.stderr)
    raise Exception

def panoptes_extract(w_id, versions, ztype, export_csv, extraction_name):
  outputs = []
  for major, minor in versions:
    runit([
      'panoptes_aggregation', 'extract',
      f'{args.exports}/{export_csv}',
      f'{args.output_dir}/Extractor_config_workflow_{w_id}_V{major}.{minor}.yaml',
      '-d', args.output_dir,
      '-o', f'{w_id}_V{major}_{minor}' #anything following a '.' in here appears to get discarded, so use _ instead
      ],
      f'{args.output_dir}/extract_{w_id}_V{major}.{minor}.log'
    )
    outputs.append(f'{args.output_dir}/{ztype}_extractor_{w_id}_V{major}_{minor}.csv')
  shutil.copy(outputs.pop(0), extraction_name + '.full.csv')
  with open(extraction_name + '.full.csv', 'a') as concatenated_file:
    for output in outputs:
      with open(output, 'r') as f:
        f.readline() #do not copy extra header lines
        shutil.copyfileobj(f, concatenated_file)

def strip_processed(w_id, views, input_name, logname, *extra_args):
  runit([
    './strip_processed.py',
    '-t', views,
    input_name
    ] + list(extra_args),
    f'{args.output_dir}/{logname}_{w_id}.log'
  )

def pick_volumes(w_id, input_name):
  runit([
    './pick_volumes.py', input_name,
    '--first_volume', workflow_defs[args.workflow_set]['first_volume'],
    '--final_volume', workflow_defs[args.workflow_set]['final_volume'],
    '--subjects_cache', f'{args.output_dir}/subjects_metadata.csv'
    ],
    f'{args.output_dir}/pick_volumes_{w_id}.log'
  )

def clean_extraction(w_id, ztype, input_name):
  runit(['./clean_extraction.py', input_name, w_id], f'{args.output_dir}/postextract_{w_id}.log')

def panoptes_reduce(w_id, versions, ztype, input_name):
  major, minor = versions[0] #We already check in panoptes_config_identity that all reduction configs are the same
  result = runit([
    'panoptes_aggregation', 'reduce',
    '-F', 'all',
    '-d', args.output_dir,
    '-o', w_id,
    input_name,
    f'{args.output_dir}/Reducer_config_workflow_{w_id}_V{major}.{minor}_{ztype}_extractor.yaml'
    ],
    f'{args.output_dir}/reduce_{w_id}.log'
  )

def panoptes(w_id, w_data):
  if 'version' in w_data:
    if type(w_data['version']) is list:
      versions = list(map(get_version, w_data['version']))
    else:
      versions = [get_version(w_data['version'])]
  else: #get all actually-used versions by looking in the export file. Report on what is found.
    with open(f"{args.exports}/{w_data['export']}", 'r') as export_file:
      reader = csv.DictReader(export_file)
      counted_versions = Counter([x['workflow_version'] for x in reader])
      print(f'No workflow version(s) given for {w_id} ({w_data["name"]}). Will use the following detected workflow version(s):')
      print('\n'.join([f'{k:>10}: {v:>8} instances' for k, v in counted_versions.items()]))
      versions = [get_version(x) for x in counted_versions.keys()]
  ztype = w_data['ztype']['type']
  export_csv = w_data['export']

  #The base name of the concatenation of all extractions for this workflow id
  extraction_name = get_extraction_name(w_id, w_data)

  #These functions iterate per-version
  if Phase.CONFIG.value in args.phase:
    panoptes_config(w_id, versions)
    config_fixups(w_id, versions)
    config_check_identity(w_id, versions, ztype)

  #This iterates per-version, but concatenates its results into a single output {extraction_name}.full.csv
  if Phase.EXTRACT.value in args.phase:
    panoptes_extract(w_id, versions, ztype, export_csv, extraction_name) #creates {extraction_name}.full.csv

  #Because we are working on the output of panoptes_extract, we are no longer version-sensitive

  #This is a built-in check that strip_processed.py seems to be working as expected -- this should be an identity transform
  if Phase.STRIP_PROCESSED.value in args.phase:
    strip_processed(w_id, 'tranches/empty_views.csv', f'{extraction_name}.full.csv', 'strip_identity_tranform_test', '--no_sort') #creates {extraction_name}.stripped.csv
    subprocess.run(['diff', '-q', f'{extraction_name}.full.csv', f'{extraction_name}.stripped.csv'], check = True, capture_output = True)

    #Whereas this will actually remove previously-completed rows of data
    strip_processed(w_id, 'tranches/views.csv', f'{extraction_name}.full.csv', 'strip_seen') #creates {extraction_name}.stripped.csv

  if Phase.PICK_VOLUMES.value in args.phase:
    pick_volumes(w_id, extraction_name + '.stripped.csv') #creates {extraction_name}.vols.csv

  if Phase.CLEAN.value in args.phase:
    clean_extraction(w_id, ztype, extraction_name + '.vols.csv') #creates {extraction_name}.cleaned.csv

  if Phase.POST_EXTRACT.value in args.phase:
    #All extraction phases have run, copy the final output to the expected filename for extractions
    shutil.copyfile(f'{extraction_name}.cleaned.csv', extraction_name + '.csv')

  #Special case -- this could be version sensitive, as panoptes_config provides the reduction
  #configuration that it uses. However, config_check_identity confirms that all
  #reduction configs are the same, so in practice this is not version sensitive.
  if Phase.REDUCE.value in args.phase:
    panoptes_reduce(w_id, versions, ztype, extraction_name + '.csv')


def main():
  exit_code = 0
  procs = []

  parse_args()
  if set(args.phase) == set(DEFAULT_PHASES):
    try: os.mkdir(args.output_dir)
    except FileExistsError:
      print(f"Output directory '{args.output_dir}' already exists.\nPlease delete it before running this script, or use --output_dir to output to a different directory.", file = sys.stderr)
      sys.exit(1)
  else:
    print('Running phases ' + ', '.join(filter(lambda x: x in args.phase, DEFAULT_PHASES))) #Comprehension to preserve phase order, rather than getting arbitrary CLI order
    print('Warning: Running a sub-set of all phases, likely on dirty data. Not recommended for a production run.', file = sys.stderr)
  if not args.no_tranche: tranchedir = tranche_info()

  os.nice(5) #Increase our niceness before we start hammering out processes
  with open(args.workflow_defs) as f:
    global workflow_defs
    workflow_defs = yaml.load(f, Loader = yaml.Loader)

  if Phase.SUBJECTS.value in args.phase:
    subjects_dfs = {}
    (subjects_dfs['subjects'], subjects_dfs['supplements'], subjects_dfs['duplicates']) = subjects.create_subjects_df(f'{args.exports}/{workflow_defs["subjects"]["export"]}', f'{args.output_dir}/subjects_metadata.csv', workflow_defs['subjects']['supplements'] if 'supplements' in workflow_defs['subjects'] else None)

  for w_id, w_data in workflow_defs[args.workflow_set]['workflows'].items():
    p_name = f'panoptes-wid-{w_id}-{w_data["name"].replace(" ", "_")}'
    p = Process(target = panoptes, name = p_name, args = (w_id, w_data))
    p.start()
    if args.verbose:
      print(f'Launched {p_name} as pid {p.pid}')
    procs.append(p)

  for p in procs:
    p.join()
    if p.exitcode != 0: exit_code = p.exitcode
    print(f'{p.name} completed with exit code {p.exitcode}')
  if exit_code != 0:
    sys.exit(exit_code)

  if Phase.SUBJECTS.value in args.phase:
    #Subject metadata checks
    used_subject_ids = set()
    for extraction_name in [get_extraction_name(x[0], x[1]) for x in workflow_defs[args.workflow_set]['workflows'].items()]:
      used_subject_ids.update(pd.read_csv(f'{extraction_name}.csv',
                                          usecols = ['subject_id'],
                                          dtype = {'subject_id': int}).squeeze('columns').values)
    #Check whether more than one of each duplicate set is referenced
    dups_found = False
    for entry in subjects_dfs['duplicates'].index.unique():
      found = []
      for s_id in subjects_dfs['duplicates']['subject_id'].loc[entry]:
        if s_id in used_subject_ids: found.append(s_id)
      if len(found) > 1:
        dups_found = True
        print(f'Error: Volume {entry[0]:2} p. {entry[1]:3} is classified under multiple subject ids: {", ".join(found)}', file = sys.stderr)
    if dups_found: sys.exit(1)

    #Check whether supplements are referenced
    for subject_id, subject_data in subjects_dfs['supplements'].iterrows():
      if subject_id in used_subject_ids:
        print(f'Supplementary subject {subject_id} (vol. {subject_data.volume:2}, p. {subject_data.page:3}) has at least one classification')
      else:
        print(f'Supplementary subject {subject_id} (vol. {subject_data.volume:2}, p. {subject_data.page:3}) has no classifications')

  print('All done, no errors')
  if args.no_tranche:
    print(f'''Suggested next invocation:
./aggregate.py -r {args.output_dir} -t 0.3''')
  else:
    print(f'''Suggested next invocations:
git add tranches
git commit -m'Latest data extraction'
./aggregate.py -r {args.output_dir} -t 0.3''')
#cp output/views_joined.csv tranches/views.csv #This one only applies if we want to encourage people to use the untested feature to skip over previously completed rows. Adding and committing "tranches", on the other hand, records reproduction information that we should keep.

main()
