#!/usr/bin/env python3

import os
import re
import git
import sys
import math
import yaml
import shutil
import filecmp
import argparse
import subprocess
from datetime import datetime, timezone
from multiprocessing import Process
from enum import Enum

#globals
args = None
workflow_defs = None
class Phase(Enum):
  CONFIG = 'config'
  EXTRACT = 'extract'
  STRIP_PROCESSED = 'strip'
  CLEAN = 'clean'
  REDUCE = 'reduce'
DEFAULT_PHASES = [x.value for x in Phase]

def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--workflow_defs',
                      default = 'workflow.yaml',
                      help = 'File defining the workflows (default: workflow.yaml)')
  parser.add_argument('--workflows',
                      default = 'launch_workflows',
                      help = 'Label for workflows to process (default: launch_workflows). See workflow.yaml.')
  parser.add_argument('--exports', '-e',
                      default = 'exports',
                      help = 'Directory of exports from the Zooniverse project (default: exports)')
  parser.add_argument('--output_dir',
                      default = 'aggregation',
                      help = 'Set output dir (default: aggregation)')
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
                      help = 'Run only certain phases of the extractor. This has a high risk of producing poor-quality data and is a developer-only option. Particularly note that only certain phase orderings will work. This only selects phases, it does not affect phase order. If all phases are specified then that is equivalent to not specifying this option at all. If this option is specified then the aggregation/ dir is expected to already exist.')

  global args
  args = parser.parse_args()

def get_version(v):
  #We pick out the parts with string operations, rather than converting to float, because of versions like "19.60"
  m = re.fullmatch(r'(\d+)\.(\d+)', v)
  if not m:
    raise Exception(f'Version {v} is not in major.minor format')
  return (int(m[1]), int(m[2]))

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
      f'{args.exports}/hms-nhs-the-nautical-health-service-workflows.csv', w_id,
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
#TODO: I do not compare the extraction files here, because they will certainly differ by the version number to extract.
#      It anyway may be fine for them to differ if everything else is identical -- the extractions are per-version,
#      but the other config files are used in operations that are per-workflow (so per-multiple-versions).
#      Still, it would be nice to confirm that the extraction files are identical apart from version number.
#      This is the tuple entry for those files:
#      ('Extraction', [f'{args.output_dir}/Extractor_config_workflow_{w_id}_V{x[0]}.{x[1]}.yaml' for x in versions]),
# TODO: I do not compare the task labels here because they can contain differences that do not matter to aggregate.py.
#       All that matters for aggregate.py is that the labels are identical which, at time of writing, they are.
#       Still, it would be nice to confirm that the task label files are identical apart from unimportant differences.
#      ('Task label', [f'{args.output_dir}/Task_labels_workflow_{w_id}_V{x[0]}.{x[1]}.yaml' for x in versions]),
# FIXME: I also need to confirm that the differences in the task files do not affect the reduction script.
#        However, as the only difference is in a hex string used as a dictionary key, and as those dictionary keys do
#        not appear anywhere in the extracted CSV file, hopefully I am OK. There could still be a problem if, say, the
#        hex string is a hash of the label that is used in some way.
def config_check_identity(w_id, versions, ztype):
  for config_type, configs in (
    ('Reduction',  [f'{args.output_dir}/Reducer_config_workflow_{w_id}_V{x[0]}.{x[1]}_{ztype}_extractor.yaml' for x in versions]),
  ):
    if len(configs) > 1:
      base_config = configs.pop()
      for config in configs:
        if not filecmp.cmp(base_config, config, shallow = False):
          raise Exception(f'''{config_type} configuration files for different versions of workflow {w_id} differ.
We rely upon these being the same to allow us to concatenate the extractions and reduce them together.''')
        elif args.verbose:
          print(f'{config_type} configuration files for different versions of workflow {w_id} are identical.')

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
  if type(w_data['version']) is list:
    versions = list(map(get_version, w_data['version']))
  else:
    versions = [get_version(w_data['version'])]
  ztype = w_data['ztype']['type']
  export_csv = w_data['export']

  #The name of the concatenation of all extractions for this workflow id
  extraction_name = f'{args.output_dir}/{ztype}_extractor_{w_id}'

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


  if Phase.CLEAN.value in args.phase:
    clean_extraction(w_id, ztype, extraction_name + '.stripped.csv') #creates {extraction_name}.cleaned.csv

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

  for w_id, w_data in workflow_defs[args.workflows].items():
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
