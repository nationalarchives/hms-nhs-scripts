#!/usr/bin/env python3

import os
import re
import git
import sys
import math
import yaml
import shutil
import argparse
import subprocess
from datetime import datetime, timezone
from multiprocessing import Process

#globals
args = None
workflow_defs = None

def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--workflow_defs',
                      default = 'workflow.yaml',
                      help = 'File defining the workflows')
  parser.add_argument('--workflows',
                      default = 'launch_workflows',
                      help = 'Label for workflows to process (see workflows.yaml)')
  parser.add_argument('--exports', '-e',
                      default = 'exports',
                      help = 'Directory of exports from the Zooniverse project')
  parser.add_argument('--output_dir',
                      default = 'aggregation',
                      help = 'Set output dir')
  parser.add_argument('--verbose', '-v',
                      action = 'store_true',
                      help = 'Verbose output')
  global args
  args = parser.parse_args()

def get_version(v):
  if not type(v) is float:
    raise Exception()
  minor, major = math.modf(v)
  minor = f'{minor:.10g}' #10 significant digits should be enough for any version number, right?
  if not minor[0:2] == '0.':
    raise Exception()
  minor = minor[2:]
  return (int(major), int(minor))

def runit(subproc_args, logfile):
  stringified_args = list(map(lambda x: str(x) if type(x) is int else x, subproc_args))
  if args.verbose:
    print(logfile + ':', ' '.join(stringified_args))
  with open(logfile, 'w') as f:
    subprocess.run(stringified_args, stdout = f, stderr = subprocess.STDOUT, check = True) 

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

def panoptes_config(w_id, major, minor):
  runit([
    'panoptes_aggregation', 'config',
    f'{args.exports}/hms-nhs-the-nautical-health-service-workflows.csv', w_id,
    '-v', major,
    '-m', minor,
    '-d', args.output_dir
    ],
    f'{args.output_dir}/config_{w_id}.log'
  )

def config_fixups(w_id, major, minor):
  if w_id == 18624:
    with open(f'{args.output_dir}/Task_labels_workflow_{w_id}_V{major}.{minor}.yaml') as f:
      lines = f.readlines()
    with open(f'{args.output_dir}/Task_labels_workflow_{w_id}_V{major}.{minor}.yaml', 'w') as f:
      for line in lines:
        print(re.sub('To a Ship Cured', 'To a/his Ship Cured', line), file = f, end = '')

def panoptes_extract(w_id, major, minor, export_csv):
  runit([
    'panoptes_aggregation', 'extract',
    f'{args.exports}/{export_csv}',
    f'{args.output_dir}/Extractor_config_workflow_{w_id}_V{major}.{minor}.yaml',
    '-d', args.output_dir,
    '-o', w_id
    ],
    f'{args.output_dir}/extract_{w_id}.log'
  )

def strip_processed(w_id, views, extraction_name, logname):
  runit([
    './strip_processed.py',
    '-t', views,
    extraction_name
    ],
    f'{args.output_dir}/{logname}_{w_id}.log'
  )

def clean_extraction(w_id, ztype, extraction_name):
  if ztype == workflow_defs['definitions']['TEXT_T']['type']:
    runit(['./clean_extraction.py', extraction_name, w_id], f'{args.output_dir}/postextract_{w_id}.log')
    shutil.move(extraction_name, f'{extraction_name}.original')
    shutil.copy(f'{extraction_name}.cleaned', extraction_name)

def panoptes_reduce(w_id, major, minor, ztype, extraction_name):
  result = runit([
    'panoptes_aggregation', 'reduce',
    '-F', 'last',
    '-d', args.output_dir,
    '-o', w_id,
    extraction_name,
    f'{args.output_dir}/Reducer_config_workflow_{w_id}_V{major}.{minor}_{ztype}_extractor.yaml'
    ],
    f'{args.output_dir}/reduce_{w_id}.log'
  )

def panoptes(w_id, w_data):
  major, minor = get_version(w_data['version'])

  ztype = w_data['ztype']['type']
  export_csv = w_data['export']
  extraction_name = f'{args.output_dir}/{ztype}_extractor_{w_id}.csv'

  panoptes_config(w_id, major, minor)
  config_fixups(w_id, major, minor)

  panoptes_extract(w_id, major, minor, export_csv)

  strip_processed(w_id, 'tranches/empty_views.csv', extraction_name, 'strip_identity_tranform_test')
  subprocess.run(['diff', '-q', extraction_name, f'{extraction_name}.new'], check = True, capture_output = True)
  shutil.copyfile(extraction_name, f'{extraction_name}.full')
  
  strip_processed(w_id, 'tranches/views.csv', extraction_name, 'strip_seen')
  shutil.copyfile(f'{extraction_name}.new', extraction_name)

  clean_extraction(w_id, ztype, extraction_name)

  panoptes_reduce(w_id, major, minor, ztype, extraction_name)


def main():
  exit_code = 0
  procs = []

  parse_args()
  tranche_info()

  with open(args.workflow_defs) as f:
    global workflow_defs
    workflow_defs = yaml.load(f, Loader = yaml.Loader)

  for w_id, w_data in workflow_defs[args.workflows].items():
    p_name = f'panoptes-wid-{w_id}'
    p = Process(target = panoptes, name = p_name, args = (w_id, w_data))
    p.start()
    if args.verbose:
      print(f'Launched {p_name} as pid {p.pid}')
    procs.append(p)

  for p in procs:
    p.join()
    if p.exitcode != 0: exit_code = p.exitcode
    print(f'{p.name} completed with exit code {p.exitcode}')
  sys.exit(exit_code)

  print('''
All done, no errors
Suggested next invocations:
./aggregate.py --timing --outdir ${tranchedir} -r ${outdir} --uncertainty -t 0.5
cp ${tranchedir}/views_joined.csv tranches/views.csv
git add tranches
git commit -m'Latest data extraction'
''')
main()
