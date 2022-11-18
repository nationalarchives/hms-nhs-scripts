#!/usr/bin/env python3
#Example incantation: ./misc_scripts/workflow_versions.py ./exports/[[:digit:]]*

import csv
import sys
from collections import Counter

for export_fnam in sys.argv[1:]:
  print(export_fnam)
  with open(export_fnam, 'r') as export_file:
    reader = csv.DictReader(export_file)
    counted_versions = Counter([x['workflow_version'] for x in reader])
    print('\n'.join([f'{k:>10}: {v:>8} instances' for k, v in counted_versions.items()]))
