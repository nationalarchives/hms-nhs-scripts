import re
import csv
import sys

writer = csv.writer(sys.stdout, quoting = csv.QUOTE_NONNUMERIC, lineterminator = '\n')
with open(sys.argv[1], 'r') as f:
  for row in csv.reader(f, quoting = csv.QUOTE_NONNUMERIC):
    writer.writerow(map(lambda cell: re.sub('^<No best guess>\n-{10}\n', '', cell) if isinstance(cell, str) else int(cell), row))
