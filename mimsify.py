#!/usr/bin/env python3

import re
import csv
import sys
import argparse
from datetime import datetime

FIELDS = [
  "admission number",
  "date of entry",
  "name",
  "quality",
  "age",
  "place of birth",
  "port sailed out of",
  "years at sea",
  "last services",
  "under what circumstances admitted (or nature of complaint)",
  "date of discharge",
  "how disposed of",
  "number of days victualled"
]

def normalize(row):
  normalized_row = {}
  for field in FIELDS:
    row[field] = row[field].strip()

    #sanity check for expected-blank "port sailed out of"
    if field == 'port sailed out of' and row['volume'] == '1':
      if row[field] != '':
        sys.stderr.write(f'Warning: non-blank "{row[field]}" in "port sailed out of" in volume 1. At row {row_count + 1} of {args.input}\n')

    if not args.no_blanks_warnings:
      if row[field] == '':
        expected_blank = (row['volume'] == '1' and field == 'port sailed out of') or field == 'Autoresolved'
        if not expected_blank:
          sys.stderr.write(f'Warning: blank in "{field}" at row {row_count + 1} of {args.input} (volume {row["volume"]}, page {row["page"]})\n')

    if re.match(r'[^\n\r]*[\n\r]+-{10}[\n\r]+', row[field]): #identify an unresolved field
      if args.unresolved:
        #Unresolved arguments are permitted, so flatten them out and remove some of the detail
        #The thinking here is to make it as simple as possible, but we could choose to leave in some of
        #this information, at the cost of making the entry more tricky to understand/search.
        sub = re.sub(r'.*[\n\r]+-{10}[\n\r]+', '', row[field]) #remove the header with the "most likely" resolution
        sub = re.sub(r' @\d+$', '', sub, flags = re.MULTILINE) #remove the count information
        sub = re.sub(r'[\n\r]+', ' OR ', sub) #replace newlines with OR
        normalized_row[field] = sub
      else:
        sys.stderr.write(f'Unresolved "{field}" at line {row_count + 1} of {args.input}\n')
        sys.exit(1)
    else:
      if field == 'date of entry' or field == 'date of discharge': #convert dates to expected format
        if row[field] != '':
          try:
            tmp = datetime.strptime(row[field], '%b %d %Y')
            normalized_row[field] = tmp.strftime('%d-%m-%Y')
          except ValueError:
            sys.stderr.write(f'Warning: bad date format \"{row[field]}\". Should be like "Apr 01 1800".\n')
            normalized_row[field] = row[field]
        else:
          normalized_row[field] = ''
      else:
        normalized_row[field] = row[field]

    #This is mainly for years at sea but also applies to Autoresolved
    if ';' in normalized_row[field]:
      normalized_row[field] = normalized_row[field].replace(';', ':')
      #We expect to see semicolons in years at sea and Autoresolved.
      #Elsewhere, they might actually be in the original text, so warn about these cases.
      #With a different "mimsy separator" we would not have this problem
      if field != 'years at sea' and field != 'Autoresolved':
        sys.stderr.write(f'Warning: replaced semicolons in "{row[field]}" (See "{field}" at row {row_count + 1} of {args.input})\n')
  return normalized_row

def next_page():
  if page_count -1 == args.pages:
    print(f'Stopped at {page_count - 1} pages')
    sys.exit(0)
  target.write(f'DSH/{row["volume"]}/{row["page"]},')
  return row['volume'], row['page']
      
def next_row():
  normalized_row = normalize(row)
  target.write('; '.join([f'{field}, {normalized_row[field]}' for field in FIELDS]))
  target.write(';')


parser = argparse.ArgumentParser()
parser.add_argument('input',
                    nargs = '?',
                    default = 'output/joined.csv',
                    help = 'Input file (default: output/joined.csv)')
parser.add_argument('--output', '-o',
                    default = 'output/mimsy.txt',
                    help = 'Output file (default: output/mimsy.txt)')
parser.add_argument('--skip', '-s',
                    nargs = '*',
                    default = [],
                    help = 'A field to skip. For example, --skip "years at sea" will leave out the "years at sea" field. Use --list to see all fields.')
parser.add_argument('--list', '-l',
                    action = 'store_true',
                    help = 'Apply any skips, list fields and exit')
parser.add_argument('--unresolved',
                    action = 'store_true',
                    help = 'Permit (and flatten) unresolved fields')
parser.add_argument('--no-blanks-warnings',
                    action = 'store_true',
                    help = 'Do not warn about unexpected blanks')
parser.add_argument('--autoresolved',
                    action = 'store_true',
                    help = 'Include information about which fields were autoresolved')
parser.add_argument('--pages',
                    type = int,
                    help = 'Stop after PAGES pages (for faster testing)')
args = parser.parse_args()

FIELDS = list(filter(lambda x: x not in args.skip, FIELDS))
if args.list:
  print('\n'.join(FIELDS))
  sys.exit(0)
if args.autoresolved:
  FIELDS.append('Autoresolved')

with open(args.input) as source:
  #QUOTE_MINIMAL is the default, but let's be explicit about what we expect
  #This is on the basis that Google Sheets appears to export as QUOTE_MINIMAL
  reader = csv.DictReader(source, quoting = csv.QUOTE_MINIMAL, strict = True)
  page_count = 1
  row_count = 1

  try:
    with open(args.output, 'w') as target:
      row = next(reader)
      current_volume, current_page = next_page()
      next_row()

      for row in reader:
        #This conditional relies upon volumes and pages appearing in order,
        #which is how aggregate.py happens to organise output/joined.csv.
        if row['page'] != current_page or row['volume'] != current_volume:
          page_count += 1
          target.write('\n')
          current_volume, current_page = next_page()
        else: target.write(' ') #so that the rows are separated the same way as the fields are
        row_count += 1
        next_row()

      target.write('\n') #Put in a trailing newline, if only to make testing simpler
  except:
    sys.stderr.write(f'Exception while parsing row {row_count + 1} of {args.input}\n\n')
    raise
