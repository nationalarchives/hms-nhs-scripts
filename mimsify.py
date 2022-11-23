#!/usr/bin/env python3

import re
import csv
import sys
import yaml
import argparse
from datetime import datetime

LEGAL_FRACTIONS = [
  '08',
  '17',
  '25',
  '33',
  '42',
   '5',
  '58',
  '67',
  '75',
  '83',
  '92'
]

def normalize(row):
  normalized_row = {}
  for field in FIELDS:
    row[field] = row[field].strip()

    #sanity check for expected-blank "port sailed out of"
    if field == 'port sailed out of' and row['volume'] == '1':
      if row[field] != '':
        sys.stderr.write(f'Warning: non-blank "{row[field]}" in "port sailed out of" in volume 1, at row {row_count + 1} of {args.input}\n')

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
        sys.stderr.write(f'Error: unresolved "{field}" at line {row_count + 1} of {args.input}\n')
        sys.exit(1)
    else:
      if field == 'date of entry' or field == 'date of discharge': #convert dates to expected format
        if row[field] != '':
          try:
            tmp = datetime.strptime(row[field], '%b %d %Y')
            normalized_row[field] = tmp.strftime('%d-%m-%Y')
          except ValueError:
            sys.stderr.write(f'Error: bad date format "{row[field]}" in "{field}" at line {row_count + 1} of {args.input}. Should be like "Apr 01 1800".\n')
            normalized_row[field] = row[field]
        else:
          normalized_row[field] = ''
      #Clean up "just fix" formatting in years_at_sea. Warn of non-standard values
      elif field == 'years at sea':
        input_subfields = [x.strip() for x in row[field].split(';')]
        if len(input_subfields) > 2:
          sys.stderr.write(f'Error: too many "{field}" sub-fields in "{row[field]}" at line {row_count + 1} of {args.input}.\n')
          normalized_row[field] = row[field]
        elif len(input_subfields) == 1:
          sys.stderr.write(f'Error: "{field}" value for "merchant" or "navy" missing in "{row[field]}" at line {row_count + 1} of {args.input}.\n')
          normalized_row[field] = row[field]
        else:
          output_subfields = []
          for subfield in input_subfields:
            try: float(subfield)
            except ValueError:
              sys.stderr.write(f'Error: bad number format "{subfield}" in "{row[field]}" ("{field}") at line {row_count + 1} of {args.input}.\n')
              output_subfields.append(subfield)
              continue
            if '.' in subfield:
              integer, fraction = subfield.split('.')
              original_fraction = fraction #for error messsage
              if fraction == '50': fraction = '5'
              if not fraction in LEGAL_FRACTIONS:
                sys.stderr.write(f'Error: illegal fraction ".{original_fraction}" in "{row[field]}" ("{field}") at line {row_count + 1} of {args.input}.\n')
                sys.stderr.write(f'       Fraction must be one of: .{", .".join(LEGAL_FRACTIONS)}.\n')
              x = f'{int(integer)}.{fraction}' #fraction is string-formatted above
              output_subfields.append(x)
            else: output_subfields.append(f'{int(subfield)}')
          normalized_row[field] = '; '.join(output_subfields)
      else:
        normalized_row[field] = row[field]

    #This is mainly for years at sea but also applies to Autoresolved
    if ';' in normalized_row[field]:
      normalized_row[field] = normalized_row[field].replace(';', ':')
      #We expect to see semicolons in years at sea and Autoresolved.
      #Elsewhere, they might actually be in the original text, so warn about these cases.
      #With a different "mimsy separator" we would not have this problem
      if field != 'years at sea' and field != 'Autoresolved':
        sys.stderr.write(f'Warning: replaced semicolons in "{row[field]}" in "{field}" at row {row_count + 1} of {args.input}\n')
  return normalized_row

def next_page():
  if page_count -1 == args.pages:
    print(f'Stopped at {page_count - 1} pages')
    sys.exit(0)
  target.write(f'DSH/{row["volume"]}/{row["page"]},')
  return row['volume'], row['page']
      
def next_row():
  normalized_row = normalize(row)
  target.write(bytes(args.field_separator, 'utf-8').decode('unicode_escape').join([f'{field}, {normalized_row[field]}' for field in FIELDS]))
  target.write(bytes(args.row_separator, 'utf-8').decode('unicode_escape'))


parser = argparse.ArgumentParser()
parser.add_argument('workflow_set',
                    help = 'Label for set of workflows to process. See workflow.yaml. "phase1" and "phase2" are good values.')
parser.add_argument('--workflow_defs',
                    default = 'workflow.yaml',
                    help = 'File defining the workflows (default: workflow.yaml)')
parser.add_argument('--input', '-i',
                    default = 'output/joined.csv',
                    help = 'Input file (default: output/joined.csv)')
parser.add_argument('--output', '-o',
                    default = 'output/mimsy.txt',
                    help = 'Output file (default: output/mimsy.txt)')
parser.add_argument('--skip', '-s',
                    nargs = '*',
                    default = [],
                    help = 'A field to skip. For example, --skip "years at sea" will leave out the "years at sea" field. Use --list to see all fields. Note that it is important to enquote the field names if they contain spaces.')
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
parser.add_argument('--field_separator',
                    default = '; ',
                    help = 'Separator printed at end of each field (default: ";")')
parser.add_argument('--row_separator',
                    default = ';',
                    help = 'Separator printed at end of each row (default: ";")')
args = parser.parse_args()

with open(args.workflow_defs) as f:
  FIELDS = [x['name'] for x in yaml.load(f, yaml.Loader)[args.workflow_set]['workflows'].values()]
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
    sys.stderr.write(f'Error: exception while parsing row {row_count + 1} of {args.input}\n\n')
    raise
