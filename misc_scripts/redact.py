#!/usr/bin/env python3
#For example: ./redact.py exports/[[:digit:]]*
#The following comments explain how to do testing on the output with various shell commands.

##Can extract all the embedded shell in these comments something like this: sed -n '4,/^import /p' misc_scripts/redact.py  | grep '^#' | sed 's/^.//' | grep -v '^#'
##Confirm that no "potentially sensitive" data has survived the process:
#for column in user_name user_id user_ip; do for x in redacted/*; do echo "*** $column $x"; grep -Fxf <(csvtool namedcol $column exports/${x#*/} | sed 1d | grep -v '^$') <(csvtool namedcol $column $x | sed 1d); done; done
##Any hits will appear following the output line telling you that the test is happening.
##This is taking the pre-redacted data from each "sensitive" column of the original file and searching for an exact match on *any* row in the equivalent column of the output file. You can see what hits look like by replacing 'exports/${x#*/}' with '$x'.
##Note that sometimes there might be a coincidental hit -- we might randomly generate a number that matches a number that already existed

##Confirm that the metadata column has been removed
#head -qn1 redacted/* | grep metadata

##Confirm that the outputs are otherwise identical
##You may need to change the exact column indices, but these are correct at time of writing. The important thing is to leave out the "sensitive" columns in both input and output, and to leave out the metadata column in the input.
#for x in redacted/*; do echo -n "${x}: "; diff -qs <(csvtool cols 1,5-10,12- exports/${x#*/}) <(csvtool cols 1,5- $x); done
##All files should be identical

##As an added paranoia check, confirm that the name column always looks like (name|anon):<6 digits>
#for x in redacted/*; do csvtool namedcol user_name $x | sed 1d | grep -vx '\(name\|anon\):[[:digit:]]\{6\}'; done
##This should produce no output

##And then to check that there are 'name:' users:
#for x in redacted/*; do echo -n "$x: "; csvtool namedcol user_name $x | sed 1d | grep -l '^name:'; done
##This should name each file, followed by (standard input)

##And that there are 'anon:' users
#for x in redacted/*; do echo -n "$x: "; csvtool namedcol user_name $x | sed 1d | grep -l '^anon:'; done
##This should name each file, followed by (standard input)

##And that the user_id and user_ip columns are exactly 6 digits or blank
#for y in redacted/*; do for x in user_id user_ip; do echo "*** $y ($x)"; csvtool namedcol $x $y | sed 1d | grep -vx '[[:digit:]]\{6\}' | grep -vx '^$'; done; done
##Any hits will appear following the output line telling you that the test is happening.

##And that the user_id and user_ip columns contain at least one 6-digit case
#for y in redacted/*; do for x in user_id user_ip; do echo -n "$y ($x) "; csvtool namedcol $x $y | sed 1d | grep -lx '[[:digit:]]\{6\}'; done; done
##Each line should end with (standard input)

##And that the user_id and user_ip columns contain at least one blank case
#for y in redacted/*; do for x in user_id user_ip; do echo -n "$y ($x) "; csvtool namedcol $x $y | sed 1d | grep -lx '^$'; done; done
##Any hits will appear following the output line telling you that the test is happening.
##Each line should end with (standard input)

##Look at 500 random lines from each file (except for the annotations and subject_data columns, to make it managable)
#for x in redacted/*; do echo; echo $x; echo; { csvtool cols 1-10,13- $x | sed 1d | shuf -n 500; } done | less

import pandas as pd
import os
import sys
import string
import secrets

#For debugging
#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)
#pd.set_option('display.expand_frame_repr', None)

def redact(row):
  def random_name():
    failcount = 0
    while failcount < 10:
      #TODO: 6 digits provides enough unique strings for Engaging Crowds. Should really be calculated.
      rnd = ''.join(secrets.choice(string.digits) for i in range(6))
      if f'{prefix}{rnd}' in redact.identities.values():
        failcount += 1
      else:
        return rnd
    raise Exception('10 failures to generate a unique pseudonym. Try increasing the number of characters in the pseudonyms.')

  uid = row['user_id'] #this seems to magically transform back to float -- perhaps because the effect of dtype = str in the read_csv is to convert to an object, not a string?
  if pd.isna(uid):
    #Anonymous user -- use the ip addr as the uid, so that all
    #classifications from the apparent-same IP addr get the same pseudonym
    uid = row['user_ip']
    prefix = 'anon:'
  else:
    prefix = 'name:'

  if uid in redact.identities:
    user_name = redact.identities[uid]
    prefix = user_name[:5]
    pseudonym = user_name[5:]
  else:
    pseudonym = random_name()
    redact.identities[uid] = f'{prefix}{pseudonym}'
    user_name = redact.identities[uid]

  if prefix == 'name:':
    return [user_name, pseudonym, '']
  elif prefix == 'anon:':
    return [user_name, '', pseudonym]
  else:
    raise Exception(f'{prefix} {user_name} {pseudonym}')
redact.identities = dict()

def main():
  try: os.mkdir('redacted')
  except FileExistsError:
    print(f"Output directory 'redacted' already exists.\nPlease delete or move it before running this script.", file = sys.stderr)
    sys.exit(1)

  for export in sys.argv[1:]:
    print(f'Redacting {export}')
    df = pd.read_csv(export, dtype = str)
    if not len(df.workflow_id.unique()) == 1:
      raise Exception(f'Too many workflow ids in {export}')
    df[['user_name', 'user_id', 'user_ip']] = df[['user_name', 'user_id', 'user_ip']].apply(redact, axis = 'columns', result_type = 'expand')
    df = df.drop('metadata', axis = 1)
    df.to_csv(f'redacted/{export[export.rfind("/"):]}', index = False)

main()
