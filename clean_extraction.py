#!/usr/bin/env python3
import pandas as pd
import re
import sys

adminrefs = set()

def clean_18617(text):
  global adminrefs

  #chomp whitespace (Panoptes extraction doesn't do this)
  result = text.strip()

  #Title case (all words start with a capital letter, rest is lowercase)
  #Sometimes there is a sentence instead of a placename, so just guess that anything
  #longer than 4 words is a sentence and leave it alone.
  #Some common words can be mis-cased by this, so we fix them up.
  #Perhaps we should just standardise case entirely for the string-matching benefits
  if len(result.split()) < 5:
    result = result.title()
    
    #Some case cleanup for exceptions

    #Make very short words lower case
    result = re.sub(r'\bA\b', 'a', result)
    result = re.sub(r'\ba\.', 'A.', result) #Don't lowercase A if it is followed by a full stop
    result = re.sub(r'\bOf\b', 'of', result)
    result = re.sub(r'\bOn\b', 'on', result)
    result = re.sub(r'\bDe\b', 'de', result)
    result = re.sub(r'\bAt\b', 'at', result)
    result = re.sub(r'\bThe\b', 'the', result)
    
    #Fix known abbreviations
    result = re.sub(r'\bUs\b', 'US', result)
    result = re.sub(r'\bUsa\b', 'USA', result)
    result = re.sub(r'\bSs\b', 'SS', result)
    result = re.sub(r'\bSb\b', 'SB', result)
    result = re.sub(r'\bNs\b', 'NS', result)
    result = re.sub(r'\bNb\b', 'NB', result)
    result = re.sub(r'\bNa\b', 'NA', result)
    result = re.sub(r'\bAb\b', 'AB', result)
    result = re.sub(r'\bNj\b', 'NJ', result)

    #Common exceptions
    result = re.sub(r'\bUpon\b', 'upon', result)

    #Lower case following certain punctuation
    result = re.sub(r'\.\.\.[A-Z]', lambda x: x[0].lower(), result)
    result = re.sub(r'[\[\]{}\'][A-Z]', lambda x: x[0].lower(), result)

    #Upper case following certain punctuation
    result = re.sub(r'\b[Ll]\'[a-z]', lambda x: x[0].upper(), result) #e.g. L'Orient
    result = re.sub(r'-[a-z]', lambda x: x[0].upper(), result) #e.g. West-Ham

  #First character is always upper case
  result = re.sub(r'^\'?[a-z]', lambda x: x[0].upper(), result) #The 'possible quote' at the beginning is just to deal with 'New Hampshire, which was annoying me.

  #Check for possible reference to another admission.
  #If it is there, log it and strip it
  number = re.search(r'\d+$', result)
  if number:
    adminrefs.add(int(number[0]))
    result = re.sub(r'\s*\d+', '', result)
    result = re.sub(r'\s+[Nn]o\.?$', '', result)

  #Drop everything to the right of a comma (inclusive of the comma)
  result = re.sub(r'\s*,.*$', '', result)

  return result

def main():
  df = pd.read_csv(sys.argv[1], keep_default_na = False)
  df['data.text'] = df['data.text'].map(clean_18617)
  df.to_csv(path_or_buf = f'{sys.argv[1]}.cleaned', index = False)
  print('Possible crossrefs:', sorted(adminrefs))

main()
