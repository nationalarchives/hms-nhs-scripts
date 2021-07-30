#!/usr/bin/env python3
import pandas as pd
import re
import sys
from decimal import Decimal, ROUND_HALF_UP

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

def clean_18619(years):
  numbers = [x.strip() for x in years.split(';')]
  if len(numbers) != 2: return years

  def round_to_month(x):
    try:
      float(x)
    except ValueError:
      if re.match(r'^[oO0 ]*$', x): x = '0'
      else: return x
    parts = re.match(r'(.*)\.(.*)$', x)
    if parts:
      integer_part = parts[1]
      if len(integer_part) == 0: integer_part = 0
      else: integer_part = int(integer_part)

      decimal_part = parts[2]
      if len(decimal_part) == 0: decimal_part = '0'
      significant_digits = len(decimal_part)
      nonzero_digits = len(decimal_part.lstrip('0'))
      leading_zeros = significant_digits - nonzero_digits

      #Round to mutiple of 0.08: see https://stackoverflow.com/a/2272174, but we must convert to int and adjust divisor accordingly
      #Should avoid any float fiddliness
      decimal_part = Decimal(decimal_part)

      #Always be working in integers
      #If there is only one significant digit then the divisor would be 0.8, so multiply everything by 10
      assert significant_digits > 0 #We deal with 0-length significant digits above
      if significant_digits == 1:
        decimal_part *= 10
        significant_digits += 1

      divisor = Decimal('8')
      divisor *= Decimal(f'1{"0" * (significant_digits - 2)}')
      decimal_part /= divisor
      decimal_part = decimal_part.to_integral_value(ROUND_HALF_UP)
      decimal_part *= divisor #Requires an integer divisor
      assert decimal_part == int(decimal_part)
      decimal_part = int(decimal_part) #Should anyway be an integer at this point -- quick way to discard any '.0'

      #Figure out what to return
      if decimal_part == 0: return f'{integer_part:02}'

      decimal_part = str(decimal_part)
      #In theory the number could gain a digit 'to the left', filling in one of the zeros -- but in practice I don't believe it can happen for rounding to 0.08 in the range (>= 0, < 1). So we don't have to detect this. If it could happen then we would just have to drop a leading 0.
      #We can however gain a digit 'to the right', for example if the input is 0.5 (becomes 0.48). No special handling is required for this (unless we needed to differentiate it from gaining a digit 'to the left' -- in which case we would need to track that we changed the significant digits from 1 to 2 in the if clause above)
      decimal_part = decimal_part.rstrip('0')
      decimal_part = f'{"0" * leading_zeros}{decimal_part}'
      assert len(decimal_part) >= 1 #Because we tested for zero above
      if len(decimal_part) < 2: decimal_part += '0'
      return f'{integer_part:02}.{decimal_part}'
    else:
      assert len(x) != 0 #The regexp near the beginning will replace empty strings with 0
      return f'{int(x):02}'

  return '; '.join([round_to_month(x) for x in numbers])


def main():
  funcmap = {
    '18617': clean_18617,
    '18619': clean_18619,
  }

  for infile, cleanfunc in zip(sys.argv[1::2], sys.argv[2::2]):
    df = pd.read_csv(infile, keep_default_na = False, dtype = { 'data.text': str }, skip_blank_lines = False)
    df['data.text'] = df['data.text'].map(funcmap[cleanfunc])
    df.to_csv(path_or_buf = f'{infile}.cleaned', index = False)

  print('Possible crossrefs:', sorted(adminrefs))

main()
