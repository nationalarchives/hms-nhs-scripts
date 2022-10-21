#!/usr/bin/env python3
import pandas as pd
import re
import sys
from decimal import Decimal, ROUND_HALF_UP
import dateutil
import datetime

adminrefs = set()

def strip(x):
  return x.strip()

def hill_navy(text):
  return re.sub(r'hill(\s*navy)\b', 'HM\g<1>', text, flags = re.IGNORECASE)

def normalise_case(text):
  result = text
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

    #Allow for Mc/Mac
    result = re.sub(r'\b(Ma?c)([a-z])', lambda x: f'{x[1]}{x[2].upper()}', result)

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
  return result


def strip_crossref(text):
  #Check for possible reference to another admission.
  #If it is there, log it and strip it
  global adminrefs
  result = text
  number = re.search(r'\b\d+\s*$', result)
  if number:
    adminrefs.add(f'{number[0]} from cell(s) reading "{text}"')
    #result = re.sub(r'\s*\d+\s*$', '', result)
    #result = re.sub(r'\s+[Nn]o\.?$', '', result)
  return result


#Place of Birth
def clean_18617(text):
  #chomp whitespace (Panoptes extraction doesn't do this)
  result = text.strip()
  result = normalise_case(result)
  result = strip_crossref(result)

  #Drop everything to the right of a comma (inclusive of the comma)
  result = re.sub(r'\s*,.*$', '', result)

  return hill_navy(result)


#Years at Sea
def clean_18619(years):
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

      decimal_part = parts[2].rstrip('0')
      if len(decimal_part) == 0: return f'{integer_part:02}'

      #Skip this part if the number is part of the 'new scheme'
      if decimal_part != '08' and \
         decimal_part != '17' and \
         decimal_part != '25' and \
         decimal_part != '33' and \
         decimal_part != '42' and \
         decimal_part != '50' and \
         decimal_part != '58' and \
         decimal_part != '67' and \
         decimal_part != '75' and \
         decimal_part != '83' and \
         decimal_part != '92':

        #Original scheme called for multiples of 0.08, so we round to that if the number is not part of the new scheme
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

        if decimal_part == 0: return f'{integer_part:02}'

        decimal_part = str(decimal_part)
        #In theory the number could gain a digit 'to the left', filling in one of the zeros -- but in practice I don't believe it can happen for rounding to 0.08 in the range (>= 0, < 1). So we don't have to detect this. If it could happen then we would just have to drop a leading 0.
        #We can however gain a digit 'to the right', for example if the input is 0.5 (becomes 0.48). No special handling is required for this (unless we needed to differentiate it from gaining a digit 'to the left' -- in which case we would need to track that we changed the significant digits from 1 to 2 in the if clause above)
        decimal_part = decimal_part.rstrip('0')
        decimal_part = f'{"0" * leading_zeros}{decimal_part}'
        assert len(decimal_part) >= 1 #Because we tested for zero above
        if len(decimal_part) < 2: decimal_part += '0'

        #Convert to the new scheme with a lookup table
        if   decimal_part == '08': decimal_part = '08' # 1 month
        elif decimal_part == '16': decimal_part = '17' # 2 months
        elif decimal_part == '24': decimal_part = '25' # 3 months
        elif decimal_part == '32': decimal_part = '33' # 4 months
        elif decimal_part == '40': decimal_part = '42' # 5 months
        elif decimal_part == '48': decimal_part = '5'  # 6 months
        elif decimal_part == '56': decimal_part = '58' # 7 months
        elif decimal_part == '64': decimal_part = '67' # 8 months
        elif decimal_part == '72': decimal_part = '75' # 9 months
        elif decimal_part == '80': decimal_part = '83' #10 months
        elif decimal_part == '88': decimal_part = '92' #11 months
        else: raise Exception('Unexpected year fraction')#If none of these conditions apply I would like to know about it

      return f'{integer_part:02}.{decimal_part}'
    else:
      assert len(x) != 0 #The regexp near the beginning will replace empty strings with 0
      return f'{int(x):02}'

  separator = re.search(r'[;:,]', years)
  if separator: separator = separator[0]
  else:
    if len(years.strip()) == 0: return ''
    else: return round_to_month(years)
  numbers = [x.strip() for x in years.split(separator)]
  if len(numbers) != 2: return years
  return ';'.join([round_to_month(x) for x in numbers])


#Last Services
def clean_18621(text):
  result = clean_text(text)

  result, count = re.subn(r'^Hms Ms\b', 'HMS', result, flags = re.IGNORECASE)
  if count != 0: return result
  result, count = re.subn(r'^Hms\b',    'HMS', result, flags = re.IGNORECASE)
  if count != 0: return result
  result, count = re.subn(r'^Hcs\b',    'HCS', result, flags = re.IGNORECASE)
  if count != 0: return result
  return result


def clean_text(text):
  return strip_crossref(hill_navy(normalise_case(strip(text))))


def unstring_number(text):
  result = strip(text)
  try: float(result)
  except ValueError:
    if re.match(r'^[oO0]*$', text): return '0'
    else: return ''
  return result

def unstring_date(text):
  text = strip(text)
  try: result = re.sub(r'[oO0]+', '0', text)
  except TypeError: return ''

  #TODO: If year is missing or badly formatted, we should be able to infer it from the volume.
  #      This would also allow us to handle dates in 'wordy' (e.g. 'July 17') format.
  #TODO: Consider just discarding the year and inferring it from the volume
  #TODO: Consider turning the character class into just 'not digit'
  #TODO: Consider trying to make a sane date string in the case that the string is duplicated (01-01-182601-01-1826)
  #TODO: Consider trying to make a sane date string if a separator is missing or some random digit
  if not re.match(r'^\d+\s*[-/\.=]\s*\d+\s*[-/\.=]\s*\d+$', result): return ''

  result = re.sub(r'\s*=\s*', '-', result)

  #Blank out if the date has any 0-component (or not-exactly-3 components)
  #In either case, give up, return the original text, and hope that the zeros get through to the aggregator, which will flag them
  #TODO: Better solutions are possible, see https://github.com/nationalarchives/hms-nhs-scripts/issues/11
  parts = [int(x) for x in re.split(r'[-/\.]', result)]
  if 0 in parts:
    return '-'.join([str(x) for x in parts]) #Just return this, preserving the zeros. The aggregator checks the candidates for those containing zeros. (The date parser, below, does not respect zero-fields.)

  #dateutil.parser.parse is thrown off by leading zeros if that results in too many digits in a field
  result = re.sub(r'-0+', '-', result)
  result = re.sub(r'^0+', '', result)

  try: result = dateutil.parser.parse(result, dayfirst = True)
  except (TypeError, ValueError): return ''
  if result.year > 9999: return text #TODO: Change to 2200
  if result.year < 1800: return text #TODO: Change to mimimum date in data set
  if result.year > 1900: result = datetime.datetime(int(f'18{str(result.year)[2:4]}'), result.month, result.day)
  return result.strftime('%d-%m-%Y') #TODO: Change to maximum date in dataset (in phase two, this will be in the early 20th century)


def main():
  funcmap = {
    '18611': unstring_number,
    '18612': unstring_date,
    '18613': clean_text,
    #'18614': dropdown, nothing to normalise
    '18616': unstring_number,
    '18617': clean_18617, #place of birth -- some special handling for extra words
    '18618': clean_text,
    '18619': clean_18619, #years at sea -- some special handling for splitting the fields and rounding to 0.08
    '18621': clean_18621, #last services -- some special handling for ship name abbreviations
    '18622': clean_text,
    '18623': unstring_date,
    #'18624': dropdown, nothing to normalise,
    '18625': unstring_number, #number or date, just strip
  }

  for infile, cleanfunc in zip(sys.argv[1::2], sys.argv[2::2]):
    df = pd.read_csv(infile, keep_default_na = False, dtype = { 'data.text': str }, skip_blank_lines = False)
    df['data.text'] = df['data.text'].map(funcmap[cleanfunc])
    df.to_csv(path_or_buf = f'{infile}.cleaned', index = False)

  print('Possible crossrefs:', adminrefs)

main()
