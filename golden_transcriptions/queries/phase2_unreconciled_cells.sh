#!/bin/bash

#Sample invocation: ./golden_transcriptions/queries/phase2_unreconciled_cells.sh ./output/joined.csv

#Method is crude.
#We always output a header for unresolved cells, even when there is no best guess.
#Thus, every failure of reconciliation outputs a 10-hyphen header on its second line.
#To test that this will give us a good estimate of the number of reconciliation failures, grepped the exports directory as follows
#grep -o -- ',[^,-]*----------[^,]*,' exports/* | highlight -- ----------
#The output from this was:
#exports/10-where-from-classifications.csv:,""value"":""------------------"",
#exports/11-nature-of-complaint-classifications.csv:,""value"":""Pneumonic \u0026 Dis----------"",
#exports/8-ship-ship-or-place-of-employment-last-ship-classifications.csv:,""value"":""---------- F of Norway"",
#exports/8-ship-ship-or-place-of-employment-last-ship-classifications.csv:,""value"":""--------------- Steam Vessel,
#As there were only 4 matches, we can also see that these will not skew our numbers much, even if our regexp is imprecise enough to pick these up.

for column in \
  'nature of complaint' \
  'name' \
  'place of birth/nationality' \
  'of what port/port of registration' \
  'where from' \
  'ship/ship or place of employment/last ship' \
  'quality' \
  'creed' \
  'how disposed of' \
  'admission number' \
  'age' \
  'number of days in hospital' \
  'date of entry' \
  'date of discharge' \
;do
  echo csvsql --blanks --tables foo --query "'select count(\"${column}\") as \"${column}\" from foo where \"${column}\" regexp \"^[^\n]*\n-{10}\n\"'" "${1:-output/joined.csv}"
done | parallel --verbose -k | paste -sd ':\n'

