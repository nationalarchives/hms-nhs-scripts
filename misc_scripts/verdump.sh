#!/bin/bash
for x in `ls exports/*.csv | grep -v -- '-subjects.csv$' | grep -v -- '-workflows.csv$'`; do
  echo -n "$x "
  echo `csvtool namedcol workflow_id $x | sed 1d | sort | uniq` | grep --color '.*'
  for y in `csvtool namedcol workflow_version $x | sed 1d | sort -g | uniq`; do
    echo -n "$y: "; csvtool namedcol metadata,workflow_version $x | \
      grep -m1 ",${y}$" | \
      grep -o 'started_at[^,]*' | sed 's/.*":""//' | sed 's/""$//'
  done
  echo
done
