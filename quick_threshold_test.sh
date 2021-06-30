#!/bin/bash

for x in 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9; do
  ./aggregate.py launch_workflows -v -1 -o joined_${x}.csv -t $x &
done

wait

for x in output/joined*.csv; do
  disagreements=`csvtool namedcol Problems $x | grep isagreements | wc -l`
  complete=`csvtool namedcol Problems $x | grep -c '^$'`
  total=`cat $x | wc -l`; total=$((total - 1))
  justblanks=`csvtool namedcol Problems $x | grep -c '^Blank(s)$'`
  echo "$x: $disagreements disagreements + $justblanks blanks + $complete complete = $total rows ($((100 * disagreements / total))% disagreement)"
done

