#!/bin/bash
#bc trick: https://stackoverflow.com/a/451204

for x in 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9; do
  ./aggregate.py launch_workflows -v -1 -o joined_${x}.csv -t $x "$@" &
done

wait

for x in output/joined*.csv; do
  disagreeing_rows=$(printf '%6d'  `csvtool namedcol Problems $x | grep isagreements | wc -l`)
  disagreeing_cells=$(printf '%6d' `csvtool namedcol Problems $x | grep isagreements | sed 's/^.* \?\([[:digit:]]\+\) disagreements$/\1/' | paste -s -d+ - | bc`)
  complete=$(printf '%6d' `csvtool namedcol Problems $x | grep -c '^$'`)
  total_rows=`cat $x | wc -l`; total_rows=$(printf '%6d' $((total_rows - 1)))
  total_cells=$(printf '%6d' $((total_rows * 13)))
  justblanks=$(printf '%6d' `csvtool namedcol Problems $x | grep -c '^Blank(s)$'`)
  disagreement_rows=$(printf  '%3d' $((100 * disagreeing_rows / total_rows)))
  disagreement_cells=$(printf '%3d' $((100 * disagreeing_cells / total_cells)))
  echo -e "$x:\t$disagreeing_rows disagreements + $justblanks blanks + $complete complete = $total_rows rows (Disagreement rows/cells: $disagreement_rows% / $disagreement_cells%)"
done

