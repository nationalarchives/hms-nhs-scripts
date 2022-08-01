#!/bin/bash

SEPARATOR='^'
while getopts "s:" x; do
  case "$x" in
    s) SEPARATOR="$OPTARG";;
    *) echo "Bad args"; exit 1;;
  esac
done
shift $((OPTIND-1))

if [ x$1 == x ]; then
  make separator="$SEPARATOR"
else
  make clean
  make outdir="$1" separator="$SEPARATOR"
fi
if [ $? -ne 0 ]; then
  echo 'Make failed' >&2
  exit 1
fi
cat <<EOF
Quick breakdown of fit of the output transcription to the golden transcriptions.

KEY
Length       Number of lines in the golden file
Left         Number of lines in the diff showing a left-hand difference (i.e. in the golden file. Counts lines beginning with <)
Right        Number of lines in the diff showing a right-hand difference (i.e. in the output file. Counts lines beginning with >)
Unrec        Number of lines with right-hand difference that are visibly unreconciled (counts right-hand lines containing $SEPARATOR)
Blank        Number of lines with right-hand difference that are blank (counts right-hand lines with no non-whitespace content)
Invisible    Number of right-hand differences, minus the number of unrec and blank lines.
             In other words, the number of disagreements with the golden file that are silently accepted.
Invisible %  100 * Invisible/Length

EOF
echo 'Length  Left  Right  Unrec  Blank  Invisible  Invisible %    Field'
for golden in GOLDEN_*; do
  base="${golden#GOLDEN_}"
  output="OUTPUT_${base}"
  diff $golden $output > "diff_${base}"
  left="`grep  '^<' diff_${base} | wc -l`"
  right="`grep '^<' diff_${base} | wc -l`"
  right_unreconciled="`grep '^>' diff_${base} | grep -F $SEPARATOR | wc -l`"
  right_blank="`grep '^>' diff_${base} | sed 's/^> //' | csvtool col 3 - | grep '^[[:blank:]]*$' | wc -l`"
  length="`cat $golden | wc -l`"
  invisible=$((right - right_unreconciled - right_blank))
  percent=`echo "100 * $invisible / $length" | bc`
  printf '%6u  %4u  %5u  %5u  %5u  %9u  %11u    %s\n' $length $left $right $right_unreconciled $right_blank $invisible $percent $base
done
