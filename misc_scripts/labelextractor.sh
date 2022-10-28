#!/bin/bash

for y in "$@"; do
  inputfile="`realpath $y`"
  targdir="task_labels/`basename $inputfile .yaml`"
  mkdir "$targdir" || { echo "Cannot create target dir $targdir"; exit 1; }
  (
    cd "$targdir" || { echo "Unable to enter $targdir/"; exit 1 ; }
    grep -A1 ^T "$inputfile" |
      grep -A1 selects |
      sed '/^--$/d' |
      paste -sd ' \n' |
      awk -v FS=. '{print > $1}'
    for z in T*; do #Lead with T to reduce chance of accidentally nuking the wrong location
      sed 's/.*\.\([[:digit:]]\+\)\.label.*: /\1: /' $z | sort -g > $z.txt;
      mv "$z" ".$z" #Again, hide rather than delete to reduce chance of accidental nukage
    done
  )
done
