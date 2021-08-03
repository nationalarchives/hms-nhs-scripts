#!/bin/bash

#Quick and dirty -- this is a Pandas job really
for x in "$@"; do
  mapfile -t < <(csvtool namedcol subject_data $x | cut -f 1 -d : | sed 1d | sed 's/^....//' | sed 's/..$//' | sort | uniq)
done

for x in ${MAPFILE[@]}; do
  grep "^${x}," exports/hms-nhs-the-nautical-health-service-subjects.csv | grep -o 'Filename.*jpeg""}",'
done
