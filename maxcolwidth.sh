#!/bin/bash

grep -q @ output/joined.csv
if [ $? -eq 0 ]; then
  echo 'Found an @ in a real field!' >&2
  echo '(This breaks my assumption that I can use @ as a separator for purpose of calculating column width)'
  false
else
  #Following https://unix.stackexchange.com/a/92149
  high=`sed 's/@/\n/g' output/lenchecker.csv | awk '{ print length }' | sort -g | tail -1`
  if [ ${high} -gt 250 ]; then
    echo "${high} is greater than 250" >&2
    echo '(The Excel max is 255, but giving myself a small buffer, just in case)' >&2
    false
  else
    echo "${high} should be OK as max col width"
  fi
fi
