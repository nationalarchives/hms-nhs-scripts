#!/bin/bash

checkdir="`dirname $0`/../${1:-output}"
checkfile="${2:-joined.csv}"
grep -q '~' "${checkdir}/${checkfile}"
if [ $? -eq 0 ]; then
  echo 'Found a ~ in a real field!' >&2
  echo '(This breaks my assumption that I can use ~ as a separator for purpose of calculating column width)'
  false
else
  #Following https://unix.stackexchange.com/a/92149
  high=`sed 's/~/\n/g' "${checkdir}"/lenchecker.csv | awk '{ print length }' | sort -g | tail -1`
  if [ ${high} -gt 49000 ]; then
    echo "${high} is greater than Google Sheets max of 50,000" >&2
    echo '{The Sheets max is 50,000, but giving myself a buffer, just in case)' >&2
  elif [ ${high} -gt 250 ]; then
    echo "${high} is greater than Excel max of 250" >&2
    echo '(The Excel max is 255, but giving myself a small buffer, just in case)' >&2
    false
  else
    echo "${high} should be OK as max col width"
  fi
fi
