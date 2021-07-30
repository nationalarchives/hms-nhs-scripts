#!/bin/bash
args=(launch_workflows -S -u -r testinput -o joined_0.5.csv -t 0.5)

diff -s extracttest/test18617_stdout.golden <(./clean_extraction.py extracttest/test18617_in.csv 18617) || { echo FAIL; exit 1; }
diff -s extracttest/test18617.golden.csv extracttest/test18617_in.csv.cleaned || { echo FAIL; exit 1; }

rm -rf output/; mkdir output; ./aggregate.py && diff -qs golden.csv output/joined.csv && { echo -e '\nChecking coverage'; rm -rf output/; mkdir output; ./aggregate.py ${args[@]} && diff -s golden_0.5.csv output/joined_0.5.csv && ./coverage.pl ${args[@]} && echo PASS || false; } || { echo FAIL; false; }

echo 'If you want to run code coverage tool:'
echo coverage run --branch --source=. ./aggregate.py ${args[@]}
echo coverage html
echo open htmlcov/index.html
