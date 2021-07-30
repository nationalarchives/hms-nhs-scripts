#!/bin/bash
args=(-S -u -r testinput -o joined_0.5.csv -t 0.5)

diff -s extracttest/test18617_stdout.golden <(./clean_extraction.py extracttest/test18617.csv 18617) || { echo FAIL; exit 1; }
diff -s extracttest/test18617.golden.csv extracttest/test18617.csv.cleaned || { echo FAIL; exit 1; }

diff -s extracttest/test18619_stdout.golden <(./clean_extraction.py extracttest/test18619.csv 18619) || { echo FAIL; exit 1; }
diff -s extracttest/test18619.golden.csv extracttest/test18619.csv.cleaned || { echo FAIL; exit 1; }

rm -rf output/; mkdir output; ./aggregate.py ${args[@]} && diff -s golden_0.5.csv output/joined_0.5.csv && ./coverage.pl ${args[@]} && echo PASS || { echo FAIL; false; }

echo 'If you want to run code coverage tool:'
echo coverage run --branch --source=. ./aggregate.py ${args[@]}
echo coverage html
echo open htmlcov/index.html
