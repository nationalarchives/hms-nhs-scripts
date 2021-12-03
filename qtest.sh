#!/bin/bash
args=(-S -u -b -r testinput -o QTEST.csv -t 0.5)

diff -s extracttest/test18617_stdout.golden <(./clean_extraction.py extracttest/test18617.csv 18617) || { echo FAIL; exit 1; }
diff -s extracttest/test18617.golden.csv extracttest/test18617.csv.cleaned || { echo FAIL; exit 1; }

diff -s extracttest/test18619_stdout.golden <(./clean_extraction.py extracttest/test18619.csv 18619) || { echo FAIL; exit 1; }
diff -s extracttest/test18619.golden.csv extracttest/test18619.csv.cleaned || { echo FAIL; exit 1; }

diff -s extracttest/test18612_stdout.golden <(./clean_extraction.py extracttest/test18612.csv 18612) || { echo FAIL; exit 1; }
diff -s extracttest/test18612.golden.csv extracttest/test18612.csv.cleaned || { echo FAIL; exit 1; }

diff -s extracttest/test18621_stdout.golden <(./clean_extraction.py extracttest/test18621.csv 18621) || { echo FAIL; exit 1; }
diff -s extracttest/test18621.golden.csv extracttest/test18621.csv.cleaned || { echo FAIL; exit 1; }

rm -f output/QTEST.csv; mkdir -p output; ./aggregate.py ${args[@]} && diff -qs golden_QTEST.csv output/QTEST.csv && ./coverage.pl ${args[@]} && echo PASS || { echo FAIL; false; }

echo 'If you want to run code coverage tool:'
echo coverage run --branch --source=. ./aggregate.py ${args[@]}
echo coverage html
echo open htmlcov/index.html
