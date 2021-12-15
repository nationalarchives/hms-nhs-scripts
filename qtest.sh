#!/bin/bash
args=(-S -u -b -o QTEST.csv -t 0.5)

./strip_processed.py -s .cleaned -t extracttest/testtranche_views.csv extracttest/testtranche_input.csv || { echo FAIL; exit 1; }
diff -s extracttest/testtranche.golden.csv extracttest/testtranche_input.csv.cleaned || { echo FAIL; exit 1; }

diff -s extracttest/test18617_stdout.golden <(./clean_extraction.py extracttest/test18617.csv 18617) || { echo FAIL; exit 1; }
diff -s extracttest/test18617.golden.csv extracttest/test18617.csv.cleaned || { echo FAIL; exit 1; }

diff -s extracttest/test18619_stdout.golden <(./clean_extraction.py extracttest/test18619.csv 18619) || { echo FAIL; exit 1; }
diff -s extracttest/test18619.golden.csv extracttest/test18619.csv.cleaned || { echo FAIL; exit 1; }

diff -s extracttest/test18612_stdout.golden <(./clean_extraction.py extracttest/test18612.csv 18612) || { echo FAIL; exit 1; }
diff -s extracttest/test18612.golden.csv extracttest/test18612.csv.cleaned || { echo FAIL; exit 1; }

diff -s extracttest/test18621_stdout.golden <(./clean_extraction.py extracttest/test18621.csv 18621) || { echo FAIL; exit 1; }
diff -s extracttest/test18621.golden.csv extracttest/test18621.csv.cleaned || { echo FAIL; exit 1; }

mkdir -p output

rm -f output/{views_,}QTEST.csv
./aggregate.py ${args[@]} -r testdata/baseline && diff -qs testdata/golden/golden_QTEST.csv output/QTEST.csv && diff -qs testdata/golden/golden_views_QTEST.csv output/views_QTEST.csv || { echo FAIL; exit 1; }

#Round2 gives input as if views_QTEST.csv had been applied. The result should be output as before, but with the complete=True rows missing from QTEST.csv.
#views_QTEST.csv should be exactly the same as before.
rm -f output/QTEST.csv #Deliberately keep the previous views files
./aggregate.py ${args[@]} -r testdata/repeat_row_tranche && diff -qs testdata/golden/golden_2_QTEST.csv output/QTEST.csv && diff -qs testdata/golden/golden_views_QTEST.csv output/views_QTEST.csv || { echo FAIL; exit 1; }

#Round3 adds another completed row. QTEST.csv should be as before: it happens that that completed row shows up the exact same way with these settings.
#views_QTEST.csv should be as before, but with the new row showing complete=True
rm -f output/QTEST.csv #Deliberately keep the previous views file
./aggregate.py ${args[@]} -r testdata/additional_row_tranche && diff -qs testdata/golden/golden_2_QTEST.csv output/QTEST.csv && diff -qs testdata/golden/golden_views_2_QTEST.csv output/views_QTEST.csv || { echo FAIL; exit 1; }

#Round4 goes back to the beginning, but now we are only reading complete pages
#With the original data, this will result in both an empty QTEST.csv file and an empty views_QTEST.csv file
page_args=(-S -o QTEST.csv -t 0.5)
rm -f output/{views_,}QTEST.csv
./aggregate.py ${page_args[@]} -r testdata/baseline && diff -qs testdata/golden/golden_3_QTEST.csv output/QTEST.csv && diff -qs testdata/golden/golden_views_3_QTEST.csv output/views_QTEST.csv || { echo FAIL; exit 1; }

#Continue by making one page almost, but not quite, complete. Output should be the same.
rm -f output/QTEST.csv #Deliberately keep the previous views file
./aggregate.py ${page_args[@]} -r testdata/incomplete_page_tranche && diff -qs testdata/golden/golden_3_QTEST.csv output/QTEST.csv && diff -qs testdata/golden/golden_views_3_QTEST.csv output/views_QTEST.csv || { echo FAIL; exit 1; }

#Then complete the page. Now we should get it in the output and in the views file.
rm -f output/QTEST.csv #Deliberately keep the previous views file
./aggregate.py ${page_args[@]} -r testdata/complete_page_tranche && diff -qs testdata/golden/golden_4_QTEST.csv output/QTEST.csv && diff -qs testdata/golden/golden_views_4_QTEST.csv output/views_QTEST.csv || { echo FAIL; exit 1; }

#Repeat, with the effect of that views file simulated. views file should be unchanged, output should be empty again.
rm -f output/QTEST.csv #Deliberately keep the previous views file
./aggregate.py ${page_args[@]} -r testdata/repeat_complete_page_tranche && diff -qs testdata/golden/golden_3_QTEST.csv output/QTEST.csv && diff -qs testdata/golden/golden_views_4_QTEST.csv output/views_QTEST.csv || { echo FAIL; exit 1; }

#Complete one more page. Both outputs should now contain that page.
rm -f output/QTEST.csv #Deliberately keep the previous views file
./aggregate.py ${page_args[@]} -r testdata/additional_page_tranche && diff -qs testdata/golden/golden_5_QTEST.csv output/QTEST.csv && diff -qs testdata/golden/golden_views_5_QTEST.csv output/views_QTEST.csv || { echo FAIL; exit 1; }

rm -f output/{views_,}QTEST.csv
./coverage.pl ${args[@]} -r testdata/baseline && echo PASS || { echo FAIL; false; }

echo 'If you want to run code coverage tool:'
echo coverage run --branch --source=. ./aggregate.py ${args[@]}
echo coverage html
echo open htmlcov/index.html
