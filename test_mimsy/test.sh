#!/bin/bash

PASSCOUNT=0
FAILCOUNT=0

function diff_test {
  local testcase="$1"
  shift
 ../mimsify.py "$@" -o output/"$testcase".csv input/"$testcase".csv && \
    diff -q expected/"$testcase".csv output > /dev/null
  if [[ $? -eq 0 ]]; then echo PASS; ((PASSCOUNT++))
  else echo "FAIL diff_test $testcase $@"; ((FAILCOUNT++))
  fi
}

function err_test {
  local testcase="$1"
  shift
  local failmsg="FAIL err_test $testcase $expected $@"
  ../mimsify.py "$@" -o output/"$testcase".csv input/"$testcase".csv 2>&1 | diff - expected/"$testcase".err
  if [[ $? -ne 0 ]]; then echo "$failmsg"; ((FAILCOUNT++)); return; fi
  diff -q expected/"$testcase".csv output > /dev/null
  if [[ $? -ne 0 ]]; then echo "$failmsg"; ((FAILCOUNT++)); return; fi
  echo PASS; ((PASSCOUNT++))
}

BASEDIR="`dirname $0`"
cd $BASEDIR
mkdir output || { echo "`realpath ${BASEDIR}`/output already exists: please remove and retry" 2>&1; exit 1; }

diff_test good_page #test basic functionality. this also tests that blanks are accepted in "port sailed out of" when "volume" is 1
err_test  unresolveds_rej #test that unresolveds are rejected. this one fails, so it is fine that there is a trailing space and no terminating line ending
diff_test unresolveds_acc --unresolved #test that unresolveds are correctly handled when we accept them
diff_test two_pages --unresolved #test two pages together (we use unresolved here just to let us combine the previous tests into two pages)
err_test  bad_blanks_vol_1 #make sure that a blank outside of "port sailed out of" is picked up in vol 1
err_test  bad_port_vol_1 #make sure that non-blank "port sailed out of" with vol 1 raises a warning
err_test  bad_blanks #make sure that blanks anywhere in not-vol-1 raise a warning
diff_test auto_page --autoresolved #check that autoresolved correctly adds autoresolved fields. Also tests that ; conversion and non-reporting of expected blank field works correctly
diff_test skip_yas --skip "years at sea"
diff_test skip_no --skip "admission number"
diff_test skip_victualled --skip "number of days victualled"
diff_test skip_two --skip "date of entry" "quality"
diff_test skip_neighbours --skip "age" "place of birth"

#If everything passes, test the whole of phase one. The reference output has not been
#checked for correctness, the point of this one is just to catch inadvertent changes.
if [[ $FAILCOUNT -eq 0 ]]; then err_test phase_one --unresolved; fi

echo "Passed $PASSCOUNT of $((PASSCOUNT + FAILCOUNT)) tests"
exit $FAILCOUNT
