#!/bin/bash

PASSCOUNT=0
FAILCOUNT=0
VERBOSE=0
SKIP_PHASE_ONE_TEST=0

function diff_test {
  local testcase="$1"
  shift
  if [[ $VERBOSE -eq 1 ]]; then
    echo "../mimsify.py "$@" -o output/mimsy/"$testcase".txt input/mimsy/"$testcase".csv 2>&1 | diff -q - expected/mimsy/"$testcase".err > /dev/null 2>&1"
  fi
  ../mimsify.py "$@" -o output/mimsy/"$testcase".txt input/mimsy/"$testcase".csv 2>&1 | diff -q - expected/mimsy/"$testcase".err > /dev/null 2>&1
  if [[ $? -ne 0 ]]; then
    echo "FAIL (bad messages) $testcase $@"
    ((FAILCOUNT++))
    return
  fi
  diff -q expected/mimsy/"$testcase".txt output/mimsy/"$testcase".txt > /dev/null
  if [[ $? -eq 0 ]]; then
    echo "PASS  diff_test $testcase $@"
    ((PASSCOUNT++))
  else
    echo "FAIL (bad output) diff_test $testcase $@"
    ((FAILCOUNT++))
  fi
}

while getopts "vP" x; do
  case "$x" in
    v) VERBOSE=1;;
    P) SKIP_PHASE_ONE_TEST=1;;
    *) echo "Bad args"; exit 1;;
  esac
done
shift $((OPTIND-1))

BASEDIR="`dirname $0`"
mkdir "$BASEDIR"/output/mimsy || { echo "`realpath ${BASEDIR}`/output/mimsy already exists: please remove and retry" 2>&1; exit 1; }
cd $BASEDIR

diff_test good_page #test basic functionality. this also tests that blanks are accepted in "port sailed out of" when "volume" is 1
diff_test unresolveds_rej #test that unresolveds are rejected. this one fails, so it is fine that there is a trailing space and no terminating line ending
diff_test unresolveds_acc --unresolved #test that unresolveds are correctly handled when we accept them
diff_test two_pages --unresolved #test two pages together (we use unresolved here just to let us combine the previous tests into two pages)
diff_test bad_blanks_vol_1 #make sure that a blank outside of "port sailed out of" is picked up in vol 1
diff_test bad_port_vol_1 #make sure that non-blank "port sailed out of" with vol 1 raises a warning
diff_test bad_blanks #make sure that blanks anywhere in not-vol-1 raise a warning
diff_test auto_page --autoresolved #check that autoresolved correctly adds autoresolved fields. Also tests that ; conversion and non-reporting of expected blank field works correctly
diff_test skip_yas --skip "years at sea" #make sure that we can skip the field that this option was added for
diff_test skip_no --skip "admission number" #make sure nothing surprising happens if we skip the first field
diff_test skip_victualled --skip "number of days victualled" #make sure nothing surprising happens if we skip the final field
diff_test skip_two --skip "date of entry" "quality" #make sure we can skip more than one field
diff_test skip_neighbours --skip "age" "place of birth" #make sure nothing surprising happens if we remove adjacent columns
diff_test dates #make sure we get errors for various "wrong format" dates, and that we cope OK with odd spacing. This also serves as a test that stripping of all input fields words.
diff_test yas

#If everything passes, test the whole of phase one. The reference output has not been
#checked for correctness, the point of this one is just to catch inadvertent changes.
if [[ $FAILCOUNT -eq 0 && $SKIP_PHASE_ONE_TEST -ne 1 ]]; then diff_test phase_one --unresolved; fi

echo "Passed $PASSCOUNT of $((PASSCOUNT + FAILCOUNT)) tests"
exit $FAILCOUNT
