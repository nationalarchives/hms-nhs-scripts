## Mimsifier Tests ##

To run: `./test.sh`

If the `output/` directory already exists beside this script then you will need to
delete it.

These tests check out various functions of the mimisifer that we might actually use.

They just compare known-good outputs against current outputs, and check messages on
stdout and stderr. If all tests pass, it finishes by comparing the
result of running the mimsifier on the full output of phase one, as produced by a
particular run of aggregate.py.

To create a new test:
* Put an input file in the `inputs/` directory, for example `inputs/newtest.csv`
* Put an expected output file in the `expected/` directory, for example `expected/newtest.txt`
* Put an expected messages file in the `expected/` directory, for example `expected/newtest.err`
  * If no messages are expected, make this an empty file. This way, the test will fail on unexpected output.
* Add a `diff_test` line to `test.sh`, for example `diff_test newtest`. Command-line arguments to `mimsifier.py` can follow this. See `test.sh` for examples.
  * This new line must go before the line that runs `phase_one` if the `FAILCOUNT` is 0

`test.sh` expects that all files have the appropriate filename extensions, as in the examples above.
