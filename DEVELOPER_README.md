This README describes the functioning of the scripts in some detail. It is intended for developers.

Square brackets following bullet points contain the names of relevant functions.

See also the [issues associated with this project](https://github.com/orgs/nationalarchives/projects/23/views/1).

## `extract.py` ##

`extract.py` is the first script in the chain. It drives the [Panoptes aggregation scripts](https://pypi.org/project/panoptes-aggregation/) to prepare the transcriptions for processing, including reconciliation of those scripts. It also does some data cleaning ahead of the reconciliation step, sometimes driving other scripts ([strip_processed.py](#strip_processedpy), [pick_volumes.py](#pick_volumespy) and [clean_extraction.py](#clean_extractionpy), below).

`extract.py` parallelizes a great deal of its operations. Nonetheless, it can take a few hours to run: the reconciliation step, in particular, is time consuming and so imposes a lower bound on the time that `extract.py` takes to run.

`extract.py` is run on a particular phase of the project (`phase1` or `phase2`) as defined in `workflow.yaml`. These phases correspond to the two phases of the HMS NHS Zooniverse project.

The key steps in `extract.py` are:

1. Generate information that can be used for data reproduction [`tranche_info`]
2. Generate a file of metadata about each page in the Admissions Registers (the "subjects", in Zooniverse terms) [A step in `main`]
3. Run `panoptes_aggregation` in `config` mode to generate configurations for each workflow [`panoptes_config`]. Separate configurations are produced for each version of the workflow used in the current phase.
4. Standardise labels in dropdowns (one of the dropdowns sometimes has a slightly different string for one of its options) [`config_fixups`]
5. Confirm that configurations for different versions of the same workflow are identical. This will be important when we get to reduction. [`config_check_identity`]
6. Run `panoptes_aggregation` in `extract` mode to extract classifications from each version of each workflow into a standard CSV file format. Where there are multiple versions of a workflow, concatenate these together. [`panoptes_extract`]
7. Strip out any rows that have already been processed in a previous run, as recorded in `tranches/views.csv`. `tranches/views.csv` is currently empty, so this is a NOP. See [tranches](#tranches) for more on the incomplete "tranche" functionality, and [strip_processed.py](#strip_processedpy) for more on the script that does the row-stripping. [`strip_processed`]
8. Remove any rows that come from an Admissions Registers volume that is not included in the currrent phase. [`pick_volumes`]
9. Clean up data in rows according to the data cleaning rules given in [DATA_README.md](DATA_README.md#cleaning). See [clean_extraction.py](#clean_extractionpy) for more on the script that does the cleaning. [`clean_extraction`]
10. Run `panoptes_aggregation` in `reduce` mode to reconcile transcriptions into a single value. [`panoptes_reduce`]
11. Perform some sanity checks on the subject metadata. Information about this is printed to the terminal, rather than stored in a file. [A step in `main`]

By default, `extract.py` also creates a directory named for the minute in which the script was run, such as `tranches/202210201634_GMT` (YYYYMMDDhhmm_tz format). This stores some files which can be helpful for reproducibility of a given run: see [Outputs](#outputs), below, for more on these.

### Sub-scripts ###

`extract.py` drives other scripts. Apart from `panoptes_aggregation`, which comes from `pip`, we have a tool for managing reading the data in tranches, a tool for picking transcriptions from particular Admissions Register volumes, and a tool for cleaning data.

#### `strip_processed.py` ####

This is a part of the incomplete "tranche" functionality (see [tranches](#tranches)). It reads `tranches/views.csv` to find out which records have enough views to be considered complete and the `*_extractor_*.full.csv` file to identify all available transcriptions. It then produces a `*_extractor_*.stripped.csv` file, removing transcriptions which belong to completed records. This has the effect of excluding them from the rest of this process, saving us from re-generating the same data over and over again.

By default, it sorts the output by classification_id and task number.

#### `pick_volumes.py` ####

This script discards transcriptions from volumes that do not belong to the current phase. It reads the `*_extractor_*.stripped.csv` file and writes a corresponding `*_extractor_*.vols.csv` file.

#### `clean_extraction.py` ####

`clean_extraction.py` cleans up the transcriptions of text type before they are passed to the reducer. For dropdowns there are really no cleanups that it would make sense to do.

`clean_extraction.py` takes a heuristic approach, applying rules such as converting everything to lower-case and transforming common transcription errors into their likely correct form (for example, changing "hill navy" to "HM Navy"). The most exact way to understand the cleaning rules is to read `clean_extraction.py`, but they are also summarised in [DATA_README.md](DATA_README.md#cleaning).

This script also looks out for likely references to other admissions and logs them.

The cleaned transcriptions are written to `*_extractor_*.cleaned.csv`.

### Outputs ###

`extract.py` produces several outputs for each input Zooniverse workflow export.

#### Files in `extraction/` ####

The following table lists the files produced for `admission number`. Similar files exist for all of the other cases, although if the data is of dropdown type then `dropdown` will replace `text` as appropriate in each file name.

File | Description | Produced by (including step under [extract.py](#extractpy), above) |
--- | --- | ---
`subjects_metadata.csv` | Data about the pages of the Admissions Registers ("subjects", in Zooniverse terms) | Step 2
`Reducer_config_workflow_18611_V3.1_text_extractor.yaml` | A configuration file used by the `reduce` mode of `panoptes_aggregation`. | `config` mode of `panoptes_aggregation in step 3.
`Extractor_config_workflow_18611_V3.1.yaml` |  A configuration file used by the `extract` mode of `panoptes_aggregation`. | `config` mode of `panoptes_aggregation in step 3.
`Task_labels_workflow_18611_V3.1.yaml` | A configuration file used other modes in `panoptes_aggregation`. | `config` mode of `panoptes_aggregation in step 3 under [extract.py](#extractpy), above. Modified in step 4.
`config_18611_V3.1.log` | Terminal output of `panoptes_aggregation` in `config` mode | Step 3
`text_extractor_18611_V3_1.csv` | The extraction of the transcriptions from version 3.1 of workflow 18611. | `extract` mode of `panoptes_aggregation` in step 6 under [extract.py](#extractpy), above.
`extract_18611_V3.1.log` | Log output of `panoptes_aggregation` in `extract` mode | Step 6
`text_extractor_18611.full.csv` | Original output from running `panoptes_aggregation` in `extract` mode. Should be identical to `text_extractor_18611_V3_1.csv`. Where we are using more than one workflow version, this file will be the concatenation of the extractions for each individual workflow version. | Step 7
`text_extractor_18611.stripped.csv` | Result of removing previously-processed rows from `text_extractor_18611.full.csv`. If `tranches/views.csv` contains no completed rows then this file should be identical to `text_extractor_18611.full.csv` | `strip_processed.py` in step 7
`strip_identity_transform_test_18611.log` | Terminal output of `strip_processed.py` when performing a test identity transform on `text_extractor_18611.full.csv` | Step 7
`strip_seen_18611.log` | Terminal output of `strip_processed.py` when removing previously-processed rows from `text_extractor_18611.full.csv` | Step 7
`text_extractor_18611.vols.csv` | Result of removing from `text_extractor_18611.stripped.csv` all rows for volumes that do not belong to the currenct phase. | Step 8
`pick_volumes_18611.log` | Terminal output of `pick_volumes.py` when removing from `text_extractor_18611.stripped.csv` all rows from volumes that do not belong to the current phase. | Step 8
`text_extractor_18611.cleaned.csv` | The extracted data immediately after cleaning. | `clean_extraction.py` in step 9
`postextract_18611.log` | Terminal output of `clean_extraction.py`. This will include possible cross-references in the original Admission Registers, though the current means of detecting them appears to be hopelessly imprecise (many false positives). | Step 9
`text_extractor_18611.csv` | The final extractions after all processing | Step 9 (it happens to be a copy of `text_extractor_18611.cleaned.csv`)
`text_reducer_18611.csv` | The reduction (also known as reconciliation) of the transcriptions | `reduce` mode of `panoptes_aggregation` in step 10
`reduce_18611.log` | Terminal output of `panoptes_aggregation` in `reduce` mode. This is the main input to `aggregate.py`, though it will also refer to `text_extractor_18611.csv` and `text_extractor_18611.csv.new` | Step 10

#### Files in `tranches/<YYYYMMDDhhmm_TZ>` ####

File | Description
--- | ---
`generated_by` | Tells you which commit of which git repository the script came from
`lines.txt` | Tells you how many lines were in each input file from the Zooniverse exports. I think that if you truncate later downloads of the files to the same length then you should have the same inputs, though this is not definitely confirmed.
`last_classifications.txt` | Lists the last few classification ids in each of those files, to give some ability to check that the truncated file looks right.

### Future Work ###

This script might be better implemented as a Makefile (with the `tranche_info` function turned into a Python script to be driven by the Makefile).


## `aggregate.py` ##

`extract.py` produces output per-column of the Admissions Registers. `aggregate.py` collects these outputs together into rows, recreating the original content of the Admission Registers as reconciled from the volunteers' transcriptions. It also makes decisions about which reconciled transcriptions can be accepted as-is and which should be passed on for human checking.

Because most of the work is done in the `main` function, the square brackets also give the content of surrounding calls to `track`. For example, [`main:Processing workflows - Generating output`] means that the relevant code appears somewhere between the calls `track('* Processing workflows')` and `track('* Generating output')`/track].

`aggregate.py` processes one column at a time, as follows:
1.  Read the relevant reconciled data as produced by `extract.py` [`main: Processing workflows - Generating output`]
2.  Count how many times each item of data has been transcribed, including repeat transcriptions by the same individual [`main: Processing workflows - Generating output`, `count_text_views`]
3.  Drop all items of data that have insufficient views to be processed yet [`main: Processing workflows - Generating output`]
4.  Handle conflicts (see [Conflict Handling](#conflict-handling), below). [`main: Processing workflows - Generating output`]
5.  Join the data columns into *data rows* [`main: Generating output - Data joined`]
6.  Join the views counts into *views rows* equivalent to the *data rows* [`main: Data joined - Views joined`]
7.  Log to `incomplete_rows.csv` all rows that had fields dropped in step 3. [`main: Views joined - Removed fields logged`]
8.  Log to `nonunique.csv` all rows that appear to have more than one transcription by the same individual. [`main: Removed fields logged - Transcriptionisms identified`]
9.  Check the data for patterns that indicate transcriber uncertainty. Flag any *data rows* with such data as bad. [`main: Removed fields logged - Transcriptionisms identified`, `has_transcriptionisms`]
10. Compute the image URL, subject id, volume and page for each page of data in the *data rows*. [`main: Transcriptionsisms identified - Subjects identified`]
11. For volume 1 only, blank out any cells in the *data rows* that had `port sailed out of`, flagging this as an autoresolution. Record all removed values in `ports_removed.csv`. (Volume 1 does not have a `port sailed out of` column.) [`main: Subjects identified - "Port sailed out of" fixed up`]
12. Add a flag to the *views rows* to record whether the row is complete (in other words, that every field in the row has at least the minimum number of views). [`main: "Port sailed out of" fixed up - Complete views identified`]
13. Add a `Problems` column to the *data rows*, recording whether any fields in the row are blank. [`main: Complete views identified - Badness identified`]
14. Drop all *data rows* that are part of an incomplete page (where the page contains at least one incomplete row, or is missing at least one row). [`main: Badness identified - Incompletes removed`]
15. Add text to the `Problems` column for every *data row* that has been flagged as containing unresolved fields. [`main: Incompleted removed - Unresolved identified`]
16. Add an `Autoresolved` column to the *data rows*. In each row, this lists the fields that were autoresolved (a reconciled answer was accepted, but the (cleaned) transcriptions were not identical). [`main: Unresolved identified - Autos identified`]
17. Final steps [`main: Autos identified - All done`]
    * Sort the completed *data rows* by volume and page
    * Generate `lenchecker.csv` from the *data rows*
      * This can be used by `maxcolwidth.sh` to check for problems with hard limits in Excel or Google Sheets
    * Generate `joined.csv` from the *data rows*
      * Includes a stamp of "reproducibility information" at the end of each row
      * And a special header to force Google Sheets to detect UTF-8 encoding
    * Generate `views.csv` from the *views rows* merged with any old data from a previous tranche (see [Tranches](#tranches), below).


### Conflict Handling ###

This section gives an overview of the conflict handling step.

The below sub-headings describe special processing for each case. In *all* cases, the following things happen:
* If the (cleaned) transcriber input was not identical, flag the field as autoresolved
* If the threshold for accepting an auto-correction was not passed, flag the field as bad
* If the field was flagged as bad for any reason, format the transcription options for manual correction

#### Zooniverse `dropdown` type ####

* Determine whether there was unanimous selection and, if not, whether there is a consensus resolution >= the `--dropdown` threshold (default: 66% agreement) [`main`, `category_resolver`]
* Decode the result into human-readable labels [`main`]

#### Zooniverse `text` type ####

For the Zooniverse text type (which is everything that is not a dropdown) [`text_resolver`] does the following:
  * If all transcriptions are missing, return an empty string
  * Identify the "real" type and process as described under the following sub-headings.
  
##### `years at sea` #####

* Flag as bad unless the field has two numbers separated by a `;` [`years_at_sea`]
* Determine whether there was unanimous selection and, if not, whether there is a consensus resolution >= the `--dropdown` threshold (default: 66% agreement) [`category_resolver`]
* If we have a valid result then convert it to standard format (2 integer digits + any decimal part, navy service first, merchant service second, separated by `; `. For example: `00; 01.5`) [`years at sea`]

##### Any other number field #####

* If the field has a surprising format, flag as bad [`number_resolver`]
* If the input contains any non-integers, flag as bad [`number_resolver`]
* Determine whether there was unanimous selection and, if not, whether there is a consensus resolution >= the `--dropdown` threshold (default: 66% agreement) [`category_resolver`]

##### Date fields #####

* If the field has a surprising format, flag as bad [`date_resolver`]
* If the field contains a 0 for day, month or year, flag as bad [`date_resolver`]
* If `dateutil.parser.parse` with `dayfirst = True` cannot parse the field, flags as bad [`date_resolver`]
* Determine whether there was unanimous selection and, if not, whether there is a consensus resolution >= the `--dropdown` threshold (default: 66% agreement) [`category_resolver`]
* If we have a valid result then convert it to standard format (`dd mmm YYYY`) (for example, `20 Aug 1843`).

##### String (arbitrary text) fields #####

This is the only case where the `--text_threshold` value is used. This number is not really to be thought of as a percentage, but is rather just some sort of indication of "certainty" in a rather undefined fashion.

* If `data.consensus_score` divided by `data.number_views` < `--text_threshold` (defaults to 0.9, but we usually run with 0.3 at time of writing) then flag as bad
* If `data.consensus_score` is equal to `data.number_views` then all transcribers wrote exactly the same thing. If this is not the case, flag as autoresolved.


### Notable Functions ###

Other than the functions referenced above, there are some that are used in certain conditions that you will see scattered around the code.

* `uncertainty`: Used when the script runs with `--uncertainty` to look for indications of uncertainty in the pre-reconciled volunteer inputs. This is off by default: in the default case, we rely on post-reconciliation checks for indications of user uncertainty, allowing the reconciler to elide indications of uncertainty in some cases.
* `flow_report`: This outputs information about paths taken through the code. At one time this was to be used with `misc_scripts/coverage.sh` to check code coverage, but the relevant code has bit-rotted.
* `track`: This outputs information to stdout. When the script runs with `--timing`, this will include information about time elapsed since the last call to `tract`.

### Handling Problems ###

The reporting in the `Problems` field of `joined.csv` is a bit imprecise. It does not attempt to count how many fields are blank and gives only a lower bound for the number of unresolved fields.

The reason that we presently give a minimum number of fields is, to the best of my memory, just that there can be more than one reason why a given field is bad and the way that we count this is quite unsophisticated. It should be possible to fix this by looking at cases of the `bad` dictionary but, for reasons that I no longer recall, this was not so straightforward last time I took a look at it.

In both cases, scanning back along the row should make it very obvious both which fields are blank and which are unresolved -- unresolved fields are in an obvious multiline format, while blank fields are, well, blank. That said, sometimes an unresolved field has been flagged because it has characters in it that may indicate transcriber uncertainty -- these might be harder to spot.

It could be useful to have a separate `unresolved` column for each of the fields. This would allow checking for problems in different kinds of data, which may well differ. (The same applies to `autoresolved`).

### Inputs ###

File | Description
--- | ---
`extraction/{text,dropdown}_reducer_186*.csv` | The reconciled data as produced by `extract.py`
`workflow.yaml` | Basic data about the workflows: how they relate to the Zooniverse project's output, what types of data they contain.
`exports/hms-nhs-the-nautical-health-service-subjects.csv` | Used only to look up the URL of the Zooniverse copy of original Admissions Register page images.
`extraction/text_extractor_18*.csv` | Used to get a true count of the number of views of each text field. The number of views as reported in the matching `text_reducer_18*.csv` file do not include any empty classifictions, but "empty" is a legal input in this project.
`extraction/text_extractor_18*.csv.new` | Used alongside the matching `extraction/text_extractor_18*.csv` to sanity-check that the post-`strip_processed.py` transformations did not lose any classifications.
`output/views_joined.csv` | If the (incomplete) "read in tranches" feature (see [tranches](#tranches), below) is used then this file, output from a previous run, is also an input.

### Outputs ###

File | Description
--- | ---
`incomplete_rows.csv` | All rows containing fields that have not had sufficient views to be included.
`incomplete_pages.csv` | All rows of pages that are incomplete, because some of the rows are incomplete and/or because some rows are entirely missing. Rows listed in `incomplete_rows.csv` may or may not also appear in this file -- it depends upon whether enough data got through to fill in some part of the page in which the row from `incomplete_rows.csv` appears.
`joined.csv` | The CSV file containing all of the volunteer-described data, for hand-checking prior to Mimsification.
`lenchecker.csv` | A crude way to check for columns too wide for Google Sheets or for Excel. For use with `maxcolwidth.sh`
`nonunique.csv` | Count of repeat classifications (total classifications minus classificiations by unique user ids) for each cell of text data. (A repeat classification is where the same user has made an additional transcription of data that that user had already transcribed.) This is an incomplete feature, so would need some checking to be sure that it is accurate, and some work to add the count for dropdowns. Also, be aware that we cannot accurately distinguish individuals as they are sometimes anonymous.
`ports_removed.csv` | A dump of content removed from the `port sailed out of` column for volume 1, which does not have that column!
`views_joined.csv` | A count of the number of times that each cell has been viewed, and a flag recording whether each row has enough views to be considered complete. Used for the unfinished "read in tranches" functionality (see [tranches](#tranches), below).
`interims/*` | When the script is run with `--dump_interims` it dumps a lot of intermediate state into this directory. These can be helpful when debugging and are best understood with reference to the code that produces them.

### Future Work ###

This script is crying out to be refactored into smaller pieces. This could just mean breaking it up into functions, but I would suggest actually splitting it into a number of scripts so that we effectively apply a series of filters, each with its own output. As well as each being individually more comprehensible, this would make it easy to develop the script by re-running only the part being worked on, and would make it more parallelisable (for example, via a Makefile). The downside would be some small cost in extra reading and writing and perhaps redundant validation, but I think that would be more than paid for in the development effort saved.

## `mimsify.py` ##

<!-- TODO: Fill this in after it gets finalised. It is pretty simple, won't take long. -->

## `testing` ##

This directory contains scripts used for testing. As with [`misc_scripts`](#misc_scripts), below, this has bit-rotted as the focus has been upon getting something functional in place rather than on following best practice with respect to testing. However, this would be the starting point for restoring the testing. Some notes follow on roughly what these scripts are supposed to do.

### `gentestinput.sh` ###

This appears to take just testing-relevant columns from the reducer output.

### `coverage.pl` ###

This script is used in conjunction with `aggregate.py --flow_report` to test code coverage. There are likely more sophisticated tools available. At time of writing, both `aggregate.py` and `coverage.pl` would require checking to make sure that `aggregate.py` is reporting on all code paths, and that `coverage.pl` is aware of all reporting strings.

### `qtest.sh` ###

This script runs "quick" tests on various aspects of the functionality. By default it runs some checks on `./strip_processed.py`, `./clean_extraction.py` and `./aggregate.py`, including using `coverage.pl` to check the test coverage. It takes the following options:
* `-E`: Skip checks on `./strip_processed.py` and `clean_extraction.py`
* `-u`: Run a check for the `aggregate.py --uncertainty`
* `-t`: Run checks on the tranching feature

### `test_mimsy.sh` ###

This is a recent creation and is **not** bit-rotted.

To run: `./testing/test_mimsy.sh`

If the `testing/output/mimsy/` directory already exists then you will need to delete it.

These tests check out various functions of the mimisifer that we might actually use.

They just compare known-good outputs against current outputs, and check messages on stdout and stderr. If all tests pass, it finishes by comparing the result of running the mimsifier on the full output of phase one, as produced by a particular run of aggregate.py.

To create a new test:
* Put an input file in the `testing/inputs/mimsy/` directory, for example `testing/inputs/mimsy/newtest.csv`
* Put an expected output file in the `testing/expected/mimsy/` directory, for example `testing/expected/mimsy/newtest.txt`
* Put an expected messages file in the `testing/expected/mimsy/` directory, for example `testing/expected/mimsy/newtest.err`
  * If no messages are expected, make this an empty file. This way, the test will fail on unexpected output.
* Add a `diff_test` line to `test_mimsy.sh`, for example `diff_test newtest`. Command-line arguments to `mimsifier.py` can follow this. See `test_mimsy.sh` for examples.
  * This new line must go before the line that runs `phase_one` if the `FAILCOUNT` is 0

`test_mimsy.sh` expects that all files have the appropriate filename extensions, as in the examples above.

## `misc_scripts/` ##

This directory contains scripts produced during development. They tend to do single-use things for a purpose that I needed at the time and may not work outside of my environment. They may also have bit-rotted. They are kept here just in case they might be useful in the future.

Some of the scripts in here may be more generally useful, though:

* `sourceme.sh`: This can be sourced in a bash shell to provide various useful functions. It is still likely to be somewhat specific to my own setup, though.
* `maxcolwidth.sh`: This exmaines `lenchecker.csv` to see if its columns are too wide for certain spreadsheets.
* `quick_threshold_test.sh`: This runs `aggregate.py` with a range of `--text_threshold` values, reporting on the proportion of problems found at each setting. For want of a better place, its output goes in `testing/output/qtt` -- you will need to delete this directory before launching a run of the script. This script may have bit-rotted by now, but a quick once-over suggests that it may still work as intended.

## `subjects.py` ##

In normal usage, this script provides a function to generate a cache of subject metadata, and another to provide a dataframe which reads the cache to give access to that data to its callers -- usually, callers will be mapping Zooniverse subject id to volume and page number, and to a URL of the page image as seen by transcribers. The creation function also returns dataframes that can be used to perform integrity checks upon the subject metadata, and `extract.py` does perform some such checks. This script can also be run in a standalone mode to dump some information about the subjects used in the project (cases where there are multiple subject ids from a single page, cases where data about a subject was missing from the exported subjects file and has instead been filled in from data provided in `workflow.yaml`.

## `workflow.yaml` ##

`workflow.yaml` provides data to other scripts. Its main purpose is to provide information about the columns in the Admissions Registers: the workflow id and version(s) that map to that column, the data type of the column in both Zooniverse and Python/Pandas terms, and the name that we use to refer to the workflow.

`workflow.yaml` has expanded to also provide the name of the `...-workflows.csv` file used by `panoptes_aggregation` and the `...-subjects.csv` file used by `subjects.py` to get the subject metadata. It can also provide information for any subjects that are missing from `...-subjects.csv`.

In an ideal world, the scripts would be entirely generic and `workflow.yaml` (and perhaps other files) would allow us to provide data to process transcriptions from arbitrary Zooniverse projects.

## Abandoned Things ##

### Testing ###

Most of the testing, as noted above, is bit-rotted and not currently of much use.

### Tranches ###

The "skip things we have processed before" functionality may well work, but is not sufficiently tested, so I do not recommend using it without doing some testing work first. The general idea is that we keep a record of what we have completely processed before so that, the next time we extract data, we do not need to re-extract the same data over again.

Note that the data under `tranches` produced by `extract.py:tranche_info` is important for reproducibility and should be committed with each production run.
