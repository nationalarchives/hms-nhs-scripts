## Data Overview ##

This readme gives an overview of the data at a level that is hopefully reasonably comprehensible to non-programmers.

The data goes through a number of transformations:

1. **Acquisition**: Volunteer transcription of images of the original Admissions Registers. Each item of data must be transcribed at least 5 times.
2. **Extraction**: Transformation of the acquired data into a form that is as friendly as possible for further processing.
3. **Reconciliation**: All transcriptions of each item of data are "reconciled" into a single consensus transcription, according to an algorithm.
4. **Aggregation**: This phase is slightly misleadingly named. We aggregate all of the reconciled data together, so that the different fields are collected into rows matching the original Admissions Registers. However, we also revisit reconciliation, deciding for each reconciled field whether we are happy with the reconciliation. This results in a csv file which can be manually edited in a spreadsheet.
5. **Hand correction**: The CSV file is reviewed. Cases where we are unhappy with the automatic reconciliation can be fixed by hand. We can also perform spot checks on reconciled data to decide whether we should be more or less optimistic about how well the automatic reconciliation has worked.
6. **Mimsification**: The edited CSV file is passed through a final process, which converts the data into a form suitable for ingest into the Mimsy cataloguing system.

These steps are descibed in more detail below.

### Acquisition ###

To understand how the volunteers collected the data, either [visit the project](https://www.zooniverse.org/projects/msalmon/hms-nhs-the-nautical-health-service) while it is still live or visit the project archive, once that goes live. <!-- TODO: Link to project archive -->

This results in data for several columns of the Admissions Register. We can divide this data up into different types, as follows:

<table>
<tr align='left'><th><b>Column Name</b></th><th><b>Data Type</b></th></tr>
<tr><td>Admission number</td><td rowspan=3>Integer</td></tr>
<tr><td>Age</td></tr>
<tr><td>Number of days victualled</td></tr>
<tr><td>Date of entry</td><td rowspan=2>Date</td></tr>
<tr><td>Date of discharge</td></tr>
<tr><td>Name</td><td rowspan=5>Text</td></tr>
<tr><td>Place of birth</td></tr>
<tr><td>Port sailed out of</td></tr>
<tr><td>Last services</td></tr>
<tr><td>Under what circumstances admitted</td></tr>
<tr><td>Quality</td><td rowspan=2>Categorical (a selection from a list of pre-determined catagories)</td></tr>
<tr><td>How disposed of</td></tr>
<tr><td>Years at sea</td><td>Special: two numbers, sometimes with decimal points, separated by a semicolon.</td></tr>
</table>

### Extraction ###

Extraction takes the original volunteer transcriptions and converts them into a form that is convenient for further processing. A lot of this is fairly uninteresting (but necessary!) transformation of the data.

The most important step in extraction is cleaning, which tries to resolve common problems in the data and to put it into a standard form. Where cleaning finds a problem, we sometimes return a blank (effectively meaning that we have thrown this transcription away) and sometimes return the original, uncleaned text (meaning that we want the reducer to try its best with what has been entered): which one to choose is a bit of a judgement call and could be refined with some experimentation.

This section describes the cleaning rules as applied to the data. Of course, the most exact description of these rules is `clean_extraction.py`, the script that performs the changes.

#### All fields ####

We always strip leading and trailing whitespace from all fields, before doing any other processing.

#### All integer fields (see `clean_extraction.py:unstring_number`) ####

1. If the field consists entirely of any mix of the digit `0` and the letter `O` (regardless of case) then this is converted to a `0`.
2. If the field does not contain a number (and only a number) then the field is blanked out.

#### All date fields (see `clean_extraction.py:unstring_date`) ####

1. Runs of characters consisting of any mix of the digit `0` and the letter `O` (regardless of case) are replaced with a `0`.
2. If the field is not made up of 3 numbers separated by `-`, `/`, `.` or `=` then the field is blanked out (for example `07 - 11.1834` is accepted, `7th November 1834 or 7|11|1834` are not).
3. If any of the three numbers are 0 then put `-` characters between them and return that (for example, `07-00-1834`). The Aggregation phase will flag this as needing a manual check.
4. Call a Python function to try to parse the date as we now have it. This will try to treat the fields as `day-month-year`, but if this does not work then it will also try `month-day-year`. 2-digit years will be converted to dates in the 1900s or 2000s. It may accept other unusual date formats, so this rule feels a bit risky.
5. If the year is greater than `9999`, return to the original text.
6. If the year is lower than `1800`, return the original text.
7. If the year is greater than `1900` (but less than `9999`), change the first two digits to `18`. This is intended to deal with dates accidentally entered as being in the 1900s or 2000s, and should also fix up any 2-digit years converted by the Python function. This rule will need to be updated for phase two, which legitimately contains dates in the 1900s.
8. Convert the date into `day-month-year` format.

#### All text fields (see `clean_extraction.py:clean_text`) ####

1. Convert various short words to lower case.
   * The full list is: `A`, `Of`, `On`, `De`, `At`, `The`. `A.` is not lower-cased.
2. Various abbreviations are converted to upper case if they have been written as an upper-case character followed by lower-case characters.
   * The full list is: `Us`, `Usa`, `Ss`, `Sb`, `Ns`, `Nb`, `Na`, `Ab`, `Nj`.
3. Title words beginning `Mac` or `Mc` as if they are surnames.
   * For example, `Mackenzie` would become `MacKenzie`.
   * Note that this is not always the right thing to do.
4. Convert `Upon` to `upon`
5. Convert to lower case following certain punctuation
   * Text immediately following, with no intervening spaces, any of: `[`, `]`, `{`, `}` `'` or `...`. For ellipses, this is only works when the text is 3 (ASCII) dots -- this will not detect an actual 'ellipsis' character.
6. Convert to upper case following certain text
   * The letter immediately following, with no intervening spaces, any of: `-`, or `L'`, regardless of case, so long as it occurs at the beginning of a word. For example, `L'orient` becomes `L'Orient`, `West-ham` becomes `West-Ham`.
7. If the first character of the field is a letter, we upper case it. This also applies if the first character is a `'` and the second character is a letter.
8. Convert the phrase `hill navy` to `HM Navy`, regardless of case and the amount of whitespace.

If the text now ends with a number, that number will be logged as a possible cross-reference in the original records: this will be printed on stdout, along with the full text.

For steps 1-7, see `clean_extraction.py:normalise_case`. For step 8, see `clean_extraction.py:hill_navy`. For the cross-ref spotter, see the (currently misleadingly named) `clean_extraction.py:strip_crossref`.

#### Place of birth (see `clean_extraction.py:clean_18617`) ####

This is very similar to the [All text fields](#all-text-fields-see-clean_extractionpyclean_text), above, but the order is steps 1-7 and then step 9. Then, we remove everything from the first comma in the text onwards. Finally, we perform step 8.

#### Last services (see `clean_extraction.py:clean_18621`) ####

This performs all of the steps in [All text fields](#all-text-fields-see-clean_extractionpyclean_text), above, in the same order. Then, ignoring case, we check for the following conversions. As soon as we make one of these conversions, we stop transforming the text.

* If the text begins with `Hms Ms` or `Hms`, change it to begin with `HMS`.
* If the text begins with `Hms`, change it to begin with `HMS`.
* If the text begins with `Hcs`, change it to begin with `HCS`.

#### All dropdown fields ####

Dropdown fields are not modified by the data cleaning step.

#### Years at sea (see `clean_extraction.py:clean_18619`) ####

Years at sea is particularly complex. There will be one or two numbers, which should be separated by a semicolon. The numbers may be integers, or they may have a fraction part. If they have a fraction part then it should either represents a number of months as a fraction of a year (i.e. be a muliple of 0.083 recurring, which is 1/12), or it is a particular fraction that is used to represent a given number of months (these fractions are a little more human-friendly than multiples of 0.083 recurring).

The cleaner will accept `:`, `,` or `;` as separators. Then, whether there is one number or two, each number goes through some conversions:
1. If the data does not appear to be a number, but it is either empty or composed entirely of the digit `0` and the letter `O` (regardless of case) then it is converted to a `0`. Otherwise we leave it as is and do not do any more transformations.
2. If the number does not have a decimal point, then we do no more transformations.
3. If the number does have a decimal point and there is no integer part given, we set the integer part to 0.
4. If the decimal part is one of the "particular human-friendly fractions" we do no further conversions. Otherwise, we round to the nearest 0.08 and convert the result into the equivalent "particular human-friendly fraction".

Finally, if there is more than one number, the numbers are joined together with a semicolon between them.

### Reconciliation ###

The reconciliation step combines different transcriptions of each data item. This step is performed by the `panoptes_aggregtion` scripts, so we do not go into how it works here. However, it is perhaps worth knowing that on the `panoptes_aggregation` side, almost all fields are treated as text, regardless of whether that text happens to be sentences, numbers or dates. Only categorical data (dropdowns, from the `panoptes_aggregation` point of view) are reconciled differently.
<!-- TODO: But it would be good to have a summary of it here, so if I can find the time, look up what panoptes_aggregation does and fill this in -->

### Aggregation ###

`aggregate.py` produces many output files in the `output` directory. For most of these, see the [table of outputs](DEVELOPER_README.md#outputs-1) in the developer doc. `joined.csv`, the file produced for hand correction, is the only one described here.

If producing a production run of data, it is also worth checking `incomplete_rows.csv` and `incomplete_pages.csv`. These record data that was dropped from `joined.csv`. If either of these files is not empty then it is worth making sure that you understand why some data has not made it into the final `joined.csv`. (It is normal for these files not to be empty if you are working with an incomplete phase of the project.)

`aggregate.py` essentially just reviews the data produced by the previous stages and combines it into rows reflecting the original Admissions Registers. Data that does not yet have enough transcriptions to be accepted are dropped, as are any pages with incomplete or missing rows (these are recorded in `incomplete_rows.csv` and `incomplete_pages.csv`, respectively).

`aggregate.py` flags data that has been automatically reconciled from differing transcriptions. It also flags data that needs to be hand corrected and makes all the possibilities for that data visible. Key reasons that data may be marked for hand correction are:
* The reconciled version of the data does not meet the threshold for reconciliation of data of that type.
  * For most types we default to requiring that 66% of the individual (cleaned) transcriptions are the same to make that transcription the accepted value.
  * For text only, we default to requiring a "confidence value" of 0.9 (on a scale of 0 to 1) to make that transcription the accepted value. (But at time of writing, we usually set the mimimum confidence value to 0.3.)
* The input data is "surprising" in a way that prevents further processing
* The field is `years at sea` and is not two numbers separated by a semicolon
* Any other number field contains a non-integer
* A date contains 0 for day, month or year
* A date cannot be parsed by a standard Python library
* The reconciled data contains any of a number of patterns of characters (for example, `...`) that might indicate transcriber uncertainty.

Some data is converted to standardised formats:
* `years at sea` is converted into two numbers separated by a semicolon and a space. The integer part of the number must be two digits long. The decimal part is optional. For example, `00; 09.75`
* Dates are converted to `dd mmm YYYY` format: for example `10 Aug 1872`. We rely on a standard Python function to do the parsing, which feels dangerously imprecise (as [noted above](#all-date-fields-see-clean_extractionpyunstring_date)).

Any data that is marked for hand correction is usually put into a format to aid hand correction. If the data is marked for correction just because it appears that the transcriber was uncertain about the transcription (for example, they have put `...` in the text) then it is not put into a special format. See [README.md](README.md#correcting-joinedcsv) for more on this.

Data which passes the threshold for reconciliation, but which does not have unanimous transcriptions, is flagged as automatically resolved.

`aggregate.py` also throws away any data entered for `port sailed out of` in volume 1, because this column does not exist in volume 1. If it does this, it marks `port sailed out of` as automatically resolved in that row.

See [README.md](README.md#about-joinedcsv) for a table of fields in `joined.csv`. See [DEVELOPER_README.md](DEVELOPER_README.md#handling-problems) for more about the `Problems` field and [README.md](README.md#correcting-joinedcsv) for an overview of what the data in the Admissions Register fields looks like.

### Hand correction ###

See [README.md](README.md#correcting-joinedcsv).

### Mimsification ###

<!-- TODO: Fill this in when the mimsifier is finalised -->

## Replication ##

You can use data stored in `tranches/<YYYYMMDD_tz>` and in the `Repo`, `Commit` and `Arg` columns of `joined.csv` to reproduce data (that is, to redo exactly what was done to produce a particular set of data), or to understand exactly how the data was produced (for example, looking at the exact version of `clean_extraction.py` that produced the data to see what cleaning rules were applied).

Every row in `joined.csv` contains information about exactly which version of the code produced it and what command-line arguments were used by aggregate.py to produce it. To study the code, you just need to do a `git checkout` from the appropriate commit.

If you have followed the advice of `extract.py` to commit the latest `tranches/` directory before running aggregate.py, then the most recent commit, prior to the commit recorded in `joined.csv`, that adds a `tranches` directory will tell you which version of `extract.py` and its related scripts were used to generate the data that made up the input to `aggregate.py`. You will also know how to truncate the Zooniverse exports to produce (what I think should be) the correct inputs to `extract.py`. You should therefore be able to replicate the whole process of producing that particular `aggregate.py`. (See [DEVELOPER_README.md](DEVELOPER_README.md#files-in-tranchesyyyymmddhhmm_tz) for a bit more on the relevant files.)
