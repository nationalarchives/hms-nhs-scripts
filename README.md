## Overview ##

These are the data processing scripts for the [Zooniverse](https://zooniverse.org) crowdsourcing project [HMS NHS: The Nautical Health Service](https://www.zooniverse.org/projects/msalmon/hms-nhs-the-nautical-health-service). HMS NHS is a [Royal Museums Greenwich](https://rmg.co.uk) project, and part of the larger [Towards a National Colletion](https://www.nationalcollection.org.uk/) project, [Engaging Crowds](https://tanc-ahrc.github.io/EngagingCrowds/).

Volunteers on HMS NHS transcribed pages from the Admissions Registers of the Dreadnought Seamen's Hospital. Each page was transcribed by multiple volunteers. These scripts reconcile the different transcriptions from each page into a single transcription. When they cannot find a reconciliation then they list all of the possibilities for a human moderator.

The general approach is to make use of Zooniverse's own [Panoptes aggregation scripts](https://pypi.org/project/panoptes-aggregation/) for the main part of the reconciliation task, with hms-nhs-scripts doing additional data cleansing and formatting the data into two output forms:

1. `joined.csv` A CSV file presenting the transcriptions row by row.
   * This file is for hand checking. Unreconciled transcriptions can be reconciled by a human moderator. Reconciled transcriptions can be spot-checked.
2. `mimsy.txt` A text file suitable for ingest into Mimsy, RMG's cataloguing system.
   * This file is generated from `joined.csv` after hand corrections.


## Example Use ##

* Clone this repository: `git clone https://github.com/nationalarchives/hms-nhs-scripts.git`
* Enter the scripts dir and create `exports` directory: `cd hms-nhs-scripts; mkdir exports`
* Install dependencies: `pip install -r requirements.txt`
* Download all necessary Zooniverse project exports:
  * `hms-nhs-the-nautical-health-service-subjects.csv` (link next to `Request new subject export` button)
  * `hms-nhs-the-nautical-health-service-workflows.csv` (link next to `Request new workflow export` button)
  * Each file listed under `Request new workflow classification export` button
* Copy the downloaded files into the new `exports/` directory.
* Extract the data from the downloads: `./extract.py phase1`. This may take a few hours. Run as `nice ./extract.py phase1` if you don't want it to dominate your computer's resources.
* Record information about the run of `extract.py` by committing that information to the repository. `extract.py` itself will tell you how to do this.
* Optionally, check `extraction/postextract_*.log` to see possible cross-references in the original input data.
* Generate `joined.csv`: `./aggregate.py -t 0.3`. This may take several minutes. `joined.csv` will appear in the `output` directory.
* Check that `joined.csv` is safe to open in certain spreadsheet software: `./misc_scripts/maxcolwidth.sh`
* Correct `joined.csv` by hand (see [Correcting joined.csv](#correcting-joinedcsv), below)
* Generate `mimsy.txt`: `./mimsify.py` (see [Generating mimsy.txt](#generating-mimsytxt), below)

<details>
<summary>
Click here for more detail
</summary>

It is a good idea to install the pip dependencies inside an isolated environment such as [virtualenv](https://pypi.org/project/virtualenv/). You can use `requirements_all.txt` instead of `requirements.txt` if you want to match the environment even more precisely.

You can get the Zooniverse project exports via the `Request new workflow classification export`, `Request new subject export` and `Request new workflow export` buttons in the `Data Exports` area of the Zooniverse Project Builder. You will need to download some different files, depending upon whether you are getting data from phase one or phase two of the project. Look at the `export` fields in `workflow.yaml` to see which files to download. Phase one uses `phase1` while phase two uses `phase2`.

`extract.py` runs the Panoptes aggregation scripts, with a few cleanup interventions from hms-nhs-scripts. The output goes in `extraction`. You can observe the cleanups by comparing `extraction/..._extractor.csv.full` with `extraction/..._extract.csv.cleaned`. At the end it will report a number of exit codes: if any of these are not 0 then an error has occurred.

`extraction/postextract_*.log` contains information about possible cross-references in the original source. You can check this by looking for the "from cell(s)" string in `text_extractor_*.csv.full`. Note that posssible cross-references are not deleted.

`aggregate.py` creates `joined.csv`, which joins the transcriptions of the separate columns into rows and reports additional information about the reconciliation process such as which columns needed automatic reconciliation and cases where automatic reconciliation failed.

At time of writing, we are running `aggregate.py` with `-t 0.3`. `-t` sets the threshold for accepting automatic resolution of text inputs. This ranges from 0 to 1, with lower numbers are more aggresive. This means that lower `-t` values will accept less "certain" resolutions as correct. These patterns are used by transcribers to indicate that they could not read some of the text.

`extract.py`, `aggregate.py` and `mimsify.py` have several options. You can find out about these by running them with `--help`.
</details>


## Correcting `joined.csv` ##

Automatic reconciliation is necessarily imperfect. You can control the "aggressiveness" of the reconciler by changing the `--text_threshold` and `--dropdown_threshold` parameters of `aggregate.py`: lower numbers are more aggressive. Greater aggression will result in more reconciled cells but also more incorrectly reconciled cells. Run with `--help` to see other options.

The recommended way to correct `joined.csv` is to open it in a spreadsheet. We used Google Sheets. You may run into some quirks if you use a different spreadsheet.

The `original` column gives you a link to the original page on Zooniverse, so that you can read it for yourself.

When correcting data, there are some formatting rules to follow:
* Dates *must* be written like `Aug 07 1872`: the month name is always the three letter abbreviation, single-digit days must have a leading zero and the year must be written with all four digits.
* Numbers *must* be written as integers, unless they are in the `years at sea` column. Numbers in `years at sea` must be written with two digits for the integer part, with the two types of service separated by `; `. For example, if the patient had spent 12 years in the Navy and 6 years and 3 months in the merchant service, this should be written `12; 06.25`.

`mimsify.py` checks for problems in its input, so if you make mistakes such as typing a date or a years at sea value in the wrong format, it will fix it if it can, and otherwise will tell you about it.

The `Problems` column tells you about missing data and fields that could not be autoresolved, or that need checking for other reasons, such as unusual inputs (for example, a date with the "day" part set to 0, which often indicates that the day is not given in the original document) or explict patterns of text that mean that the transcriber was uncertain about something (for example `...`, `\[ill\]`, `(HMS Iphigenia?)`). To find rows that need correction, use `Ctrl-Down` and `Ctrl-Up` to leap across empty cells in this column. If there are large numbers of problems, you could consider increasing the aggressiveness of the reconciliation.

Individual cells that need correction are often easily spotted. The following example is from `number of days victualled` ([volume 1, p. 89](https://panoptes-uploads.zooniverse.org/subject_location/ac549e8b-d2f3-4fcf-902e-4df825c0e6c9.jpeg), admission no. 7127):
```
6.0
----------
6.0 @2
70.0 @2
```

Here, the `6.0` at the top is the script's best guess at the correct value. The rows beneath the hyphens indicate that 2 transcribers thought the number was `6`, and 2 transcribers thought the number was `70`. Looking at the original page image shows that the correct value is `6`, so you should delete the original value of the cell and replace it with `6`.

Here is a second example from `how disposed of` ([volume 1, p. 90](https://panoptes-uploads.zooniverse.org/subject_location/16907fa8-138d-4b18-9232-fc4292ed7910.jpeg), admission no. 7160):
```
<No best guess>
----------
To a/his ship cured
Request cured
Shipped
```

Here, the script has no guess at the correct value. The rows beneath the hyphens indicate that 1 transcriber thought it was `To a/his ship cured`, 1 thought it was `Request cured` and 1 thought it was `Shipped` (when a transcription was given only once, there is no `@`). Looking at the original page shows that the correct text is `Request Shipped to go to his home (Cured)` -- which was not an option in the dropdown used by the transcribers, so it is not surprising that they gave different answers! You should either choose one of the possible answers as the "correct" one, or else delete the original value of the cell and replace it with the actual text from the page.

Sometimes cells need correction because they contain text that looks like an indication of transcriber uncertainty (for example, `...` or `[]`). These will not stand out as much as the examples above, but should still look pretty odd when scanning by eye. It is just important to be aware that you need to look out for these kinds of things, as well as the more obvious cases -- both might exist in the same row.

The `Autoresolved` column lists fields where transcriptions disagreed and were successfully autoresolved. You can use this information to do spot checks that the resolver is performing correctly. If you find too many mistakes then you could consider reducing the aggressiveness of the reconciliation.

Note that the final few rows of the final page of the final volume (at the bottom of the CSV file) are blank. You will need to manually delete the various bad admission numbers/zeroes/Missing Entry values in these cells. (There are also blank rows to be found throughout phase2, but the scripts do a better job of finding and removing these for you.)

## Generating `mimsy.txt`

Once you have corrected `joined.csv`, you can generate a file for Mimsy ingest by running `mimsify.py` to create `output/mimsy.txt`. Either put the corrected CSV file in the `output` directory, or use `mimsify.py <location>` to tell the script where the corrected file is. I recommend calling the corrected file `corrected.csv`, placing it next to `joined.csv` and running `mimsify.py output/corrected.csv`.

You are likely to see messages as `mimsy.py` runs. Warnings are for information: you may want to check what is happening, but the script can handle the input. Errors are a real problem that you should resolve. Both types of message will tell you where the problem is in the input file.

Run with `--unresolved` to allow unresolved fields and flatten them into a convenient format. If you run without this option then any unresolved fields will trigger an error message.

Run with `--help` to see other options.


## About `joined.csv`

You can read about data transformations involved in creating `joined.csv` in [DATA_README.md](DATA_README.md).

The columns in `joined.csv` are as follows:

<table>
<tr align='left'><th>Column</b></th><th><b>Header</b></th><th><b>Description</b></th></tr>
<tr><td>A</td><td>original</td><td>Link to the image of the original page on Zooniverse. Helpful for checking and reconciling transcriptions.</td></tr>
<tr><td>B</td><td>subject</td><td>The Zooniverse subject id for the page.</td></tr>
<tr><td>C</td><td>volume</td><td>The volume of the Admissions Registers that the page comes from.</td></tr>
<tr><td>D</td><td>page</td><td>The number of the page in the volume.</td></tr>
<tr><td>E</td><td>admission number</td><td rowspan=13>Reconciled transcription for each column of the original page.</td></tr>
<tr><td>F</td><td>date of entry</td></tr>
<tr><td>G</td><td>name</td></tr>
<tr><td>H</td><td>quality</td></tr>
<tr><td>I</td><td>age</td></tr>
<tr><td>J</td><td>place of birth</td></tr>
<tr><td>K</td><td>port sailed out of</td></tr>
<tr><td>L</td><td>years at sea</td></tr>
<tr><td>M</td><td>last services</td></tr>
<tr><td>N</td><td>under what circumstances admitted (or nature of complaint)</td></tr>
<tr><td>O</td><td>date of discharge</td></tr>
<tr><td>P</td><td>how disposed of</td></tr>
<tr><td>Q</td><td>number of days victualled</td></tr>
<tr><td>R</td><td>Problems</td><td>Records problems that need a human to fix them. Provides a minimum count for unresolved fields and also flags up when some of the fields are empty.</td></tr>
<tr><td>S</td><td>Autoresolved</td><td>Lists which fields in the row had to be reconciled by the script. All other fields should have total agreement among the transcriptions (after cleaning).</td></tr>
<tr><td>T</td><td>Repo</td><td>The repository from which came the scripts which produced this row.</td></tr>
<tr><td>U</td><td>Commit</td><td>The commit from which came the scripts which produced this row.</td></tr>
<tr><td>V</td><td>Args</td><td>The exact invocation of `aggregate.py` which produced this row.</td></tr>
<tr><td>W</td><td>§°—’“”…；£ªéºöœü</td><td>This peculiar header exists to make sure that Google Sheets understands that the file's character set is UTF-8, not ASCII. Without this header, Google Sheets loses some of the information from the original transcriptions. There is no content in this column.</tr>
</table>



## About `mimsy.txt`

<!--TODO: Document this format once we have agreed its final form -->
