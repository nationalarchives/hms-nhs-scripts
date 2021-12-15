## Overview ##

These are the data processing scripts for the [Zooniverse](https://zooniverse.org) crowdsourcing project [HMS NHS: The Nautical Health Service](https://www.zooniverse.org/projects/msalmon/hms-nhs-the-nautical-health-service). HMS NHS is a [Royal Museums Greenwich](https://rmg.co.uk) project, and part of the larger [Towards a National Colletion](https://www.nationalcollection.org.uk/) project, [Engaging Crowds](https://tanc-ahrc.github.io/EngagingCrowds/).

These scripts process the data from HMS NHS into useable forms. The general approach is to make use of Zooniverse's own Panoptes tool as much as possible. These scripts do additional data cleansing and format the data into two output forms.

The first output form is a CSV file containing the data from the project, including information
about what was automatically resolved by code in the script, and what could not be resolved.
This CSV file is intended to be hand checked. Unresolvable transcriptions can be replaced with
some valid transcription. Automatically resolved transcriptions can also be spot-checked if
desired.

The second output form takes the manually-edited CSV file as its input. Its output is another
CSV file, this one suitable for ingest into RMG's cataloguing system.


## Simple Usage Example ##

* Install Zooniverse's Panoptes scripts: `pip install panoptes_aggregation[gui]`.
* Download all workflow data, plus `...-subjects.csv` and `...-workflows.csv`, from the Zooniverse project
* Put the above into exports/
* Run ./extract.sh. This will run the Panoptes scripts and, in between the 'extract' and 'reduce' steps, clean_extraction.py to clean the extracted data before the reducer sees it. Output goes in aggregation/. Takes a while, mostly in the reduction step.
* Optionally, compare aggregation/..._extractor.csv.original with aggregation/..._extract.csv.cleaned
* Run ./aggregate.py. You can see one sample commandline in qtest.sh. At time of writing, the commandline used to generate real outputs is ./aggregate.py -t 0.3.


## Manifest ##

### Panoptes ###

* extract.sh: Runs Zooniverse's Panoptes extraction and aggregation scripts.
* exports/: Location where exports from the Zooniverse project should go. This directory must contain the export for each live workflow, plus the ...-subjects.csv and ...-workflows.csv exports.
* aggregation/: Directory containing the result of running extract.sh on exports from the Zooniverse project.

### Extraction Cleaning ###
* clean_extraction.py: Script run between the Panoptes extraction and reduction steps, to provide cleaner input to the reduction step.
* extracttest/: Test inputs for clean_extraction.py
* qtest.sh: Simple test driver for checking clean_extraction.py and aggregate.py

### Aggregation (produce first CSV file) ###
* aggregate.py: Reconciliation script.
* coverage.pl: Quick & dirty coverage checker for aggregate.py's -f mode.
* workflow.yaml: Data file describing the workflows. Used by aggregate.py.
* qtest.sh: Simple test driver for checking clean_extraction.py and aggregate.py
* testdata/: Test inputs and golden logs for aggregate.py
* gentestinput.sh: Script for generating the initial contents of testinput/ (now testdata/)
* golden_0.5.csv: Reference file for qtest.sh (testing the first CSV output)
* output/: Directory where output of aggregate.py goes

### Catalogue (produce second CSV file) ###
* templatizer.py: Generates the second CSV output
* golden_for_mimsy.csv: Reference file for testing the second CSV output

### Misc ###
labelextractor.sh: Extracts all of the labels for the dropdown tasks
maxcolwidth.sh: Checks the column widths do not exceed some maximum. Not currently used.
quick_threshold_test.sh: Simple test driver for comparing the effects of different autoreconciliation aggressiveness


## CSV Format One ##

This is the CSV file for manual editing. It contains...
 
## TODO ##

* Add tests for the unstring_number and unstring_date functions of clean_extraction.py
* Track which files are modified in the cleaning step and pass this information through to the first CSV file.
* Accurately count problem fields + transcription problems.
