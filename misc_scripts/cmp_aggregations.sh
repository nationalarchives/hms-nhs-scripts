#!/bin/bash

#Comparing certain extract.py outputs that should differ only by aggregator version after upgrading panoptes_aggregation
#Note that this does not compare logs. The extractor and reducer logs are expected to be different (pretty much just a dump of a progress bar).
#Other files should be identical
#Example use: ./misc_scripts/cmp_extractions.sh saved_extraction/

refdir="$1"
newdir="${2:-`dirname $0`/../extraction}"

for x in "$newdir"/dropdown_extractor_186*; do { 
  echo "Reference directory: ${refdir}"
  echo "New directory:       ${newdir}"
  echo -n "${x}: "
  diff -sq <(csvtool namedcols classification_id,user_name,user_id,workflow_id,task,created_at,subject_id,extractor,data.value                     "$x") \
           <(csvtool namedcols classification_id,user_name,user_id,workflow_id,task,created_at,subject_id,extractor,data.value "$refdir"/"`basename $x`")
  } done >dropdown_extractor.diff 2>&1
echo "Dropdown extractions done"

for x in "$newdir"/dropdown_reducer_186*; do { 
  echo "Reference directory: ${refdir}"
  echo "New directory:       ${newdir}"
  echo -n "${x}: "
  diff -sq <(csvtool namedcols subject_id,workflow_id,task,reducer,data.value                     "$x") \
           <(csvtool namedcols subject_id,workflow_id,task,reducer,data.value "$refdir"/"`basename $x`")
  } done >dropdown_reducer.diff 2>&1
echo "Dropdown reductions done"

for x in "$newdir"/text_extractor_186*; do { 
  echo "Reference directory: ${refdir}"
  echo "New directory:       ${newdir}"
  echo -n "${x}: "
  diff -sq <(csvtool namedcols classification_id,user_name,user_id,workflow_id,task,created_at,subject_id,extractor,data.text,data.gold_standard                     "$x") \
           <(csvtool namedcols classification_id,user_name,user_id,workflow_id,task,created_at,subject_id,extractor,data.text,data.gold_standard "$refdir"/"`basename $x`")
  } done >text_extractor.diff 2>&1
echo "Text extractions done"

for x in "$newdir"/text_reducer_186*; do { 
  echo "Reference directory: ${refdir}"
  echo "New directory:       ${newdir}"
  echo -n "${x}: "
  diff -sq <(csvtool namedcols subject_id,workflow_id,task,reducer,data.aligned_text,data.number_views,data.consensus_score,data.consensus_text,data.gold_standard,data.user_ids                     "$x") \
           <(csvtool namedcols subject_id,workflow_id,task,reducer,data.aligned_text,data.number_views,data.consensus_score,data.consensus_text,data.gold_standard,data.user_ids "$refdir"/"`basename $x`")
  } done >text_reducer.diff 2>&1
echo "Text reductions done"

