#!/bin/bash

mkdir testinput/ || { echo testinput exists, delete it before running this script; exit 1; }

for x in extraction/text_reducer_*.csv; do
  csvtool namedcol subject_id,task,data.aligned_text,data.number_views,data.consensus_score,data.consensus_text $x > testinput/`basename $x`
done

for x in extraction/dropdown_reducer_*.csv; do
  csvtool namedcol subject_id,task,data.value $x > testinput/`basename $x`
done

ln extraction/Task_labels_workflow_186{1,2}4_V3.1.yaml testinput/
