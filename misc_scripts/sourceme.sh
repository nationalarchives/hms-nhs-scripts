export ROOTME="`git rev-parse --show-toplevel`"
alias c-sql='csvsql --tables foo --query' #DANGER: expects double quotes around fields with spaces inside the query string (so --query 'select "admission number"...' works)
export WORKFLOW_RANGE='workflow_id > 18610 and workflow_id < 18626'
export SUBJECTS="${ROOTME}/exports/hms-nhs-the-nautical-health-service-subjects.csv" #Note that the subjects file can be really misleading -- current guess is that some subjects have been removed from the project since they were classified

#e.g. log_total_time 25.log -- for use with logs created with --timing
function log_total_time {
  for x in "$@"; do
    echo -n "$x: "
    cat "$x" | sed 's/^. *\([[:digit:]]\+\)[^[:digit:]].*/\1/' | paste -sd+ | bc
  done
}

function linecount {
  for x in "$@"; do
    echo -n "${x}: "
    csvtool cols 1 "$x" | wc -l
  done
}

function diff_outputs {
  left="$1"
  right="$2"
  if [ -z "$left" ]; then left="output_new_view_GOLD"; fi
  if [ -z "$right" ]; then right="output"; fi
  echo "$left vs $right"
  diff -qr "$left" "$right"
  diff -s <(csvtool cols 1-17,19 "$left"/joined.csv) <(csvtool cols 1-17,19 "$right"/joined.csv) || linecount "$left"/joined.csv "$right"/joined.csv
}

function qdiff_outputs {
  diff_outputs output_new_view_GOLD_10
}

function meld_full {
  left="$1"
  right="$2"
  if [ -z "$right" ]; then
    right="$left"
    left="output"
  fi
  echo "meld <(csvtool cols 1-19 "$left"/joined.csv) <(csvtool cols 1-19 "$right"/joined.csv)"
  meld <(csvtool cols 1-19 "$left"/joined.csv) <(csvtool cols 1-19 "$right"/joined.csv)&
}

function meld_brief {
  left="$1"
  right="$2"
  if [ -z "$right" ]; then
    right="$left"
    left="output"
  fi
  meld <(csvtool cols 1-19 "$left"/joined.csv | head -n 200) <(csvtool cols 1-19 "$right"/joined.csv | head -n 200)&
}

function subject_lookup {
  for x in "$@"; do
    csvsql --tables foo --query "select * from foo where subject_id=${x} and ${WORKFLOW_RANGE}" $SUBJECTS #DANGER: expects double quotes around fields with spaces inside the query string (so --query 'select "admission number"...' works)
  done
}

function subject_metadata {
  for x in "$@"; do
    csvsql --tables foo --query "select distinct metadata,locations from foo where subject_id=${x} and ${WORKFLOW_RANGE}" $SUBJECTS #DANGER: expects double quotes around fields with spaces inside the query string (so --query 'select "admission number"...' works)
  done
}

#Note that the subjects file can be really misleading -- current guess is that some subjects have been removed from the project since they were classified
function gen_subjects_per_workflow {
  for x in `seq 18611 18614` `seq 18616 18619` `seq 18621 18625`; do
    echo  "select subject_id from foo where workflow_id=$x" $SUBJECTS
    c-sql "select subject_id from foo where workflow_id=$x" $SUBJECTS > subject_ids_$x.csv
  done
}

#Note that the subjects file can be really misleading -- current guess is that some subjects have been removed from the project since they were classified
function gen_metadata_per_workflow {
  for x in `seq 18611 18614` `seq 18616 18619` `seq 18621 18625`; do
    echo  "select metadata from foo where workflow_id=$x" $SUBJECTS
    c-sql "select metadata from foo where workflow_id=$x" $SUBJECTS > subject_metadata_$x.csv
  done
}

#Note that the subjects file can be really misleading -- current guess is that some subjects have been removed from the project since they were classified
function gen_created_per_workflow {
  for x in `seq 18611 18614` `seq 18616 18619` `seq 18621 18625`; do
    echo  "select created_at from foo where workflow_id=$x" $SUBJECTS
    c-sql "select created_at from foo where workflow_id=$x" $SUBJECTS > subject_created_at_$x.csv
  done
}

function gen_subjects_per_workflow_from_classifications {
  for x in 1861{1,2,3,6,7,8,9} 1862{1,2,3,5}; do
    echo  "select distinct subject_id from foo where workflow_id=$x order by subject_id" aggregation/text_extractor_${x}.csv.full
    c-sql "select distinct subject_id from foo where workflow_id=$x order by subject_id" aggregation/text_extractor_${x}.csv.full > subject_ids_${x}_by_classifications.csv &
  done
  for x in 186{1,2}4; do
    echo  "select distinct subject_id from foo where workflow_id=$x order by subject_id" aggregation/dropdown_extractor_${x}.csv.full
    c-sql "select distinct subject_id from foo where workflow_id=$x order by subject_id" aggregation/dropdown_extractor_${x}.csv.full > subject_ids_${x}_by_classifications.csv &
  done
  wait
}
