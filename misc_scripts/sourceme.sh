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

function subject_volume {
  for x in "$@"; do
    echo -n "$x: "
    csvsql --tables foo --query "select metadata from foo where subject_id=${x} limit 1" "$SUBJECTS" |
      sed 1d | #remove the header
      tr -s '"' | #squash the '""' into '"'
      sed "s/.\(.*\)./echo '\1' | jq -r .Filename/" | #Convert the result into a legitimate jq incantation to get the filename -- note that this must be done line by line and that we need to replace the leading and trailing " with ' -- though right now we limit to 1 result anyway
      sh | #Execute said legitimate incantation
      sed 's/.*_\([[:digit:]]\+\)-\([[:digit:]]\+\).*/v. \1, p. (uncorrected) \2/' #Get the volume number and (uncorrected) page number from the filename string
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
    echo  "select distinct subject_id from foo where workflow_id=$x order by subject_id" extraction/text_extractor_${x}.csv.full
    c-sql "select distinct subject_id from foo where workflow_id=$x order by subject_id" extraction/text_extractor_${x}.csv.full > subject_ids_${x}_by_classifications.csv &
  done
  for x in 186{1,2}4; do
    echo  "select distinct subject_id from foo where workflow_id=$x order by subject_id" extraction/dropdown_extractor_${x}.csv.full
    c-sql "select distinct subject_id from foo where workflow_id=$x order by subject_id" extraction/dropdown_extractor_${x}.csv.full > subject_ids_${x}_by_classifications.csv &
  done
  wait
}

function versions_per_volume {
  for export in `yq -r '.launch_workflows[].export' workflow.yaml`; do #for each exports file
    echo "$export" #tell me what I am working with
    c-sql 'select workflow_version,subject_data from foo' "${ROOTME}/exports/${export}" | #get the workflow version and the subject data
      sed 's/^\([^,]\+\),.*""Filename"":""\([^"]\+\).*/\1,\2/' | #Do a hacky thing with sed to simplify us down to the workflow_version,filename
      sed 's/^\([^,]\+\),.*_\([^-]\+\)-.*/\2,\1/' | #Do another thing with sed to replace the filename with the volume number (and swap the fields for easier sorting)
      sort -g | uniq
    echo
  done
}

#Dump contents of logs that are littered with progress bar outputs
function _dump_progress_logs {
  sed "/${1}: .*| ETA: /d" "${4}"/"${3}"*.log | sed "/^${1}: 100% |#\{${2}\}| Time:  /d"
}

function dump_extract_logs {
  _dump_progress_logs Extracting 45 extract "$@"
}

function dump_reduce_logs {
  _dump_progress_logs Reducing 47 reduce "$@"
}

#Compare extract.py output dirs, ignoring the two types of log that dump progress bars. Instead, sed-filter those, and diff the output of that.
function cmp_extract {
  diff -qr "$1" "$2" | grep -v '^Files .* and .*/\(extract\|reduce\)_[^/]*\.log differ$'
  for logtype in extract reduce; do
    diff <(dump_${logtype}_logs "$1") <(dump_${logtype}_logs "$2")
    if [ $? -eq 0 ]; then #If the extract/reduce logs are identical, tell me of anything dodgy in them
      if [ `wc -l <(dump_${logtype}_logs "$1") | tail -n1 | sed 's/^[[:blank:]]*\([[:digit:]]\+\) .*/\1/'` -ne 0 ]; then
        echo "Dubious line(s) in {${1},${2}}/${logtype}*.log"
        dump_${logtype}_logs "$1"
      fi
    fi
  done
}
