#!/bin/bash

drop_t='dropdown'
text_t='text'
outdir=aggregation
indir=exports

#TODO: The following duplicates the information in workflow.yaml
#      workflow.yaml should be the single source of truth.

#launch workflows
name=(1-admission-number-classifications.csv
      2-date-of-entry-classifications.csv
      3-name-classifications.csv
      4-quality-classifications.csv
      5-age-classifications.csv
      6-place-of-birth-classifications.csv
      7-port-sailed-out-of-classifications.csv
      8-years-at-sea-classifications.csv
      9-last-services-classifications.csv
      10-under-what-circumstances-admitted-or-nature-of-complaint-classifications.csv
      11-date-of-discharge-classifications.csv
      12-how-disposed-of-classifications.csv
      13-number-of-days-victualled-classifications.csv
)
         id=(  18611   18612   18613   18614   18616   18617   18618   18619   18621   18622   18623   18624   18625)
    version=(      3       3       3       3       3       3       3       3       3       3       3       3       3)
      minor=(      1       1       1       1       1       1       1       1       1       1       1       1       1)
   datatype=($text_t $text_t $text_t $drop_t $text_t $text_t $text_t $text_t $text_t $text_t $text_t $drop_t $text_t)
postextract=(  false   false   false   false   false    true   false    true   false   false   false   false   false)

#development workflows (matches name in YAML file)
#name=(1-admission-number-workflow-classifications.csv
#      2-date-of-entry-workflow-classifications.csv
#      3-name-workflow-classifications.csv
#      4-quality-workflow-classifications.csv
#      5-age-workflow-classifications.csv
#      6-place-of-birth-or-nationality-workflow-classifications.csv
#      7-port-sailed-out-of-workflow-classifications.csv
#      8-years-at-sea-workflow-classifications.csv
#      9-last-services-or-ship-or-last-ship-workflow-classifications.csv
#      10-under-what-circumstances-admitted-or-nature-of-complaint-workflow-classifications.csv
#      11-date-of-discharge-workflow-classifications.csv
#      12-how-disposed-of-workflow-classifications.csv
#      13-number-of-days-victualled-or-in-hospital-workflow-classifications.csv
#)
#      id=(  18109   18110   18111   18112   18113   18114   18115   18116   18117   18118   18119   18120   18121)
# version=(      1       1       1       5       1       1       1       1       1      11       1       1       3)
#   minor=(      1       1       1       8       1       1       1       1       1      12       1       1       1)
#datatype=($text_t $text_t $text_t $drop_t $text_t $text_t $text_t $text_t $text_t $text_t $text_t $drop_t $text_t)

#Highest available versions at time of writing
#version=(    3     3     3     5     3     3     3     3     3    11     3    35     3)
#  minor=(    6     7     9     8     6    10     6     7    16    18     6    50     7)

rm -rf "${outdir}"
mkdir  "${outdir}"

processes=()
for i in {0..12}; do
  {
    set -o pipefail
    { panoptes_aggregation config "${indir}"/hms-nhs-the-nautical-health-service-workflows.csv ${id[$i]} -v ${version[$i]} -m ${minor[$i]} -d "${outdir}"               > "${outdir}/config_${id[$i]}.log"  2>&1;  } &&
    { panoptes_aggregation extract "${indir}/${name[$i]}" "${outdir}"/Extractor_config_workflow_${id[$i]}_V${version[$i]}.${minor[$i]}.yaml -d "${outdir}" -o ${id[$i]} > "${outdir}/extract_${id[$i]}.log" 2>&1; } &&
    if ${postextract[$i]}; then
      { ./clean_extraction.py "${outdir}/${datatype[$i]}_extractor_${id[$i]}.csv" > "${outdir}/postextract_${id[$i]}.log" ${id[$i]} 2>&1 &&
         mv "${outdir}/${datatype[$i]}_extractor_${id[$i]}.csv" "${outdir}/${datatype[$i]}_extractor_${id[$i]}.csv.original" &&
         cp "${outdir}/${datatype[$i]}_extractor_${id[$i]}.csv.cleaned" "${outdir}/${datatype[$i]}_extractor_${id[$i]}.csv"; }
    else
       true
    fi &&
    { panoptes_aggregation reduce \
        -F last \
        -d "${outdir}" -o ${id[$i]} \
        "${outdir}"/${datatype[$i]}_extractor_${id[$i]}.csv \
        "${outdir}"/Reducer_config_workflow_${id[$i]}_V${version[$i]}.${minor[$i]}_${datatype[$i]}_extractor.yaml > "${outdir}/reduce_${id[$i]}.log" 2>&1;
    } || { echo "*** Workflow ${id[$i]} failed" >&2; false; }
  }&
  processes+=($!)
done

final_errcode=0
for p in ${processes[@]}; do
  wait $p
  proc_errcode=$?
  if [ $proc_errcode -ne 0 ]; then final_errcode=1; fi
done
if [ $final_errcode -eq 0 ]; then
  echo "All done, no errors"
else
  echo "Errors: look for *** above" >&2
fi
exit $final_errcode
