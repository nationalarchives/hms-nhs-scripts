#!/bin/bash

drop_t='dropdown'
text_t='text'
outdir=aggregation
name=(1-admission-number-workflow-classifications.csv
      2-date-of-entry-workflow-classifications.csv
      3-name-workflow-classifications.csv
      4-quality-workflow-classifications.csv
      5-age-workflow-classifications.csv
      6-place-of-birth-or-nationality-workflow-classifications.csv
      7-port-sailed-out-of-workflow-classifications.csv
      8-years-at-sea-workflow-classifications.csv
      9-last-services-or-ship-or-last-ship-workflow-classifications.csv
      10-under-what-circumstances-admitted-or-nature-of-complaint-workflow-classifications.csv
      11-date-of-discharge-workflow-classifications.csv
      12-how-disposed-of-workflow-classifications.csv
      13-number-of-days-victualled-or-in-hospital-workflow-classifications.csv
) 
      id=(  18109   18110   18111   18112   18113   18114   18115   18116   18117   18118   18119   18120   18121)
 version=(      1       1       1       5       1       1       1       1       1      11       1       1       3)
   minor=(      1       1       1       8       1       1       1       1       1      12       1       1       1)
datatype=($text_t $text_t $text_t $drop_t $text_t $text_t $text_t $text_t $text_t $text_t $text_t $drop_t $text_t)

#Highest available versions at time of writing
#version=(    3     3     3     5     3     3     3     3     3    11     3    35     3)
#  minor=(    6     7     9     8     6    10     6     7    16    18     6    50     7)

rm -rf "${outdir}"
mkdir  "${outdir}"

#Configuration
for i in {0..12}; do
  panoptes_aggregation config hms-nhs-the-nautical-health-service-workflows.csv ${id[$i]} -v ${version[$i]} -m ${minor[$i]} -d "${outdir}" &
done
wait

#Extraction
for i in {0..12}; do
  panoptes_aggregation extract "${name[$i]}" "${outdir}"/Extractor_config_workflow_${id[$i]}_V${version[$i]}.${minor[$i]}.yaml -d "${outdir}" -o ${id[$i]} &
done
wait

#Reduce
for i in {0..12}; do
  panoptes_aggregation reduce \
    -F last \
    -d "${outdir}" -o ${id[$i]} \
    "${outdir}"/${datatype[$i]}_extractor_${id[$i]}.csv \
    "${outdir}"/Reducer_config_workflow_${id[$i]}_V${version[$i]}.${minor[$i]}_${datatype[$i]}_extractor.yaml &
done
wait
