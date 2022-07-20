rm -rf aggregation_GOLDEN_multiversion
cp -r aggregation_GOLDEN aggregation_GOLDEN_multiversion
cd aggregation_GOLDEN_multiversion

cp dropdown_extractor_18614{,_V3_1}.csv 
cp dropdown_extractor_18624{,_V3_1}.csv 
for x in 186{11,12,13,16,17,18,19,21,22,23,25}; do
  cp text_extractor_${x}.csv text_extractor_${x}_V3_1.csv
done
for x in 186{11,12,13,14,16,17,18,19,21,22,23,24,25}; do
  mv config_${x}.log config_${x}_V3.1.log
  mv extract_${x}.log extract_${x}_V3.1.log
done
