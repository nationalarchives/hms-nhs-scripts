#!/bin/bash
#At one time I needed this to get an install together. No longer seems to be needed.
WHEEL=panoptes_aggregation-3.6.0-py3-none-any.whl
METADATA=panoptes_aggregation-3.6.0.dist-info/METADATA

trap "{ echo 'Something went wrong' >&2; rm "$WHEEL" || echo 'Unable to remove wheel' >&2; exit 1; }" ERR

pip download --no-deps panoptes_aggregation==3.6.0
unzip -p "$WHEEL" "$METADATA" |
  sed 's#^Requires-Dist: \([^ ]\+\) ([^,]\+,#Requires-Dist: \1 (#' |
  zip "$WHEEL" -
echo -e '@ -\n@='"$METADATA" | zipnote -w "$WHEEL" #re https://stackoverflow.com/a/54702078
pip install -c constraints.txt "$WHEEL" #this should stop us getting deps with different version to the stored ones
pip freeze --exclude panoptes_aggregation       > constraints.txt
pip freeze --exclude panoptes_aggregation --all > constraints_all.txt
rm "$WHEEL"
