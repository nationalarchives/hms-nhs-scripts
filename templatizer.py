#!/usr/bin/env python
import os
import pandas as pd

df = pd.read_csv('output/joined.csv', index_col = ['subject_id', 'task', 'volume', 'page', 'Autoresolved', 'Problems'], na_filter = False)

#Assumption: each subject id maps 1:1 to a (volume, page) pair
for volume in df.index.unique(level = 'volume'):
  os.mkdir(f'output/{volume}') #Will fail if volume already exists, which is what I want -- guarantees that each volume is only met once in the data
  for page in df.index.unique(level = 'page'):
    with open(f'output/{volume}/{page}.txt', 'x') as f:
      lines = []
      for index, row in df.xs((volume, page), level = ['volume', 'page']).iterrows():
        line = []
        for heading in df.columns:
          value = row[heading]
          line.append(f'{heading}, {row[heading]}')
        lines.append('; '.join(line))
      f.write('\n'.join(lines))
