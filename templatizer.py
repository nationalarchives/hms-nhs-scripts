#!/usr/bin/env python
import pandas as pd

df = pd.read_csv('output/joined.csv', index_col = ['subject_id', 'task', 'volume', 'page', 'Autoresolved', 'Problems'], na_filter = False)

#Assumption: each subject id maps 1:1 to a (volume, page) pair
#TODO: I'm sure there must be a more Pandas way to do this. And it's probably less code and more efficient.
pages = []
for volume in df.index.unique(level = 'volume'):
  for page in df.xs((volume), level = 'volume').index.unique(level = 'page'):
    lines = []
    for index, row in df.xs((volume, page), level = ['volume', 'page']).iterrows():
      line = []
      for heading in df.columns:
        value = row[heading]
        line.append(f'{heading}, {row[heading]}')
      lines.append('; '.join(line))
    pages.append([f'DSH/{volume}/{page}', '. '.join(lines) + '.'])
pd.DataFrame(pages).to_csv(path_or_buf = 'output/for_mimsy.csv', index = False, header = False)
