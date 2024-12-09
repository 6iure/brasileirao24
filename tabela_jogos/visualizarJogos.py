#%%

import pandas as pd

dfjogos = pd.read_parquet('../data/sa24_01_jogos.parquet')
dfjogos

# %%

pd.set_option('display.max_columns', None)
dfjogos.head()

# %%

dfjogos.info()
