#%%

import pandas as pd

dfjogos = pd.read_parquet('../data/sa24_01_jogos.parquet')
dfjogos

# %%

pd.set_option('display.max_columns', None)
dfjogos.head()

# %%

dfjogos.info()

# %% #* Renomeando colunas para ajudar na 'legibilidade do codigo'

dfjogos = dfjogos.rename(columns={"me1": "entrada_m1",
                              "matr1" : "atraso_m1",
                              "ve1": "entrada_v1",
                              "vatr1": "atraso_v1",
                              "ini1": "inicio_1t",
                              "atr1": "atraso_inicio_1t",
                              "fim1" : "fim_1t",
                              "acr1": "acr_1t",
                              "me2": "entrada_m2",
                              "matr2": "atraso_m2",
                              "ve2": "entrada_v2",
                              "vatr2": "atraso_v2",
                              "ini2": "inicio_2t",
                              "atr2": "atraso_2t",
                              "fim2": "fim_2t"})

dfjogos