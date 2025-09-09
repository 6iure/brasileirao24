# %% 
import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
# %%
jogos_path = '../data/sa25_01_jogos.parquet'
jogos = pd.read_parquet(jogos_path, columns=['rodada', 'jogo', 'mandante', 'visitante', 'mres2', 'vres2'])
jogos

# %%
jogos.head()

# %% agrupando a tabela jogos por mandante e somando a quantia de gols que cada time fez e sofreu como mandante
gols_mandante = (jogos
    .groupby(by='mandante', as_index=False)
    .agg(gols_como_mandante = ('mres2', 'sum'),
         gols_sofridos_casa = ('vres2', 'sum'))
    )

gols_mandante

# %% agrupando a tabela jogos por vistante e somando a quantia de gols que cada time fez e sofreu como visitante
gols_visitante = (jogos
    .groupby(by='visitante', as_index=False)
    .agg(gols_como_visitante = ('vres2', 'sum'),
          gols_sofridos_fora = ('mres2', 'sum'))
    )

gols_visitante
# %% merge entre as tabelas de gols como mandante e visitante para criacao da tabela de gols marcados/sofridos como mandante/visitante por time

gols_mv = gols_mandante.merge(gols_visitante,
                              how='inner',
                              left_on=['mandante'],
                              right_on=['visitante']
                              )
gols_mv

# %%
gols_mv.drop(columns='visitante', inplace=True)

# %%
gols_mv = gols_mv.rename(columns={'mandante' : 'time'})
gols_mv

# %%
gols_mv['saldo_casa'] = gols_mv['gols_como_mandante'] - gols_mv['gols_sofridos_casa']
gols_mv['saldo_fora'] = gols_mv['gols_como_visitante'] - gols_mv['gols_sofridos_fora']
gols_mv

# %%
gols_mv_ordenado = gols_mv.sort_values('saldo_casa', ascending=False)
gols_mv_ordenado = gols_mv.sort_values('saldo_fora', ascending=False)

# %% heatmap de eficiencia

plt.figure(figsize=(10,8))
sns.heatmap(
    gols_mv_ordenado.set_index('time')[['saldo_casa', 'saldo_fora']],
    annot=True,
    cmap='coolwarm',
    center=0,
    linewidths=0.5
)

plt.title('saldo de gols por time')
# %%
plt.figure(figsize=(10,8))
sns.scatterplot(
    data=gols_mv,
    x=gols_mv['gols_como_mandante'],
    y=gols_mv['gols_sofridos_casa'],
    hue='time',
)

plt.axline((0, 0), slope=1, linestyle='', color='white')
