# %%
import pandas as pd
pd.plotting.register_matplotlib_converters()
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import colormaps
# %%

amarelos_path = '../data/sa24_06_camarelos.parquet'

df_amarelos = pd.read_parquet(amarelos_path)
df_amarelos

# %%

df_amarelos.info(memory_usage='deep')

# %%
df_amarelos['jogo'].describe()

# %%
#Mostra a quantia de amarelos no 1 jogo
df_amarelos[df_amarelos['jogo'] == 1]
#* ou df_amarelos.query('jogo == 1')

# %%
#* trazendo cartoes apenas para jogadores da partida 1
df_amarelos[df_amarelos['jogo'] == 1] #? como posso passar mais um parametro?

# %%
jogos_path = '../data/sa25_01_jogos.parquet'
df_jogos = pd.read_parquet(jogos_path, columns=['competicao', 'comp', 'ano', 'rodada', 'jogo', 'mandante', 'visitante'])
df_jogos

# %%
# Fazendo merge entre os DFs jogos e cartoes amarelos

df_merge = df_jogos.merge(df_amarelos,
                how= 'outer',
                on=['comp', 'jogo', 'ano']
)

df_merge.info()

#%%
df_merge['rodada'] = df_merge['rodada'].astype(int) #* transformando em int a coluna rodada

# %%
df_merge.groupby('jogo')['mandante'].count()

# %%
(df_merge
        .groupby(by='jogo', as_index=False)
        .agg(amarelo_jogo = ('comp', 'count'))
)

# %%
df_merge.jogo.value_counts()
# %%

df_merge.head()

#%%
# * Criando df com cartoes amarelos apenas para jogadores (excluindo a comissao tecnica)
df_aaj = df_merge.dropna(subset=['n_cbf'])
df_aaj

# %%
#* amarelos por rodada (APENAS PARA JOGADORES)
amarelos_rodada_j = df_aaj.groupby(by='rodada', as_index=False).agg(amarelo_rodada_j = ('rodada', 'count'))
amarelos_rodada_j.head()

# %% 
# Grafico em linhas usando o SEABORN de amarelo por rodada
plt.figure(figsize=(16,6))
plt.xlim(1,38)
plt.xticks(range(1,39, 1))
sns.lineplot(data=amarelos_rodada_j, x='rodada', y='amarelo_rodada_j')

# %%
plt.figure(figsize=(16,6))
plt.xlim(1,38)
plt.xticks(range(1,39, 1))
sns.barplot(data=amarelos_rodada_j, x='rodada', y='amarelo_rodada_j')
# %% 
# * Cartoes amarelos por Rodada no Brasileirao
janela = plt.figure(figsize=(10,5))
grafico = janela.add_axes([0,0,1,1])
colors = plt.cm.Wistia(0.2 + 0.7 * amarelos_rodada_j['amarelo_rodada_j'] / max(amarelos_rodada_j['amarelo_rodada_j']))

bars = grafico.bar(
        amarelos_rodada_j['rodada'],
        amarelos_rodada_j['amarelo_rodada_j'],
        color = colors,
        linewidth=1.2,
        alpha=0.8,
)

plt.title('Cartoes Amarelos por Rodada - Brasileirao 2024', fontsize=16, fontweight="bold")
plt.xlabel('Rodada', fontsize=12, color='#555', labelpad=10)
plt.ylabel('Total de Cartoes', fontsize=12, labelpad=10, color='#555')

grafico.set_xlim(0.5, 38.5)  # Margem para melhor visualização
grafico.set_xticks(range(1, 39, 4))
grafico.grid(axis='y', linestyle=':', alpha=0.4, color='gray')
grafico.set_axisbelow(True)

# numero exato em cima de cada barra
for bar in bars:
    height = bar.get_height()
    grafico.annotate(f'{height}',
                     xy=(bar.get_x() + bar.get_width() / 2, height),  
                     xytext=(0, 2),  
                     textcoords="offset points",
                     ha='center',
                     va='bottom') 
plt.show()
# %% 
#* Cartoes amarelos por time (Apenas para jogadores)
amarelos_time = (df_aaj
                .groupby(by='time', as_index=False)
                .agg(amarelos_time = ('time', 'count'))
                .sort_values('amarelos_time', ascending=True)
        )
amarelos_time   
# %%
#* grafico em barras verticais de quantos amarelos cada time recebeu
janela = plt.figure(figsize=(10,8), facecolor='white')
grafico = janela.add_axes([0,0,1,1])
colors = plt.cm.Wistia(0.1 + 0.9 * amarelos_time['amarelos_time'] / max(amarelos_time['amarelos_time']))

bars = grafico.barh(
    amarelos_time['time'],
    amarelos_time['amarelos_time'],
    color = colors,
    linewidth=1.2,
    alpha=0.8
    )

plt.title("Cartoes Amarelos por Time - Brasileirao 2024", fontsize=15, fontweight="bold",)
plt.xlabel("Total de Cartoes (Apenas Jogadores)",fontsize=12, color='#555' )
plt.ylabel("Times", fontsize=12, color='#555')

grafico.grid(axis='x', linestyle='--', alpha=0.4, color='#cccccc')
grafico.set_axisbelow(True)

for spine in ['top', 'right', 'bottom']:
    grafico.spines[spine].set_visible(False)

for bar in bars:
    width = bar.get_width()
    grafico.annotate(f'{width:.0f}',
                     xy=(width -1, bar.get_y() + bar.get_height()/2),
                     xytext=(0, 0),
                     textcoords="offset points",
                     ha='right',
                     va='center',
                     fontsize=10,
                     color='white',
                     fontweight='bold'
                     )

plt.show()
# %%
#* Filtrando por NOME, quais foram os jogadores mais amarelados
# todo melhorar, colocar time de cada jogador e talvez seu apelido
amarelos_nome = (df_aaj
                 .groupby(by=['nome_jogador', 'time'], as_index=False)
                 .agg(amarelos_nome = ('nome_jogador', 'count'))
                 .sort_values('amarelos_nome', ascending=True)
                 )
amarelos_nome

# %%
mais_amarelados = amarelos_nome.tail(10)
mais_amarelados
# %% Grafico de jogadores com mais amarelos 
#todo adicionar time de cada jogador dentro da barra 

janela = plt. figure(figsize=(10, 5))
grafico = janela.add_axes([0,0,1,1])
colors = plt.cm.Wistia(0.2 + 0.7 * amarelos_time['amarelos_time'] / max(amarelos_time['amarelos_time']))
bars = grafico.barh(
    mais_amarelados['nome_jogador'], 
    mais_amarelados['amarelos_nome'],
    color = colors,
    alpha = 0.8
)

plt.title('Jogadores que Mais Receberam Cartoes Amarelos - Brasileirao 2024', fontsize=15, fontweight="bold")
plt.xlabel('Total de Cartoes', fontsize=12, color='#555')
plt.ylabel('Nome do Jogador', fontsize=12, color='#555')

grafico.grid(axis='x', linestyle='--', alpha=0.4, color='#cccccc')
grafico.set_axisbelow(True)

for spine in ['top', 'right', 'bottom']:
    grafico.spines[spine].set_visible(False)

for bar in bars:
    width = bar.get_width()
    grafico.annotate(f'{width:.0f}',
                     xy=(width -0.1, bar.get_y() + bar.get_height()/2),
                     xytext=(0, 0),
                     textcoords="offset points",
                     ha='right',
                     va='center',
                     fontsize=10,
                     color='white',
                     fontweight='bold'
                     )

plt.show()
#%%
comissao_path = '../data/sa25_04_comissaotec.parquet'
df_comissaotec = pd.read_parquet(comissao_path)
df_comissaotec

# %%
# mantendo apenas os dados de individuos que nao tem n_cbf, ou seja, integrantes da comissao tecnica 
df_aac = df_merge[df_merge['n_cbf'].isna()]
df_aac

# %%
df_aac = df_aac.rename(columns={'nome_jogador' : 'nome'})
df_aac

# %% 

df_aac.dropna(subset=['time'], inplace=True)

# %%

df_aac[df_aac['time'].str.startswith("Bah")]
# %% fazendo merge entre os dfs de amarelos apenas para comissao e da comissao tecnica 

df_amarelos_comissao = df_aac.merge(df_comissaotec,
                                              how='left',
                                              on=['comp', 'ano', 'jogo', 'time', 'nome'])

#tirando colunas que os valores sao desnecessarios/vazios
df_amarelos_comissao = df_amarelos_comissao.drop(columns=['m/v_x', 'T/R', 'n_cbf'])
df_amarelos_comissao

# %% 
df_amarelos_comissao.info()

# %% filtrando por apenas time e amarelos por comissao
amarelos_comissao = (df_amarelos_comissao
                .groupby(by='time', as_index=False)
                .agg(amarelos_comissao = ('time', 'count'))
                .sort_values('amarelos_comissao', ascending=True)
        )
amarelos_comissao

# 154 cartoes totais
# %% Grafico de Comissoes Tecnicas Mais Indisciplinadas (por time comissoes q + recebem cartoes amarelos)
janela = plt.figure(figsize=(10,5))
grafico = janela.add_axes([0,0,1,1])
colors = plt.cm.Wistia(0.2 + 0.7 * amarelos_comissao['amarelos_comissao'] / max(amarelos_comissao['amarelos_comissao']))

bars = grafico.barh(
    amarelos_comissao['time'],
    amarelos_comissao['amarelos_comissao'],
    color = colors,
    alpha = 0.8
    )

plt.title("Comissoes Tecnicas Mais Indisciplinadas - Brasileirao 2024", fontsize=15, fontweight="bold")
plt.xlabel("Total de Cartoes", fontsize=12, color='#555')
plt.ylabel("Times", fontsize=12, color='#555')

grafico.grid(axis='x', linestyle='--', alpha=0.4, color='#CCCCCC')
grafico.set_axisbelow(True)

for spine in ['top', 'right', 'bottom']:
    grafico.spines[spine].set_visible(False)

for bar in bars:
    width = bar.get_width()
    grafico.annotate(
        f'{width:.0f}',
        xy=(width, bar.get_y() + bar.get_height()/2),
        xytext=(2, 0),
        textcoords="offset points",
        ha='left',
        va='center',
        color='black',
        fontweight='bold',
    )

plt.show()
# %%
amarelos_funcao = (df_amarelos_comissao
                   .groupby(by='funcao', as_index=False)
                   .agg(amarelos_funcao = ('funcao', 'count'))
                   .sort_values('amarelos_funcao', ascending=True)
                   )
amarelos_funcao
# 134 cartoes totais
# %% tabela dos tecnicos mais amarelados 
df_apenas_tecnicos = df_amarelos_comissao[df_amarelos_comissao['funcao'] == 'Técnico']
df_apenas_tecnicos
# %%

df_apenas_tecnicos['nome'].value_counts()

# %%
amarelos_tecnicos = (df_apenas_tecnicos
                   .groupby(by='nome', as_index=False)
                   .agg(amarelos_tecnicos = ('nome', 'count'))
                   .sort_values('amarelos_tecnicos', ascending=True)
                   )
amarelos_tecnicos
# %% grafico dos tecnicos mais amarelados em 2024
#todo adicionar o time de cada tecnico
janela = plt.figure(figsize=(12, 8))
grafico = janela.add_axes([0.1, 0.2, 0.75, 0.7])
colors = plt.cm.Wistia(0.2 + 0.7 * amarelos_tecnicos['amarelos_tecnicos'] / max(amarelos_tecnicos['amarelos_tecnicos']))
bars = grafico.barh(
    amarelos_tecnicos['nome'],
    amarelos_tecnicos['amarelos_tecnicos'],
    color=colors,
    alpha=0.8
    )

plt.title('Tecnicos Mais Indisciplinados - Brasileirao 2024', fontsize=15, fontweight='bold')
plt.xlabel('Total de Cartoes', fontsize=12, color='#555')
plt.ylabel('Nome', fontsize=12, color='#555')

grafico.grid(axis='x', linestyle='--', alpha=0.4, color='#CCCCCC')
grafico.set_axisbelow(True)

for spine in ['top', 'right', 'bottom']:
    grafico.spines[spine].set_visible(False)

for bar in bars:
    width = bar.get_width()
    grafico.annotate(f'{width}',
                     xy=(width, bar.get_y() + bar.get_height()/2),
                     xytext=(2, 0),
                     textcoords="offset points",
                     ha='left',
                     va='center',
                     fontweight='bold'
                     )

plt.show()
# %%
