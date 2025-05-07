# %%
import pandas as pd
pd.plotting.register_matplotlib_converters()
import matplotlib.pyplot as plt
import seaborn as sns
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
# * grafico usando apenas MATPLOTLIB

janela = plt.figure(figsize=(10,5))
grafico = janela.add_axes([0,0,1,1])
bars = grafico.bar(amarelos_rodada_j['rodada'], amarelos_rodada_j['amarelo_rodada_j'])

plt.title('Cartoes Amarelos por Rodada no Brasileirao 2024', fontsize=15, fontweight="bold")
plt.xlabel('Numero da Rodada')
plt.ylabel('Quantia de Cartoes')

grafico.set_xlim(0.5, 38.5)  # Margem para melhor visualização
grafico.set_xticks(range(1, 38, 4))  # Força a exibição de todas as rodadas (1 a 38)

for bar in bars:
    height = bar.get_height()
    grafico.annotate(f'{height}',
                     xy=(bar.get_x() + bar.get_width() / 2, height),  # Posição no topo da barra
                     xytext=(0, 2),  # Deslocamento vertical do texto (2 pontos para cima)
                     textcoords="offset points",
                     ha='center', va='bottom')  # Alinhamento centralizado na base
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
#* grafico em barras verticais de quantos amarelos cada time recebeu (deixar mais bonitinho)
janela = plt.figure(figsize=(10,5))
grafico = janela.add_axes([0,0,1,1])
bars = grafico.barh(amarelos_time['time'], amarelos_time['amarelos_time'])

plt.title("Cartoes Amarelos por Time no Brasileirao 2024", fontsize=15, fontweight="bold")
plt.xlabel("Quantia de Cartoes")
plt.ylabel("Times")

for bar in bars:
    width = bar.get_width()
    grafico.annotate(f'{width}',
                     xy=(width, bar.get_y() + bar.get_height()/2),
                     xytext=(3, 0),
                     textcoords="offset points",
                     ha='left', va='center')

plt.show()
# %%
#* Filtrando por NOME, quais foram os jogadores mais amarelados
# todo melhorar, colocar time de cada jogador e talvez seu apelido
amarelos_nome = (df_aaj
                 .groupby(by='nome_jogador', as_index=False)
                 .agg(amarelos_nome = ('nome_jogador', 'count'))
                 .sort_values('amarelos_nome', ascending=True)
                 )
amarelos_nome

# %%
mais_amarelados = amarelos_nome.tail(10)
mais_amarelados
# %%

janela = plt. figure(figsize=(10, 5))
grafico = janela.add_axes([0,0,1,1])
grafico.barh(mais_amarelados['nome_jogador'], mais_amarelados['amarelos_nome'])

plt.title('Jogadores que Mais Receberam Cartoes Amarelos no Brasileirao 2024', fontsize=15, fontweight="bold")
plt.xlabel('Quantia de Cartoes')
plt.ylabel('Nome do Jogador')

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

# %%
amarelos_comissao = (df_amarelos_comissao
                .groupby(by='time', as_index=False)
                .agg(amarelos_comissao = ('time', 'count'))
                .sort_values('amarelos_comissao', ascending=True)
        )
amarelos_comissao

# 154 cartoes totais
# %% Grafico de Comissao Tecnica Mais Indisciplinadas (por time comissoes q + recebem cartoes amarelos)
janela = plt.figure(figsize=(10,5))
grafico = janela.add_axes([0,0,1,1])
bars = grafico.barh(amarelos_comissao['time'], amarelos_comissao['amarelos_comissao'])

plt.title("Comissoes Tecnicas Mais Indisciplinadas", fontsize=15, fontweight="bold")
plt.xlabel("Quantia de Cartoes")
plt.ylabel("Times")

for bar in bars:
    width = bar.get_width()
    grafico.annotate(f'{width}',
                     xy=(width, bar.get_y() + bar.get_height()/2),
                     xytext=(3, 0),
                     textcoords="offset points",
                     ha='left', va='center')

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

janela = plt.figure(figsize=(10,5))
grafico = janela.add_axes([0,0,1,1])
bars = grafico.barh(amarelos_tecnicos['nome'],amarelos_tecnicos['amarelos_tecnicos'])

plt.title('Os Tecnicos mais Amarelados no Brasileirao 2024', fontsize=15, fontweight='bold')
plt.xlabel('Quantia de Cartoes')
plt.ylabel('Nome')

for bar in bars:
    width = bar.get_width()
    grafico.annotate(f'{width}',
                     xy=(width, bar.get_y() + bar.get_height()/2),
                     xytext=(3, 0),
                     textcoords="offset points",
                     ha='left', va='center')

plt.show()
# %%
