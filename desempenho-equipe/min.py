# %%
import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
# %%

gols_path = '../data/sa25_05_gols.parquet'
gols = pd.read_parquet(gols_path)
gols

# %% quantos gols cada time teve na temp
gols['time'].value_counts()

# %%
gols = gols.drop(columns=['comp', 'ano'])
gols

# %%

# tabela que mostre os gols marcados e minutos de um TIME na temporada
gols_inter = gols[gols['time'] == 'Internacional/RS']
gols_inter.head(50) 

# %% contando quantos gols o inter fez na temporada
gols_inter['jogo'].count()

# %%
def floatmaker(valor):
        if '+' in str(valor):
            minutos = float(str(valor).replace('+', '').strip()) + 45
        else: 
            minutos = float((str(valor).replace(':00', '').strip()))
        return minutos

gols_inter['floatmin'] = gols_inter['minutos'].apply(floatmaker)

gols_inter.head(50)

# %%

#  #*funcao de conversao dos minutos para um relogio de 90 min e adiciona os acrescimos no tempo.
# def convert_min(valor, tempo):
#         base = 45 if tempo == '2T' else 0
#         if '+' in str(valor): 
#             minutos = float(str(valor).replace('+', '').strip()) + 45
#         else:
#             minutos = float(str(valor).replace(':00', '').strip())  
#         return minutos + base
    
# gols_inter['floatmin'] = gols_inter.apply(
#      lambda x: convert_min(x['minutos'], x['tempo']),
#      axis=1
# )

# gols_inter.head(20)

# %%
gols_inter.sort_values(by='floatmin', ascending=False).head(20)

# %% #*criando coluna que mostra quantos gols foram feitos no minuto 

gols_inter['golsmin'] = gols_inter.groupby(['floatmin', 'tempo'])['floatmin'].transform('size')
gols_inter.sort_values(by='golsmin', ascending=False).head(20)

#%%
gols_inter.sort_values(by='floatmin', ascending=True)
# %%
#* criando novo DF que mostra todos os minutos de um jogo. inclusive aqueles q nao foram marcados gols

max_minutos = 60
todos_minutos = pd.DataFrame({'floatmin': range(1, max_minutos + 1)})
print(todos_minutos)
# %% fazendo merge do df dos gols com o df de minutos totais.

gols_inter_tudo = pd.merge(
    todos_minutos,
    gols_inter,
    on='floatmin',
    how='left'
)
gols_inter_tudo

#%% preenchendo os valores nulos na coluna de gols no minuto
gols_inter_tudo['golsmin'] = gols_inter_tudo['golsmin'].fillna(0)
gols_inter_tudo.head(20)
# %%
plt.figure(figsize=(10,8))
sns.lineplot(
    data=gols_inter_tudo,
    x='floatmin',
    y='golsmin'
)

# %%
gols_inter_tudo.sort_values(by='minutos', ascending=False).head(20)

# %%

# Primeiro, agrupar os dados por minuto e tempo
gols_por_minuto_tempo = gols_inter_tudo.groupby(['floatmin', 'tempo'])['golsmin'].size().unstack(fill_value=0)

# Renomear as colunas para melhor entendimento
gols_por_minuto_tempo.columns = ['1T', '2T']

# Resetar o índice para plotar
gols_por_minuto_tempo = gols_por_minuto_tempo.reset_index()

# %%
minutos = gols_por_minuto_tempo['floatmin']
gols_1T = gols_por_minuto_tempo['1T']
gols_2T = gols_por_minuto_tempo['2T']

fig, ax = plt.subplots(figsize=(20, 8))
ax.bar(minutos, gols_1T, 
       color="#fcf4f4", 
       label='1º Tempo', 
       edgecolor='black', 
       width=1.0) # width=1.0 para que as barras se toquem, criando um efeito de histograma

# Barras para o 2º Tempo (empilhadas sobre as do 1º)
ax.bar(minutos, gols_2T, 
       bottom=gols_1T, 
       color="#ff0000", 
       label='2º Tempo', 
       edgecolor='black', 
       width=1.0)

# 3. Personalizar o título e os rótulos dos eixos
ax.set_title('Distribuição de Gols do Internacional por Minuto', 
             fontsize=22, fontweight='bold', pad=2)
ax.text(0.5, 0.94, 'Brasileirão 2024',
        horizontalalignment='center',
        fontsize=16,
        color='gray',
        transform=ax.transAxes)
ax.set_xlabel('Minuto do Jogo', fontsize=16, fontweight='bold', labelpad=20)
ax.set_ylabel('Quantidade de Gols', fontsize=16, fontweight='bold', labelpad=20)

# 4. **(Correção Principal)** Mostrar TODOS os ticks do eixo X e rotacionar
# Criar um range de todos os minutos possíveis no jogo para garantir que o eixo seja contínuo
minuto_max = int(gols_por_minuto_tempo['floatmin'].max())
ax.set_xticks(np.arange(1, minuto_max + 2, 2))
ax.tick_params(axis='x', labelsize=12, rotation=0) # Rotacionar para evitar sobreposição

# 6. Melhorar a grade e remover bordas desnecessárias
ax.grid(axis='y', alpha=0.3, linestyle='--')
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

# 7. Configurar a legenda
ax.legend(title='Tempo', fontsize=12, title_fontsize=13, frameon=False, loc='upper left')

# 8. Ajustar os limites do eixo para um melhor enquadramento
ax.set_xlim(0, minuto_max + 5)
ax.set_ylim(0, gols_por_minuto_tempo[['1T', '2T']].sum(axis=1).max() * 1.1) # 10% de espaço acima da maior barra

# Ajustar layout para garantir que nada seja cortado
plt.tight_layout()
plt.show()
# %%
#todo fiz para os gols marcados, agora fazer para os gols sofridos 
#todo + fazer uma especie de automatizacao por time, para nao ter q criar um df por time.