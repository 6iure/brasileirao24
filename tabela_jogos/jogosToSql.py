import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# fazendo a conexão com o db
conn_string = 'postgresql://postgres:postgres@localhost:5432/brasileirao24'
  
db = create_engine(conn_string) 
conn = db.connect() 
conn1 = psycopg2.connect( 
    database="brasileirao24", 
    user='postgres',  
    password='postgres',  
    host='localhost',  
    port= '5432'
) 

conn1.autocommit = True
cursor = conn1.cursor()

cursor.execute('drop table if exists jogos')

sql = '''
       CREATE TABLE jogos (
        competicao TEXT NOT NULL,
        comp TEXT NOT NULL,
        ano INTEGER NOT NULL,
        mandante TEXT NOT NULL,
        visitante TEXT NOT NULL,
        data DATE NOT NULL,
        hora TIME NOT NULL,
        estadio TEXT NOT NULL,
        cidade TEXT NOT NULL,
        entrada_m1 TIME NOT NULL,
        atraso_m1 INTEGER NOT NULL,
        entrada_v1 TIME NOT NULL,
        atraso_v1 INTEGER NOT NULL,
        atraso_inicio_1t INTEGER NOT NULL,
        inicio_1t TIME NOT NULL,
        fim_1t TIME NOT NULL,
        acr1_t INTEGER NOT NULL, 
        entrada_m2 TIME NOT NULL,
        atraso_m2 INTEGER NOT NULL,
        entrada_v2 TIME NOT NULL,
        atraso_v2 INTEGER NOT NULL,
        inicio_2t TIME NOT NULL,
        atraso_2t INTEGER NOT NULL,
        fim_2t TIME NOT NULL,
        acr_2t INTEGER NOT NULL,
        mres1 INTEGER NOT NULL,
        vres1 INTEGER NOT NULL,
        mres2 INTEGER NOT NULL,
        vres2 INTEGER NOT NULL,
        mpen BOOLEAN DEFAULT NULL,
        vpen BOOLEAN DEFAULT NULL
    );
'''

cursor.execute(sql)

data = pd.read_parquet('data/sa24_01_jogos.parquet')

data = data.rename(columns={"me1": "entrada_m1",
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
                              "fim2": "fim_2t",
                              "acr2": "acr_2t"
                              })

data = data[["competicao",
        "comp",
        "ano",
        "mandante",
        "visitante",
        "data",
        "hora",
        "estadio",
        "cidade",
        "entrada_m1",
        "atraso_m1",
        "entrada_v1",
        "atraso_v1",
        "atraso_inicio_1t",
        "inicio_1t",
        "fim_1t",
        "acr_1t",
        "entrada_m2",
        "atraso_m2",
        "entrada_v2",
        "atraso_v2",
        "inicio_2t",
        "atraso_2t",
        "fim_2t",
        "acr_2t",
        "mres1",
        "vres1",
        "mres2",
        "vres2",
        "mpen",
        "vpen"]]

print(data)

data.to_sql('jogos', conn, if_exists="replace", index=False)

# fetching all rows 
sql1='''select * from jogos;'''
cursor.execute(sql1) 
for i in cursor.fetchall(): 
    print(i) 
  
conn1.commit() 
conn1.close() 