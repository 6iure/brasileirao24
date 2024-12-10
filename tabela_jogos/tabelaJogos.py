import psycopg2

conn = psycopg2.connect(

    dbname = "brasileirao24",
    user = "postgres",
    password = "postgres",
    host = "localhost"
)

cursor = conn.cursor()

table_creation = '''
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
        atraso_inicio_1t INTEGER NOT NULL, --diferente do DF
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
        acr2_t INTEGER NOT NULL,
        mres1 INTEGER NOT NULL,
        vres1 INTEGER NOT NULL,
        mres2 INTEGER NOT NULL,
        vres2 INTEGER NOT NULL,
        mpen BOOLEAN DEFAULT NULL,
        vpen BOOLEAN DEFAULT NULL
    )
'''

cursor.execute(table_creation)

conn.commit()
cursor.close()
conn.close()
