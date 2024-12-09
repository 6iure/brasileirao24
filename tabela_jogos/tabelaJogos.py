import psycopg2

conn = psycopg2.connect(

    dbname = "brasileirao24",
    user = "postgres",
    password = "postgres",
    host = "localhost"
)