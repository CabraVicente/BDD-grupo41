import psycopg2 as psql
import csv

def connect() -> psql.connection:
    conn = psql.connect(
        database="grupo41e2",
        user="grupo41",
        host="pavlov.ing.puc.cl",
        password="2_balas",
        port=5432
    )

    return conn

def load_table(filename):
    data = []
    with open(filename, "r", newline="") as f:
        datos = csv.reader(f, delimiter=";")
        for i in datos:
            data.append(i)
    
    return {"nombres": data[0], "datos": data[1:]}
        

def disconnect(connection):
    connection.close()