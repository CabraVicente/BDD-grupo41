import psycopg2 as psql
import csv
import re

def connect():
    conn = psql.connect(
        database="grupo41e2",
        user="grupo41",
        host="pavlov.ing.puc.cl",
        password="2_balas",
        port=5432
    )

    return conn

def load_table(filename, delimiter = ";", encoding = "utf-8"):
    data = []
    bad_chars = r'[^a-zA-Z:";0-9 ,.\n-@]'
    
    to_fix = []
    with open(filename, "r", encoding=encoding) as f:
        to_fix = f.readlines()
    
    with open("data/temp.csv", "w") as f:
        for line in to_fix:
            f.write(re.sub(bad_chars, "_", line))

    with open("data/temp.csv", "r", newline="", encoding=encoding) as f:
        datos = csv.reader(f, delimiter=delimiter)
        for i in datos:
            data.append(i)
    
    return {"nombres": data[0], "datos": data[1:]}
        

def disconnect(connection):
    connection.close()