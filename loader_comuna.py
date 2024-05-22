# Ayuda me wa demenciar ;-----;
import psycopg2 as psql

conexion_insana = psql.connect(
    database = "grupo41",
    user = "grupo41",
    host = "localhost",
    password = "2_balas",
    port = 5432
)

cur = conexion_insana.cursor()

cur.execute(
    """DROP TABLE COMUNAS;"""
)

with open("/datos/comunas.csv","r") as comuna:
    comunas = comuna.readlines()
headers = comunas[0].split(",")

cur.execute(
    """CREATE TABLE COMUNAS(cut INTEGER PRIMARY KEY, nombre VARCHAR(30), provincia VARCHAR(30), region VARCHAR(30));"""
)
conexion_insana.commit()

for comuna in comunas[1:]:
    cut, nombre, provincia, region = comuna.split(",")
    cur.execute(
    f"""INSERT INTO COMUNAS(cut, nombre, provincia, region) VALUES({int(cut)},{nombre},{provincia},{region});""")
    conexion_insana.commit()
    
cur.close()
conexion_insana.close()
