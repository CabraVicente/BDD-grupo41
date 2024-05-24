import psycopg2 as psql
import os

# Conexión a la base de datos
conexion_insana = psql.connect(
    database="grupo41e2",
    user="grupo41",
    host="pavlov.ing.puc.cl",
    password="2_balas",
    port=5432
)

cur = conexion_insana.cursor()

# Elimina la tabla si existe
cur.execute("DROP TABLE IF EXISTS COMUNAS;")

# Construye la ruta absoluta al archivo CSV
script_dir = os.path.dirname(__file__)
csv_path = os.path.join(script_dir, "../datos/comunas.csv")

# Lee el archivo CSV
with open("./data/comuna.csv", "r") as comuna:
    comunas = comuna.readlines()
headers = comunas[0].strip().split(",")

# Crea la tabla COMUNAS
cur.execute(
    """CREATE TABLE COMUNAS(
    cut INTEGER PRIMARY KEY, 
    nombre VARCHAR(30), 
    provincia VARCHAR(60), 
    region VARCHAR(60)
    );"""
)
conexion_insana.commit()

# Inserta los datos en la tabla COMUNAS
for comuna in comunas[1:]:
    cut, nombre, provincia, region = [campo.strip().strip('"') for campo in comuna.strip().split(",")]
    cur.execute(
        "INSERT INTO COMUNAS (cut, nombre, provincia, region) VALUES (%s, %s, %s, %s)",
        (int(cut), nombre, provincia, region)
    )

conexion_insana.commit()
cur.close()
conexion_insana.close()