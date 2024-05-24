import psycopg2 as psql
import loaders.tools as loader
import hashlib
import string

conexion_insana = loader.connect()

cur = conexion_insana.cursor()

csv_path = "./data/clientes.csv"

table = loader.load_table(csv_path)

cur.execute(
    """CREATE TABLE Cliente(
    nombre VARCHAR(30),
    correo VARCHAR(60) PRIMARY KEY,
    telefono VARCHAR(9),
    clave_hash VARCHAR(68)
    );"""
)
conexion_insana.commit()

# RECORDAR: encriptar clave
for linea in table["datos"]:

    nombre, correo, telefono, clave, direccion, comuna_cut = linea
    
    encrypted = hashlib.sha256(clave.encode()).hexdigest()
    encrypted = encrypted[:16] + " " + encrypted[16:32] + " " + encrypted[32:48] + " " + encrypted[48:]

    cur.execute(
        "INSERT INTO Cliente(nombre, correo, telefono, clave_hash) VALUES (%s, %s, %s, %s) ON CONFLICT (correo) DO NOTHING;",
        (nombre[:30], correo[:60], telefono[2:11], encrypted)
    )

conexion_insana.commit()
cur.close()
loader.disconnect(conexion_insana)