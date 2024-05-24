import psycopg2 as psql
import os

conexion_insana = psql.connect(
    database="grupo41e2",
    user="grupo41",
    host="localhost",
    password="2_balas",
    port=5432
)

cur = conexion_insana.cursor()

cur.execute("DROP TABLE IF EXISTS CLIENTES;")

script_dir = os.path.dirname(__file__)
csv_path = os.path.join(script_dir, "../datos/clientes.csv")

with open(csv_path, "r") as clientes:
    cliente = clientes.readlines()
headers = cliente[0].strip().split(";")

# comuna como tal contiene nombre de comuna y su cut, esto sirve para pasarlo a tupla
# en base a esto podríamos sacar uno de los dos (ejemplo: cut) y lo conectamos con la tabla de comunas.
# EN CASO DE TOMAR CUT DEJAR EL ÚLTIMO ATRIBUTO COMO 'comuna_cut INTEGER', y especificar en el informe.
# Para la limpieza ver datos que no cumplan con los requisitos.
cur.execute(
    """CREATE TABLE CLIENTES(
    nombre VARCHAR(30),
    correo VARCHAR(60) PRIMARY KEY,
    telefono VARCHAR(9),
    clave VARCHAR(30),
    direccion VARCHAR(60),
    comuna_cut INTEGER
    );"""
)
conexion_insana.commit()

# RECORDAR: encriptar clave
for linea in cliente[1:]:
    nombre, correo, telefono, clave, direccion, comuna_cut = linea.strip().split(";")
    cur.execute(
        "INSERT INTO CLIENTES(nombre, correo, telefono, clave, direccion, comuna_cut) VALUES (%s, %s, %s, %s, %s, %s)",
        (nombre, correo, telefono[2:], clave, direccion, int(comuna_cut))
    )
# En caso de que el loader no esté del todo completo, añadir crear tabla 'CLIENTE' y dropear 'CLIENTES'

conexion_insana.commit()
cur.close()
conexion_insana.close()