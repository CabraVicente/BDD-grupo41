import psycopg2 as psql
import os

conexion_insana = psql.connect(
    database="grupo41e2",
    user="grupo41",
    host="pavlov.ing.puc.cl",
    password="2_balas",
    port=5432
)

cur = conexion_insana.cursor()

cur.execute("DROP TABLE IF EXISTS CLIENTES;")

script_dir = os.path.dirname(__file__)
csv_path = os.path.join(script_dir, "../datos/clientes.csv")
#ola cabra por ahora voy a cambiar este path pa poder ejecutar la cosa despues vemos como arreglarlo bien
csv_path = "./data/clientes.csv"

with open(csv_path, "r") as clientes:
    cliente = clientes.readlines()
headers = cliente[0].strip().split(";")

# comuna como tal contiene nombre de comuna y su cut, esto sirve para pasarlo a tupla
# en base a esto podrÃ­amos sacar uno de los dos (ejemplo: cut) y lo conectamos con la tabla de comunas.
# EN CASO DE TOMAR CUT DEJAR EL ÃšLTIMO ATRIBUTO COMO 'comuna_cut INTEGER', y especificar en el informe.
# Para la limpieza ver datos que no cumplan con los requisitos.
cur.execute(
    """CREATE TABLE CLIENTES(
    nombre VARCHAR(30),
    correo VARCHAR(30) PRIMARY KEY,
    telefono VARCHAR(11),
    clave VARCHAR(30),
    direccion VARCHAR(30),
    comuna_cut INTEGER
    );"""
)
conexion_insana.commit()

# RECORDAR: encriptar clave
for linea in cliente[1:]:
    if (linea.count(";") < 5 or linea.count('"') % 2 == 1):
        print("ERROR en linea: "+linea)
        continue

    nombre, correo, telefono, clave, direccion, comuna_cut = linea.strip().split(";")
    if (len(nombre) > 30 or len(correo) > 30 or len(telefono) > 11 or len(clave) > 30 or len(direccion) > 30):
        print("dato no calza: "+linea)
        continue

    cur.execute(
        "INSERT INTO CLIENTES(nombre, correo, telefono, clave, direccion, comuna_cut) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (correo) DO NOTHING",
        (nombre, correo, telefono[2:], clave, direccion, int(comuna_cut))
    )
# En caso de que el loader no estÃ© del todo completo, aÃ±adir crear tabla 'CLIENTE' y dropear 'CLIENTES'

conexion_insana.commit()
cur.close()
conexion_insana.close()