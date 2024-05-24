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

cur.execute("DROP TABLE IF EXISTS DESPACHADOR;")

script_dir = os.path.dirname(__file__)
csv_path = "./data/cldeldes.csv"

with open(csv_path, "r") as texto:
    despachadores = texto.readlines()
headers = despachadores[0].strip().split(";")

cur.execute(
    """CREATE TABLE DESPACHADOR(
    nombre VARCHAR(30),
    telefono VARCHAR(9)
    );"""
)
conexion_insana.commit()

# RECORDAR: encriptar clave
for linea in despachadores[1:]:
    print(linea)
    clientenombre, clienteemail, clientetelefono, clienteclave, deliverynombre, deliveryvigente, deliverytelefono, deliverytiempo, deliverypreciounitario, deliverypreciomensual, deliveryprecioanual, despachadornombre, despachadortelefono = linea.strip().split(";")
    """ if (linea.count(";") < 5 or linea.count('"') % 2 == 1):
        print("ERROR en linea: "+linea)
        continue
    if (len(nombre) > 30 or len(correo) > 30 or len(telefono) > 11 or len(clave) > 30 or len(direccion) > 30):
        print("dato no calza: "+linea)
        continue
"""
    cur.execute(
        "INSERT INTO DESPACHADOR(nombre, telefono) VALUES (%s, %s)",
        (despachadornombre, despachadortelefono[2:])
    )
# En caso de que el loader no estÃ© del todo completo, aÃ±adir crear tabla 'CLIENTE' y dropear 'CLIENTES'

conexion_insana.commit()
cur.close()
conexion_insana.close()