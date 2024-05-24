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

cur.execute("DROP TABLE IF EXISTS DELIVERY_MANAGER;")

script_dir = os.path.dirname(__file__)
csv_path = "./data/cldeldes.csv"

with open(csv_path, "r") as texto:
    deliveries = texto.readlines()
headers = deliveries[0].strip().split(";")

cur.execute(
    """CREATE TABLE DELIVERY_MANAGER(
    nombre VARCHAR(30) PRIMARY KEY,
    vigente BOOLEAN
    );"""
)
conexion_insana.commit()

for linea in deliveries[1:]:
    if (linea.count(";") < 5 or linea.count('"') % 2 == 1):
        print("ERROR en linea: "+linea)
        continue

    clientenombre, clienteemail, clientetelefono, clienteclave, deliverynombre, deliveryvigente, deliverytelefono, deliverytiempo, deliverypreciounitario, deliverypreciomensual, deliveryprecioanual, despachadornombre, despachadortelefono = linea.strip().split(";")

    cur.execute(
        "INSERT INTO DELIVERY_MANAGER(nombre, vigente) VALUES (%s, %s) ON CONFLICT (nombre) DO NOTHING",
        (deliverynombre, bool(deliveryvigente))
    )
# En caso de que el loader no estÃ© del todo completo, aÃ±adir crear tabla 'CLIENTE' y dropear 'CLIENTES'

conexion_insana.commit()
cur.close()
conexion_insana.close()