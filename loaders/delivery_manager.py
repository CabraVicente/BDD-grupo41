import psycopg2 as psql
import loader_tools as loader

conn = loader.connect()
cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS DELIVERY_MANAGER CASCADE;")


data = loader.load_table("./data/cldeldes.csv")

cur.execute(
    """CREATE TABLE DELIVERY_MANAGER(
    nombre VARCHAR(30) PRIMARY KEY,
    vigente BOOLEAN
    );"""
)

for fila in data["datos"]:
    clientenombre, clienteemail, clientetelefono, clienteclave, deliverynombre, deliveryvigente, deliverytelefono, deliverytiempo, deliverypreciounitario, deliverypreciomensual, deliveryprecioanual, despachadornombre, despachadortelefono = fila
    cur.execute(
        "INSERT INTO DELIVERY_MANAGER(nombre, vigente) VALUES (%s, %s) ON CONFLICT (nombre) DO NOTHING",
        (deliverynombre, True if deliveryvigente=="TRUE" else False)
    )
    conn.commit()
# En caso de que el loader no estÃ© del todo completo, aÃ±adir crear tabla 'CLIENTE' y dropear 'CLIENTES'

conn.commit()
cur.close()
conn.close()