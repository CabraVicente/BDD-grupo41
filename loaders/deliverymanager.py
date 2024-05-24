import psycopg2 as psql
import loaders.tools as loader

conn = loader.connect()
cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS DeliveryManager CASCADE;")


data = loader.load_table("./data/cldeldes.csv")

cur.execute(
    """CREATE TABLE DeliveryManager(
    nombre VARCHAR(30) PRIMARY KEY,
    vigente BOOLEAN
    );"""
)

for fila in data["datos"]:
    clientenombre, clienteemail, clientetelefono, clienteclave, deliverynombre, deliveryvigente, deliverytelefono, deliverytiempo, deliverypreciounitario, deliverypreciomensual, deliveryprecioanual, despachadornombre, despachadortelefono = fila
    cur.execute(
        "INSERT INTO DeliveryManager(nombre, vigente) VALUES (%s, %s) ON CONFLICT (nombre) DO NOTHING",
        (deliverynombre, True if deliveryvigente=="TRUE" else False)
    )
    conn.commit()

conn.commit()
cur.close()
loader.disconnect(conn)