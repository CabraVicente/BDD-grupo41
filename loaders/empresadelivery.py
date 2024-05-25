import psycopg2 as psql
import loaders.tools as loader

conn = loader.connect()
cur = conn.cursor()

data = loader.load_table("./data/cldeldes.csv")

cur.execute(
    """CREATE TABLE EmpresaDelivery(
    nombre VARCHAR(30) PRIMARY KEY,
    vigente BOOLEAN,
    precio_unitario INT,
    precio_sus_mensual INT,
    precio_sus_anual INT
    );"""
)

for fila in data["datos"]:
    clientenombre, clienteemail, clientetelefono, clienteclave, deliverynombre, deliveryvigente, deliverytelefono, deliverytiempo, deliverypreciounitario, deliverypreciomensual, deliveryprecioanual, despachadornombre, despachadortelefono = fila
    cur.execute(
        "INSERT INTO EmpresaDelivery(nombre, vigente, precio_unitario, precio_sus_mensual, precio_sus_anual) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (nombre) DO NOTHING",
        (deliverynombre, deliveryvigente, deliverypreciounitario, deliverypreciomensual, deliveryprecioanual)
    )

conn.commit()
cur.close()
loader.disconnect(conn)