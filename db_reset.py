import psycopg2 as psql
import loaders.tools as loader

conn = loader.connect()

cur = conn.cursor()

tables = [
    "Cliente",
    "Comuna",
    "Direccion",
    "DeliveryManager",
    "Despachador",
    #"Pedido",
    #"Calificacion",
    "Plato",
    "Restaurante",
    "Sucursal"
]

for table in tables:
    cur.execute("DROP TABLE IF EXISTS " + table + " CASCADE;")
    print("Dropped table '" + table + "'")

conn.commit()
cur.close()
loader.disconnect(conn)