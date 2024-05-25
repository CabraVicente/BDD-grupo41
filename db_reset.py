import psycopg2 as psql
import loaders.tools as loader

conn = loader.connect()

cur = conn.cursor()

tables = [
    "Cliente",
    "Comuna",
    "Direccion",
    "EmpresaDelivery",
    "Despachador",
    "Pedido",
    "Calificacion",
    "Plato",
    "Pedido_Plato",
    "Restaurante",
    "Sucursal",
    "Suscripcion"
]

for table in tables:
    cur.execute("DROP TABLE IF EXISTS " + table + " CASCADE;")
    print("Dropped table '" + table + "'")

conn.commit()
cur.close()
loader.disconnect(conn)