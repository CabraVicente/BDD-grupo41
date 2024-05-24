import psycopg2 as psql
import loaders.tools as loader

conn = loader.connect()
cur = conn.cursor()


data = loader.load_table("./data/pedidos.csv")

cur.execute(
    """CREATE TABLE Pedido(
    id INTEGER PRIMARY KEY,
    estado TEXT,
    fecha_hora TIMESTAMP
    );"""
)

data = loader.load_table("./data/calificacion.csv")

cur.execute(
    """CREATE TABLE Calificacion(
    id_pedido INTEGER,
    calif_pedido FLOAT,
    calif_cliente FLOAT
    );"""
)

data = loader.load_table("./data/pedidos2.csv")
for x in data["datos"]:
    id, cliente, sucursal, delivery, despachador, plato, fecha, hora, estado = x
    cur.execute(
        "INSERT INTO PEDIDOS(id, estado, fecha_hora) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING",
        (int(id),estado,str(fecha, hora))
    )
    conn.commit()

data = loader.load_table("./data/calificacion.csv")
for y in data["datos"]:
    id_pedido, calif_pedido, calif_cliente = y
    cur.execute(
        "INSERT INTO CALIFICACION(id_pedido, calif_pedido, calif_cliente) VALUES (%s, %s, %s)",
        (int(id_pedido),float(calif_pedido),float(calif_cliente))
    )
    conn.commit()

conn.commit()
cur.close()
loader.disconnect(conn)