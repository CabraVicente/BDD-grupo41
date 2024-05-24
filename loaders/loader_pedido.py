import psycopg2 as psql
import loader_tools as loader

conn = loader.connect()
cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS PEDIDO;")


data = loader.load_table("./data/cldeldes.csv")

#cur.execute(
#    """CREATE TABLE PEDIDO (
#    id INTEGER PRIMARY KEY,
#    estado TEXT,
#    fecha_hora TIMESTAMP,
#    rating_pedido FLOAT,
#    rating_cliente FLOAT
#    );"""
#)

# Creamos las tablas necesarias para obtener los ratings
cur.execute(
    """CREATE TABLE PEDIDOS (
    id INTEGER PRIMARY KEY,
    estado TEXT,
    fecha_hora TIMESTAMP
    );"""
)

cur.execute(
    """CREATE TABLE CALIFICACION (
    id_pedido INTEGER,
    calif_pedido FLOAT,
    calif_cliente FLOAT
    );"""
)

data = loader.load_table("./data/pedidos.csv")
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

cur.execute(
    """ CREATE TABLE ARTIFICIAL AS
        SELECT pedidos.id, pedidos.estado, pedidos.fecha_hora, calificacion.calif_pedido, calificacion.calif_cliente
        FROM pedidos
        JOIN calificacion ON pedidos.id = calificacion.id_pedido;
    """
)
cur.execute(
    """ CREATE TABLE PEDIDO AS
        SELECT pedidos.id AS id, 
        pedidos.estado AS estado, 
        pedidos.fecha_hora AS fecha_hora, 
        calificacion.calif_pedido AS rating_pedido, 
        calificacion.calif_cliente AS rating_cliente
        FROM ARTIFICIAL
    """
)

# Obliteramos las tablas con las que obtuvimos los ratings
cur.execute("DROP TABLE PEDIDOS")
cur.execute("DROP TABLE CALIFICACION")
cur.execute("DROP TABLE ARTIFICIAL")

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