import psycopg2 as psql
import loaders.tools as loader

conn = loader.connect()
cur = conn.cursor()

data = loader.load_table("./data/cldeldes.csv")

#Revisar que hacer con precio, ya que para ello necesitamos sacar información de deliverymanager
cur.execute(
    """CREATE TABLE Suscripcion(
    correo_cliente VARCHAR(64) FOREIGN KEY,
    nombre_empresa VARCHAR(30) FOREIGN KEY,
    medio_de_pago VARCHAR(30),
    fecha_prox_pago DATE,
    estado VARCHAR(30),
    fecha_ultimo_pago DATE,
    monto_ultimo_pago INT,
    ciclo TEXT NO NULL,
    precio INTEGER
    PRIMARY KEY (correo_cliente, nombre_empresa)
    );"""
)

for fila in data["datos"]:
    email, nombre, estado, ultimopago, fecha_ultimopago, ciclo = fila
    fecha_ultimo = "20"+fecha_ultimopago[6:]+fecha_ultimopago[2:6]+fecha_ultimopago[:2]
    precio = loader.precio_delivery(nombre) * 4
    if ciclo == "anual":
        precio = precio * 12
        fecha_proximopago = str(int(fecha_ultimo[:4])+1)+fecha_ultimo[4:]
    else:
        fecha_proximopago = fecha_ultimo[:5]+str(int(fecha_ultimo[5:7])+1)+fecha_ultimo[7:]
    cur.execute(
        "INSERT INTO DeliveryManager(correo_cliente, nombre_empresa, medio_de_pago, fecha_prox_pago, estado, fecha_ultimo_pago, monto_ultimo_pago, ciclo, precio) VALUES (%s, %s, %s, %s, %s, %s, %s, %0s, %s) ON CONFLICT (correo_cliente, nombre_empresa) DO NOTHING",
        (email, nombre, "débito", fecha_proximopago ,estado, fecha_ultimopago, ultimopago, ciclo, precio)
    )
    conn.commit()

conn.commit()
cur.close()
loader.disconnect(conn)