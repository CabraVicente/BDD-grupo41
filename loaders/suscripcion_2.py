import psycopg2 as psql
import loaders.tools as loader

def buscador_precios(lista1,nombre, tipo):
    # Por loader lista1 debería ser una lista de listas
    # tipo es "anual" o "mensual"
    for fila in lista1:
        if fila[4] == nombre:
            if tipo == "mensual":
                return int(fila[9])
            else:
                return int(fila[10])
            # dentro del mismo archivo se encuentran los precios mensuales y anuales
conn = loader.connect()
cur = conn.cursor()

data = loader.load_table("./data/suscripciones.csv")
data2 = loader.load_table("./data/cldeldes.csv")

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
    if ciclo == "anual":
        fecha_proximopago = str(int(fecha_ultimo[:4])+1)+fecha_ultimo[4:]
        precio = buscador_precios(data2,nombre,"anual")
    else:
        fecha_proximopago = fecha_ultimo[:5]+str(int(fecha_ultimo[5:7])+1)+fecha_ultimo[7:]
        precio = buscador_precios(data2,nombre,"mensual")
    cur.execute(
        "INSERT INTO DeliveryManager(correo_cliente, nombre_empresa, medio_de_pago, fecha_prox_pago, estado, fecha_ultimo_pago, monto_ultimo_pago, ciclo, precio) VALUES (%s, %s, %s, %s, %s, %s, %s, %0s, %s) ON CONFLICT (correo_cliente, nombre_empresa) DO NOTHING",
        (email, nombre, "débito", fecha_proximopago ,estado, fecha_ultimopago, ultimopago, ciclo, precio)
    )

conn.commit()
cur.close()
loader.disconnect(conn)