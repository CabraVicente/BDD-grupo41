import psycopg2 as psql
import loaders.tools as loader


# Opcion con SQL, no sé si funcione, hay que probarla

conn = loader.connect()
cur = conn.cursor()

data = loader.load_table("./data/suscripciones.csv")
data2 = loader.load_table(".data/cldeldes.csv")

cur.execute(
    """CREATE TABLE Artificial(
        nombre VARCHAR(30) PRIMARY KEY,
        precio_fijo INTEGER,
        precio_mensual INTEGER,
        precio_anual INTEGER
    )
    """
)
for elemento in data2["datos"]:
    nombre = elemento[4]
    precio = elemento[8]
    precio_mensual = elemento[9]
    precio_anual = elemento[10]
    cur.execute(
        "INSERT INTO Artificial(nombre, precio_fijo, precio_mensual, precio_anual) VALUES(%s, %s, %s, %s) ON CONFLICT (nombre) DO NOTHING"
    )


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
        cur.execute("SELECT precio_anual FROM Artificial WHERE nombre = %s", nombre)
        precio = cur.fetchone[0]
    else:
        fecha_proximopago = fecha_ultimo[:5]+str(int(fecha_ultimo[5:7])+1)+fecha_ultimo[7:]
        cur.execute("SELECT precio_mensual FROM Artificial WHERE nombre = %s", nombre)
        precio = cur.fetchone[0]
    cur.execute(
        "INSERT INTO DeliveryManager(correo_cliente, nombre_empresa, medio_de_pago, fecha_prox_pago, estado, fecha_ultimo_pago, monto_ultimo_pago, ciclo, precio) VALUES (%s, %s, %s, %s, %s, %s, %s, %0s, %s) ON CONFLICT (correo_cliente, nombre_empresa) DO NOTHING",
        (email, nombre, "débito", fecha_proximopago ,estado, fecha_ultimopago, ultimopago, ciclo, "precio")
    )
    conn.commit()

cur.execute("DROP TABLE Artificial")

conn.commit()
cur.close()
loader.disconnect(conn)