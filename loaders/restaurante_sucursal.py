import psycopg2 as psql
import loaders.tools as loader

conn = loader.connect()
cur = conn.cursor()

data = loader.load_table("./data/restaurantes.csv")

cur.execute(
    """CREATE TABLE Restaurante(
    nombre VARCHAR(30) PRIMARY KEY,
    estilo TEXT,
    repartomin INT
    );"""
)
cur.execute(
    """CREATE TABLE Sucursal(
    sucursal TEXT,
    restaurante_nombre VARCHAR(30) references Restaurante(nombre),
    direccion VARCHAR(30),
    telefono TEXT NOT NULL,
    area_de_despacho TEXT,
    PRIMARY KEY (sucursal, restaurante_nombre)
    );"""
)

for fila in data["datos"]:
    nombre, vigente, estilo, repartomin, sucursal, direccion, telefono, area = fila
    cur.execute(
        "INSERT INTO Restaurante(nombre, estilo, repartomin) VALUES (%s, %s, %s) ON CONFLICT (nombre) DO NOTHING",
        (nombre, estilo, int(repartomin))
    )

    cur.execute(
        "INSERT INTO Sucursal(sucursal, restaurante_nombre, direccion, telefono, area_de_despacho) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (sucursal, restaurante_nombre) DO NOTHING",
        (sucursal, nombre[:30], direccion[:30], telefono, area)
    )

conn.commit()
cur.close()
loader.disconnect(conn)