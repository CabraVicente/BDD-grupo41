import psycopg2 as psql
import loader_tools as loader

conn = loader.connect()
cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS Restaurante CASCADE;")
cur.execute("DROP TABLE IF EXISTS Sucursal;")

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
    sucursal TEXT PRIMARY KEY,
    direccion VARCHAR(30),
    telefono TEXT,
    area_de_despacho TEXT,
    restaurante_nombre VARCHAR(30),
    CONSTRAINT restaurante_nombre
        FOREIGN KEY(restaurante_nombre)
            REFERENCES Restaurante(nombre)
    );"""
)
conn.commit()

for fila in data["datos"]:
    nombre, vigente, estilo, repartomin, sucursal, direccion, telefono, area = fila
    cur.execute(
        "INSERT INTO Restaurante(nombre, estilo, repartomin) VALUES (%s, %s, %s) ON CONFLICT (nombre) DO NOTHING",
        (nombre, estilo, int(repartomin))
    )
    conn.commit()
    cur.execute(
        "INSERT INTO Sucursal(sucursal, restaurante_nombre, direccion, telefono, area_de_despacho) VALUES (%s, %s, %s, %s, %s)",
        (sucursal, nombre[:30], direccion[:30], telefono, area)
    )
# En caso de que el loader no estÃ© del todo completo, aÃ±adir crear tabla 'CLIENTE' y dropear 'CLIENTES'

conn.commit()
cur.close()
conn.close()