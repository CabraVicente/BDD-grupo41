import psycopg2 as psql
import loader_tools as loader

conn = loader.connect()
cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS Restaurante;")
cur.execute("DROP TABLE IF EXISTS Sucursal;")

data = loader.load_table("./data/restaurantes.csv")

cur.execute(
    """CREATE TABLE Restaurante(
    id_restaurante VARCHAR(30) PRIMARY KEY,
    nombre VARCHAR(30),
    estilo TEXT,
    precio_minimo_despacho_gratis INT
    );"""
)
cur.execute(
    """CREATE TABLE Sucursal(
    nombre TEXT PRIMARY KEY,
    CONSTRAINT id_restaurante
        FOREIGN KEY(id_restaurante)
            REFERENCES Restaurante(id_restaurante)
    direccion VARCHAR(30),
    telefono TEXT
    area_de_despacho TEXT
    );"""
)
conn.commit()

# RECORDAR: encriptar clave
for linea in cliente[1:]:
    if (linea.count(";") < 5 or linea.count('"') % 2 == 1):
        print("ERROR en linea: "+linea)
        continue

    nombre, correo, telefono, clave, direccion, comuna_cut = linea.strip().split(";")
    if (len(nombre) > 30 or len(correo) > 30 or len(telefono) > 11 or len(clave) > 30 or len(direccion) > 30):
        print("dato no calza: "+linea)
        continue

    cur.execute(
        "INSERT INTO CLIENTES(nombre, correo, telefono, clave, direccion, comuna_cut) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (correo) DO NOTHING",
        (nombre, correo, telefono[2:], clave, direccion, int(comuna_cut))
    )
# En caso de que el loader no estÃ© del todo completo, aÃ±adir crear tabla 'CLIENTE' y dropear 'CLIENTES'

conexion_insana.commit()
cur.close()
conexion_insana.close()