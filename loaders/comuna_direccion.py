import psycopg2 as psql
import loaders.tools as loader

conexion_insana = loader.connect()
cur = conexion_insana.cursor()

table_comuna = loader.load_table("data/comuna.csv", ",")
table_cliente = loader.load_table("data/clientes.csv")


cur.execute(
    """CREATE TABLE Comuna(
    cut INT PRIMARY KEY, 
    region VARCHAR(60),
    provincia VARCHAR(60),
    nombre VARCHAR(30)
    );"""
)

cur.execute(
    """CREATE TABLE Direccion(
    comuna_cut INT REFERENCES Comuna(cut),
    persona_correo VARCHAR(60) REFERENCES Cliente(correo),
    direccion TEXT,
    PRIMARY KEY (comuna_cut, persona_correo, direccion)
    );"""
)

# Inserta los datos en la tabla Comuna
for comuna in table_comuna["datos"]:
    cut, nombre, provincia, region = comuna
    cur.execute(
        "INSERT INTO Comuna (cut, region, provincia, nombre) VALUES (%s, %s, %s, %s)",
        (int(cut), region, provincia, nombre)
    )

# Direcciones
for fila in table_cliente["datos"]:
    nombre, correo, telefono, clave, direccion, comuna_cut = fila
    cur.execute(
        "INSERT INTO Direccion (comuna_cut, persona_correo, direccion) VALUES (%s, %s, %s) ON CONFLICT (comuna_cut, persona_correo, direccion) DO NOTHING",
        (int(cut), correo, direccion)
    )

conexion_insana.commit()
cur.close()
loader.disconnect(conexion_insana)