import psycopg2 as psql
import loaders.tools as loader
import hashlib
import string

conexion_insana = loader.connect()

cur = conexion_insana.cursor()

csv_path = "./data/cldeldes.csv"

table = loader.load_table(csv_path)

cur.execute(
    """CREATE TABLE Despacho(
    sucursal TEXT,
    restaurante_nombre VARCHAR(30),
    pedido_id INT REFERENCES Pedido(id),
    despachador_telefono VARCHAR(9) REFERENCES Despachador(telefono),
    FOREIGN KEY (sucursal, restaurante_nombre) REFERENCES Sucursal(sucursal, restaurante_nombre)
    PRIMARY KEY (pedido_id, sucursal, restaurante_nombre)
    );"""
)

# RECORDAR: encriptar clave
for linea in table["datos"]:

    nombre, correo, telefono, clave, direccion, comuna_cut = linea
    
    encrypted = hashlib.sha256(clave.encode()).hexdigest()

    cur.execute(
        "INSERT INTO Cliente(nombre, correo, telefono, clave_hash) VALUES (%s, %s, %s, %s) ON CONFLICT (correo) DO NOTHING;",
        (nombre[:30], correo[:60], telefono[2:11], encrypted)
    )

conexion_insana.commit()
cur.close()
loader.disconnect(conexion_insana)