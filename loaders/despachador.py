import psycopg2 as psql
import loaders.tools as loader

conexion_insana = loader.connect()
cur = conexion_insana.cursor()


csv_path = "./data/cldeldes.csv"

table = loader.load_table(csv_path)

cur.execute(
    """CREATE TABLE Despachador(
    nombre VARCHAR(30),
    telefono VARCHAR(9) PRIMARY KEY
    );"""
)
conexion_insana.commit()

for linea in table["datos"]:

    clientenombre, clienteemail, clientetelefono, clienteclave, deliverynombre, deliveryvigente, deliverytelefono, deliverytiempo, deliverypreciounitario, deliverypreciomensual, deliveryprecioanual, despachadornombre, despachadortelefono = linea

    cur.execute(
        "INSERT INTO DESPACHADOR(nombre, telefono) VALUES (%s, %s) ON CONFLICT (telefono) DO NOTHING",
        (despachadornombre[:30], despachadortelefono[2:])
    )

conexion_insana.commit()
cur.close()
loader.disconnect(conexion_insana)