import psycopg2 as psql
import loaders.tools as loader

conexion_insana = loader.connect()
cur = conexion_insana.cursor()


csv_path = "./data/cldeldes.csv"

table = loader.load_table(csv_path)

cur.execute(
    """CREATE TABLE Despachador(
    nombre VARCHAR(60),
    telefono VARCHAR(9) PRIMARY KEY
    );
    
    CREATE TABLE EmpresaDelivery_Despachador(
        empresa_nombre VARCHAR(30) REFERENCES EmpresaDelivery(nombre),
        despachador_telefono VARCHAR(9) REFERENCES Despachador(telefono),
        PRIMARY KEY (empresa_nombre, despachador_telefono)
    );
    """
)

for linea in table["datos"]:

    clientenombre, clienteemail, clientetelefono, clienteclave, deliverynombre, deliveryvigente, deliverytelefono, deliverytiempo, deliverypreciounitario, deliverypreciomensual, deliveryprecioanual, despachadornombre, despachadortelefono = linea

    cur.execute(
        "INSERT INTO Despachador(nombre, telefono) VALUES (%s, %s) ON CONFLICT (telefono) DO NOTHING",
        (despachadornombre[:60], despachadortelefono[2:])
    )

    cur.execute(
        """INSERT INTO EmpresaDelivery_Despachador(empresa_nombre, despachador_telefono)
        VALUES (%s, %s)
        ON CONFLICT (empresa_nombre, despachador_telefono) DO NOTHING
        """,
        (deliverynombre[:30], despachadortelefono[2:])
    )

conexion_insana.commit()
cur.close()
loader.disconnect(conexion_insana)