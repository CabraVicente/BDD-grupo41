import psycopg2 as psql
import loaders.tools as loader


# Opcion con SQL, no sé si funcione, hay que probarla

conn = loader.connect()
cur = conn.cursor()

data = loader.load_table("./data/suscripciones.csv")

cur.execute(
    """CREATE TABLE Suscripcion(
    cliente_correo VARCHAR(64) REFERENCES Cliente(correo),
    empresa_nombre VARCHAR(30) REFERENCES EmpresaDelivery(nombre),
    medio_de_pago VARCHAR(30),
    fecha_prox_pago DATE,
    estado VARCHAR(30),
    fecha_ultimo_pago DATE,
    monto_ultimo_pago INT,
    ciclo TEXT NOT NULL,
    PRIMARY KEY (cliente_correo, empresa_nombre)
    );"""
)

for fila in data["datos"]:
    email, nombre, estado, ultimopago, fecha_ultimopago, ciclo = fila
    fecha_ultimo = "20" + fecha_ultimopago[6:] + "-" + fecha_ultimopago[3:5] + "-" + fecha_ultimopago[:2]
    
    # calculando fecha de siguiente pago
    cur.execute(
        """
        CREATE TABLE temp(
            fecha DATE
        );

        INSERT INTO temp(fecha) VALUES(%s);
        """,
        (fecha_ultimo,)
    )
    if ciclo == "anual":
        cur.execute("SELECT fecha + INTERVAL '1 year' FROM temp;")
        fecha_proximopago = cur.fetchone()[0]
    else:
        cur.execute("SELECT fecha + INTERVAL '1 month' FROM temp;")
        fecha_proximopago = cur.fetchone()[0]
    
    cur.execute("DROP TABLE temp")
    cur.execute(
        """
        INSERT INTO Suscripcion(cliente_correo, empresa_nombre, medio_de_pago, fecha_prox_pago, estado, fecha_ultimo_pago, monto_ultimo_pago, ciclo)
        SELECT %s, %s, %s, %s, %s, %s, %s, %s
        WHERE %s IN (SELECT correo FROM Cliente) AND %s IN (SELECT nombre FROM EmpresaDelivery)
        ON CONFLICT (cliente_correo, empresa_nombre) DO NOTHING;
        """,
        (email, nombre, "debito", fecha_proximopago ,estado, fecha_ultimo, ultimopago, ciclo,
        email, nombre)
    )

conn.commit()
cur.close()
loader.disconnect(conn)