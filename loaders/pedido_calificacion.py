import psycopg2 as psql
import loaders.tools as loader

conn = loader.connect()
cur = conn.cursor()

cur.execute(
    """CREATE TABLE Pedido(
    id INT PRIMARY KEY,
    cliente_correo VARCHAR(60) REFERENCES Cliente(correo),
    estado TEXT,
    fecha_hora TIMESTAMP
    );"""
)

cur.execute(
    """CREATE TABLE Calificacion(
    pedido_id INT,
    calif_pedido FLOAT,
    calif_cliente FLOAT,
    FOREIGN KEY (pedido_id) REFERENCES Pedido(id),
    PRIMARY KEY (pedido_id)
    );"""
)

cur.execute(
    """CREATE TABLE Pedido_Plato(
    pedido_id INT REFERENCES Pedido(id),
    plato_id INT,
    restaurante_nombre VARCHAR(30),
    cantidad INT,
    ingredientes_removidos TEXT,
    FOREIGN KEY (plato_id, restaurante_nombre) REFERENCES Plato(id, restaurante_nombre),
    PRIMARY KEY (pedido_id, plato_id, restaurante_nombre)
    );"""
)

data = loader.load_table("./data/pedidos.csv", encoding = "latin-1")
for x in data["datos"]:
    id, cliente, sucursal, delivery, despachador, platos, fecha, hora, estado = x

    fecha = "20" + fecha[6:] + "-" + fecha[3:5] + "-" + fecha[:2]
    cur.execute(
        """INSERT INTO Pedido(id, cliente_correo, estado, fecha_hora)
        SELECT %s, %s, %s, %s
        WHERE %s IN (SELECT correo FROM Cliente)
        ON CONFLICT (id) DO NOTHING
        """,
        (int(id), cliente[:60], estado, fecha + " " + hora, cliente[:60])
    )

    platos_del_pedido = []
    cantidades = {}
    for plato in platos.split("  "):
        if (plato not in platos_del_pedido):
            platos_del_pedido.append(plato)
            cantidades[plato] = 1
        else:
            cantidades[plato] += 1

    for plato in platos_del_pedido:
        cur.execute(
            """
            CREATE TABLE temp(
                pedido_id INT,
                plato_id INT,
                cantidad INT,
                ingredientes_removidos TEXT
            );

            INSERT INTO temp(pedido_id, plato_id, cantidad, ingredientes_removidos)
            SELECT %s, %s, %s, %s
            WHERE %s IN (SELECT id FROM Pedido)
            ;

            INSERT INTO Pedido_Plato(pedido_id, plato_id, restaurante_nombre, cantidad, ingredientes_removidos)
                SELECT temp.pedido_id, temp.plato_id, plato.restaurante_nombre, temp.cantidad, temp.ingredientes_removidos
                FROM temp,plato
                WHERE temp.plato_id = plato.id
            ON CONFLICT (pedido_id, plato_id, restaurante_nombre) DO NOTHING;

            DROP TABLE temp;
            """,
            (int(id), int(plato), int(cantidades[plato]), "", int(id))
        )

data = loader.load_table("./data/calificacion.csv")
for y in data["datos"]:
    id_pedido, calif_pedido, calif_cliente = y
    cur.execute(
        "INSERT INTO Calificacion(pedido_id, calif_pedido, calif_cliente) SELECT %s, %s, %s WHERE %s IN (SELECT id FROM Pedido) ON CONFLICT(pedido_id) DO NOTHING",
        (int(id_pedido),float(calif_pedido),float(calif_cliente), int(id_pedido))
    )

conn.commit()
cur.close()
loader.disconnect(conn)