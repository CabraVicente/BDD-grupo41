import psycopg2 as psql
import loaders.tools as loader

conexion_insana = loader.connect()

cur = conexion_insana.cursor()

table = loader.load_table("./data/pedidos.csv", encoding = "latin-1")

cur.execute(
    """CREATE TABLE Despacho(
    sucursal TEXT,
    restaurante_nombre VARCHAR(30),
    pedido_id INT,
    despachador_telefono VARCHAR(9) REFERENCES Despachador(telefono),
    empresa_nombre VARCHAR(30) REFERENCES EmpresaDelivery(nombre),
    FOREIGN KEY (pedido_id) REFERENCES Pedido(id) ON DELETE CASCADE,
    FOREIGN KEY (sucursal, restaurante_nombre) REFERENCES Sucursal(sucursal, restaurante_nombre),
    PRIMARY KEY (pedido_id, sucursal, restaurante_nombre)
    );"""
)

bad_data = 0
def correct_pedido(id):
    cur.execute(
        """
        DELETE FROM Pedido
        WHERE id = %s;
        """,
        (id,)
    )
    print("Pedido", id, "invalido, fue removido de la tabla.")

for linea in table["datos"]:
    id, cliente, sucursal, delivery, despachador, platos, fecha, hora, estado = linea
    
    # Asumiendo que todos los pedidos tienen platos de un mismo restoran
    cur.execute(
        """
        SELECT Plato.restaurante_nombre
        FROM Pedido_Plato, Plato
        WHERE %s = Pedido_Plato.pedido_id AND Pedido_Plato.plato_id = Plato.id;
        """,
        (id,)
    )
    res = cur.fetchone()
    if (res is None):
        bad_data += 1
        correct_pedido(id)
        continue
    restaurante = res[0]

    # Asumiendo que las empresas de delivery no contratan a mas de una persona con el mismo nombre xdddd
    cur.execute(
        """
        SELECT Despachador.telefono
        FROM Despachador,EmpresaDelivery_Despachador
        WHERE Despachador.telefono = EmpresaDelivery_Despachador.despachador_telefono AND %s = Despachador.nombre AND %s = EmpresaDelivery_Despachador.empresa_nombre;
        """,
        (despachador[:60], delivery[:30])
    )
    res = cur.fetchone()
    if (res is None):
        bad_data += 1
        correct_pedido(id)
        continue
    despachador_telefono = res[0]

    cur.execute(
        """
        INSERT INTO Despacho(pedido_id, sucursal, restaurante_nombre, despachador_telefono, empresa_nombre)
        SELECT %s, %s, %s, %s, %s
        WHERE
            %s IN (SELECT id FROM Pedido) AND
            %s IN (SELECT sucursal FROM Sucursal WHERE restaurante_nombre = %s) AND
            %s IN (SELECT restaurante_nombre FROM Sucursal WHERE sucursal = %s)
        ON CONFLICT (pedido_id, sucursal, restaurante_nombre) DO NOTHING;
        """,
        (id, sucursal, restaurante, despachador_telefono, delivery[:30],
         id,
         sucursal, restaurante,
         restaurante, sucursal)
    )

    cur.execute(
        """
        SELECT pedido_id
        FROM Despacho
        WHERE pedido_id = %s;
        """,
        (id,)
    )

    if (cur.fetchone() is None):
        bad_data += 1
        correct_pedido(id)

    

print(bad_data, "Pedidos removidos.")
conexion_insana.commit()
cur.close()
loader.disconnect(conexion_insana)