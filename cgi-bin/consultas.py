#!/usr/bin/python3

import psycopg2 as psql
from web_table import print_table
import cgi
import cgitb
import re

cgitb.enable()

try:
    conn = psql.connect(
        database="grupo41e2",
        user="grupo41",
        host="pavlov.ing.puc.cl",
        password="2_balas",
        port=5432
    )
except:
    print("NO HAY CONEXION")

cur = conn.cursor()

form = cgi.FieldStorage()
param = [
    form.getvalue("param0"),
    form.getvalue("param1"),
    form.getvalue("param2")
]

param = [
    param[0] if param[0] is not None else "",
    param[1] if param[1] is not None else "",
    param[2] if param[2] is not None else "",
]
consulta = form.getvalue("type")

consulta = int(consulta)
resultado = {"nombres":["vacio"], "datos":[[0]]}

def result_get(cursor):
    return {"nombres":[d[0] for d in cur.description], "datos":cur.fetchall()}

if consulta == 0:
    if (param[2] != ""):
        cur.execute(
            """SELECT %s FROM %s WHERE %s""" %
            (param[0], param[1], param[2])
        )
    else:
        cur.execute(
            """SELECT %s FROM %s""" %
            (param[0], param[1])
        )

    resultado = result_get(cur)
elif consulta == 1:
    cur.execute(
        """SELECT restaurante.nombre AS Restaurante FROM restaurante,plato
        WHERE plato.restaurante_nombre = restaurante.nombre AND plato.nombre=%s AND plato.disponibilidad=True
        ORDER BY restaurante.nombre""",
        (param[0],)
    )

    resultado = result_get(cur)
elif consulta == 2:
    cur.execute(
        """
        CREATE TABLE temp (
            cliente_correo VARCHAR(60),
            mes TEXT,
            pedido INT,
            gasto INT
        );
        INSERT INTO temp
            SELECT
                Pedido.cliente_correo,
                to_char(Pedido.fecha_hora, 'YYYY-MM') AS mes,
                Pedido.id,
                SUM(Plato.precio) AS Gasto_por_platos
            FROM Pedido, Pedido_Plato, Plato, Despacho, EmpresaDelivery
            WHERE 
                Pedido.id = Pedido_Plato.pedido_id AND
                Pedido_Plato.plato_id = Plato.id AND
                Pedido.id = Despacho.pedido_id AND
                Despacho.empresa_nombre = EmpresaDelivery.nombre
            GROUP BY Pedido.cliente_correo, to_char(Pedido.fecha_hora, 'YYYY-MM'), Pedido.id
            ORDER BY Pedido.cliente_correo;

        SELECT
            temp.mes,
            string_agg(CAST(temp.pedido AS TEXT), ', ') AS id_de_pedidos,
            SUM(temp.gasto) +
            SUM(
                CASE
                    WHEN EmpresaDelivery.nombre IN (SELECT empresa_nombre FROM Suscripcion WHERE cliente_correo = temp.cliente_correo) THEN 0
                    ELSE EmpresaDelivery.precio_unitario
                END
            ) AS gastos
        FROM temp, Despacho, EmpresaDelivery
        WHERE
            temp.pedido = Despacho.pedido_id AND
            Despacho.empresa_nombre = EmpresaDelivery.nombre AND
            temp.cliente_correo = %s
        GROUP BY temp.cliente_correo, temp.mes;
        """,
        (param[0],)
    )

    resultado = result_get(cur)

    cur.execute("DROP TABLE temp")
elif consulta == 3:
    cur.execute(
        """SELECT Pedido.id AS pedido_id, SUM(Plato.precio) as precio_total, Pedido.estado AS pedido_estado
        FROM Pedido, Pedido_Plato, Plato
        WHERE
            Pedido.id = Pedido_Plato.pedido_id AND
            Pedido_Plato.plato_id = Plato.id AND
            Pedido_Plato.restaurante_nombre = Plato.restaurante_nombre
        GROUP BY Pedido.id;
        """
    )

    resultado = result_get(cur)
elif consulta == 4 or consulta == 5:
    if (param[1] != "on"):
        cur.execute(
            """SELECT nombre, restriccion
            FROM plato
            WHERE plato.estilo=%s
            ORDER BY nombre""",
            (param[0],)
        )
    else:
        cur.execute(
            """SELECT plato.nombre AS plato, restaurante.nombre AS restaurante, string_agg(sucursal.area_de_despacho, ', ') AS comunas_con_delivery
            FROM plato, restaurante, sucursal
            WHERE plato.restaurante_nombre = restaurante.nombre AND restaurante.nombre = sucursal.restaurante_nombre AND
                plato.estilo=%s
            GROUP BY plato.nombre, restaurante.nombre
            ORDER BY plato.nombre""",
            (param[0],)
        )

    resultado = result_get(cur)
elif consulta == 6:
    cur.execute(
        """CREATE VIEW temp AS
        SELECT EmpresaDelivery.nombre AS suscripcion_a_empresa, string_agg(DISTINCT Plato.restaurante_nombre, ', ') AS restaurantes
        FROM Pedido, Despacho, Pedido_Plato, Plato, Suscripcion, EmpresaDelivery
        WHERE 
            Pedido.id = Despacho.pedido_id AND Pedido.id = Pedido_Plato.pedido_id AND
            Pedido_Plato.plato_id = Plato.id AND
            Despacho.empresa_nombre = EmpresaDelivery.nombre AND
            Suscripcion.empresa_nombre = EmpresaDelivery.nombre
        GROUP BY EmpresaDelivery.nombre;

        SELECT temp.suscripcion_a_empresa, temp.restaurantes
        FROM temp, Suscripcion
        WHERE
            Suscripcion.cliente_correo = %s AND
            Suscripcion.empresa_nombre = temp.suscripcion_a_empresa AND
            Suscripcion.estado = 'Vigente';
        """,
        (param[0],)
    )

    resultado = result_get(cur)

    cur.execute("DROP VIEW temp;")
elif consulta == 7:
    cur.execute(
        """CREATE VIEW temp AS
        SELECT Pedido.cliente_correo, Pedido.id AS pedido_id, Despacho.empresa_nombre, SUM(Plato.precio) AS gasto_en_platos
        FROM Pedido, Pedido_Plato, Plato, Despacho
        WHERE
            Pedido.id = Despacho.pedido_id AND
            Pedido.id = Pedido_Plato.pedido_id AND
            Pedido_Plato.plato_id = Plato.id AND
            Pedido.estado = 'entregado a cliente' AND
            Despacho.empresa_nombre NOT IN (
                SELECT Suscripcion.empresa_nombre
                FROM Suscripcion
                WHERE Pedido.cliente_correo = Suscripcion.cliente_correo
            )
        GROUP BY Pedido.cliente_correo, Pedido.id, Despacho.empresa_nombre;

        SELECT temp.cliente_correo, temp.gasto_en_platos, SUM(EmpresaDelivery.precio_unitario) AS gasto_delivery
        FROM temp, EmpresaDelivery
        WHERE
            temp.empresa_nombre = EmpresaDelivery.nombre
        GROUP BY temp.cliente_correo, temp.gasto_en_platos;
        """
    )

    resultado = result_get(cur)
    cur.execute("DROP VIEW temp;")
elif consulta == 8:
    cur.execute(
        """SELECT plato.nombre AS plato, string_agg(restaurante.nombre, ', ') AS restaurantes
        FROM plato,restaurante
        WHERE plato.restaurante_nombre = restaurante.nombre
        GROUP BY plato.nombre
        ORDER BY plato.nombre"""
    )

    resultado = result_get(cur)
elif consulta == 9:
    cur.execute(
        """SELECT pedido_id, calif_cliente, calif_pedido
        FROM calificacion
        WHERE calif_cliente >= %s AND calif_pedido >= %s
        ORDER BY calif_cliente DESC, calif_pedido DESC""",
        (int(param[0]),int(param[1]))
    )

    resultado = result_get(cur)
elif consulta == 10:
    cur.execute(
        """SELECT nombre AS plato, ingredientes
        FROM plato
        WHERE ingredientes LIKE '%%' || %s || '%%'
        GROUP BY plato, ingredientes
        ORDER BY plato.nombre""",
        (param[0],)
    )

    resultado = result_get(cur)

print_table(resultado)

cur.close()
conn.close()