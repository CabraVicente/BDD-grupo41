#!/usr/bin/python3

import psycopg2 as psql
from web_table import print_table
import cgi
import cgitb

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
            """SELECT plato.nombre AS plato, restaurante.nombre AS restaurante
            FROM plato, restaurante
            WHERE plato.restaurante_nombre = restaurante.nombre AND
                plato.estilo=%s
            ORDER BY plato.nombre""",
            (param[0],)
        )
    resultado = result_get(cur)

print_table(resultado)

cur.close()
conn.close()