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
    print("NO HAY CONNEXXXX")

cur = conn.cursor()

form = cgi.FieldStorage()
param = [
    form.getvalue("param0"),
    form.getvalue("param1"),
    form.getvalue("param2")
]
consulta = form.getvalue("type")

consulta = int(consulta)
resultado = {"nombres":["vacio"], "datos":[[0]]}


if consulta == 0:
    if (param[2] != None):
        cur.execute(
            """SELECT %s FROM %s WHERE %s""" %
            (param[0], param[1], param[2])
        )
    else:
        cur.execute(
            """SELECT %s FROM %s""" %
            (param[0], param[1])
        )
    resultado["datos"] = cur.fetchall()
elif consulta == 1:
    resultado["nombres"][0] = "Restaurante"

print_table(resultado)

cur.close()
conn.close()