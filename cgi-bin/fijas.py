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
nombre = form.getvalue("nombre")
consulta = form.getvalue("type")

consulta = int(consulta)
resultado = {"nombres":["vacio"], "datos":[[0]]}

match consulta:
    case 1:
        resultado["nombres"][0] = "Restaurante"

print_table(resultado)

cur.close()
conn.close()