import psycopg2 as psql
import cgi

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
print(nombre)

cur.close()
conn.close()