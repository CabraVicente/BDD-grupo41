import psycopg2 as psql

try:
    conn = psql.connect(
        database="grupo41e2",
        user="grupo41",
        host="pavlov.ing.puc.cl",
        password="2_balas",
        port=5432
    )
except:
    print("ERROR DE CONEXION")

cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS CLIENTES;")
cur.execute("DROP TABLE IF EXISTS Cliente;")
cur.execute("DROP TABLE IF EXISTS comunas;")

conn.commit()
cur.close()
conn.close()