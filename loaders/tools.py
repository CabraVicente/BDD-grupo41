import psycopg2 as psql
import csv

def connect():
    conn = psql.connect(
        database="grupo41e2",
        user="grupo41",
        host="pavlov.ing.puc.cl",
        password="2_balas",
        port=5432
    )

    return conn

def load_table(filename):
    data = []
    with open(filename, "r", newline="") as f:
        datos = csv.reader(f, delimiter=";")
        for i in datos:
            data.append(i)
    
    return {"nombres": data[0], "datos": data[1:]}
        

def disconnect(connection):
    connection.close()

def precio_delivery(nombre_empresa):
    if nombre_empresa == "ATuCasa":
        return 1800
    elif nombre_empresa == "FoodNow":
        return 2400
    elif nombre_empresa == "PedidosNow":
        return 900
    elif nombre_empresa == "UbreFoods":
        return 300
    elif nombre_empresa == "ComidaAhora" or nombre_empresa == "DCComida":
        return 3600
    elif nombre_empresa == "FastDelivery":
        return 1500
    elif nombre_empresa == "Baloon":
        return 4600
    elif nombre_empresa == "SlowDelivery" or nombre_empresa == "NotRappido":
        return 3300
    elif nombre_empresa == "EsquinaShop":
        return 600
    elif nombre_empresa == "Speed" or nombre_empresa == "FoodBasics":
        return 5400
    elif nombre_empresa == "DCCRappi":
        return 1300
    elif nombre_empresa == "DidiEats":
        return 2700