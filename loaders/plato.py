import psycopg2 as psql
import loaders.tools as loader

conn = loader.connect()
cur = conn.cursor()

cur.execute(
    """CREATE TABLE Plato(
    id INT,
    restaurante_nombre VARCHAR(30) references Restaurante(nombre),
    nombre VARCHAR(30) NOT NULL,
    descripcion TEXT,
    estilo VARCHAR(30) NOT NULL,
    precio INT,
    ingredientes TEXT,
    porte VARCHAR(20),
    disponibilidad BOOL,
    tiempo_prep INT,
    persona_x_porcion INT,
    restriccion TEXT,
    PRIMARY KEY (id, restaurante_nombre)
    );"""
)

tabla = loader.load_table("./data/platos.csv")

for fila in tabla["datos"]:
    id, nombre, descripcion, disponibilidad, estilo, restriccion, ingredientes, porciones, precio, tiempo, restaurant, repartomin, vigente = fila
    cur.execute(
        """INSERT INTO Plato(id, restaurante_nombre, nombre, descripcion, estilo, precio, ingredientes, porte, disponibilidad, tiempo_prep, persona_x_porcion, restriccion)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id, restaurante_nombre) DO NOTHING""",
        (id, restaurant, nombre[:30], descripcion, estilo[:30], precio, ingredientes, "Normal", disponibilidad, tiempo, porciones, restriccion)
    )

conn.commit()
cur.close()
loader.disconnect(conn)