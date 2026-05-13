#Importar librerias necesarias
import json
import joblib
import pymysql
import numpy as np
from kafka import KafkaConsumer

# Constantes

FEATURES        = ["economy", "family", "health", "freedom", "trust", "generosity"]
TARGET          = "happiness_score"
TOPIC           = "happiness-predictions"
KAFKA_BOOTSTRAP = "localhost:9093"
MODEL_PATH      = "modelo/happiness_model.pkl"

DB_CONFIG = {
    "host"    : "localhost",
    "port"    : 3308,               
    "user"    : "predictions_user",
    "password": "predictions_password",
    "database": "predictions_db",
    "charset" : "utf8mb4",
}

DDL = """
    CREATE TABLE IF NOT EXISTS predictions (
        id               INT AUTO_INCREMENT PRIMARY KEY,
        economy          FLOAT,
        family           FLOAT,
        health           FLOAT,
        freedom          FLOAT,
        trust            FLOAT,
        generosity       FLOAT,
        happiness_score  FLOAT,
        predicted_score  FLOAT
    )
"""

# Funciones 

def build_consumer(topic: str, bootstrap_servers: str) -> KafkaConsumer:
    """
    Crea y devuelve un KafkaConsumer suscrito al tópico indicado,
    configurado para deserializar mensajes JSON.

    Args:
        topic             : Nombre del tópico Kafka a consumir.
        bootstrap_servers : Dirección del broker Kafka (ej: 'localhost:9092').

    Returns:
        Instancia de KafkaConsumer lista para recibir mensajes.
    """
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        consumer_timeout_ms=180000      # se detiene si no llegan mensajes en 3 minutos
    )


def setup_database(conn) -> None:
    """
    Crea la tabla 'predictions' en MySQL si aún no existe.

    Args:
        conn : Conexión activa a MySQL (pymysql.connect).
    """
    cur = conn.cursor()
    cur.execute(DDL)
    conn.commit()
    print("Tabla 'predictions' lista.")


def predict_and_store(consumer: KafkaConsumer, model, conn) -> None:
    """
    Consume cada mensaje del tópico, genera una predicción con el modelo
    y guarda las features, el score real y la predicción en MySQL.

    Args:
        consumer : Instancia de KafkaConsumer.
        model    : Modelo de regresión cargado desde el .pkl.
        conn     : Conexión activa a MySQL (pymysql.connect).
    """
    cur   = conn.cursor()
    total = 0

    for message in consumer:
        record = message.value

        # Extraer features en el orden correcto
        features = [[record[f] for f in FEATURES]]
        predicted = float(model.predict(np.array(features))[0])
        actual    = float(record[TARGET])

        cur.execute(
            """INSERT INTO predictions
               (economy, family, health, freedom, trust, generosity,
                happiness_score, predicted_score)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                record["economy"],
                record["family"],
                record["health"],
                record["freedom"],
                record["trust"],
                record["generosity"],
                actual,
                predicted,
            )
        )
        conn.commit()
        total += 1
        print(f"Registro {total} — Real: {actual:.4f} | Predicción: {predicted:.4f}")

    print(f"{total} registros procesados y almacenados.")


# Funcion principal

def run_consumer(
    topic      : str = TOPIC,
    model_path : str = MODEL_PATH,
    bootstrap  : str = KAFKA_BOOTSTRAP,
):
    """
    Orquesta el flujo completo del consumer: carga el modelo .pkl,
    conecta a MySQL, crea la tabla si no existe y procesa cada mensaje
    del tópico generando y almacenando predicciones.

    Args:
        topic      : Nombre del tópico Kafka a consumir.
        model_path : Ruta al modelo serializado (.pkl).
        bootstrap  : Dirección del broker Kafka.
    """
    print(f"Cargando modelo desde: {model_path}")
    model = joblib.load(model_path)

    print(f"[INFO] Conectando a MySQL...")
    conn = pymysql.connect(**DB_CONFIG)
    setup_database(conn)

    print(f"Escuchando tópico '{topic}'...")
    consumer = build_consumer(topic, bootstrap)

    predict_and_store(consumer, model, conn)

    consumer.close()
    conn.close()
    print("Consumer finalizado.")

if __name__ == "__main__":
    run_consumer()