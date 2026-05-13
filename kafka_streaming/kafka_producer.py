# Importar librerias necesarias 
import os
import json
import time
import pandas as pd
from kafka import KafkaProducer
from sklearn.model_selection import train_test_split

# CONSTANTES

FEATURES        = ["economy", "family", "health", "freedom", "trust", "generosity"]
TARGET          = "happiness_score"
TEST_SIZE       = 0.30
RANDOM_STATE    = 42
KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC           = "happiness-predictions"
DELAY_SECONDS   = 0.5      # pausa entre mensajes para simular streaming

# FUNCIONES

def get_test_set(parquet_path: str) -> pd.DataFrame:
    """
    Carga el parquet combinado y devuelve los datos de prueba,
    aplicando el mismo split que se usó en el entrenamiento.

    Args:
        parquet_path : Ruta al parquet combinado (happiness_combined.parquet).

    Returns:
        DataFrame con las filas del set de prueba (features + target).
    """
    df = pd.read_parquet(parquet_path)
    X  = df[FEATURES]
    y  = df[TARGET]

    _, X_test, _, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )

    # Reunir features y target en un solo DataFrame para transmitir
    test_df = X_test.copy()
    test_df[TARGET] = y_test

    return test_df.reset_index(drop=True)


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    """
    Crea y devuelve un KafkaProducer que serializa cada mensaje como JSON.

    Args:
        bootstrap_servers : Dirección del broker Kafka.

    Returns:
        Instancia de KafkaProducer lista para enviar mensajes.
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )


def stream_records(producer: KafkaProducer, topic: str, df: pd.DataFrame):
    """
    Envía cada fila del DataFrame como un mensaje JSON al tópico de Kafka,
    con una pausa entre mensajes para simular el streaming.

    Args:
        producer : Instancia de KafkaProducer.
        topic    : Nombre del tópico Kafka destino.
        df       : DataFrame con los registros a transmitir.
    """
    total = len(df)
    print(f"[INFO] Transmitiendo {total} registros al tópico '{topic}'...")

    for i, (_, row) in enumerate(df.iterrows()):
        mensaje = row.to_dict()
        producer.send(topic, value=mensaje)
        print(f"[INFO] Enviado {i + 1}/{total}: {mensaje}")
        time.sleep(DELAY_SECONDS)

    producer.flush()
    print(f"[OK] {total} registros transmitidos correctamente.")


# FUNCIÓN PRINCIPAL — llamable desde el DAG

def run_producer(parquet_path: str, topic: str = TOPIC):
    """
    Orquesta el flujo completo del producer: obtiene el set de prueba,
    crea el producer y transmite los registros fila a fila a Kafka.

    Args:
        parquet_path : Ruta al parquet combinado (happiness_combined.parquet).
        topic        : Nombre del tópico Kafka destino.
    """
    test_df  = get_test_set(parquet_path)
    producer = build_producer(KAFKA_BOOTSTRAP)
    stream_records(producer, topic, test_df)
    producer.close()