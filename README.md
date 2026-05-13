# workshop_ETL_3

Integrar el aprendizaje automático con el procesamiento de datos en tiempo real para desarrollar un sistema predictivo integral capaz de calcular índices de felicidad para diferentes países y años.

El proyecto integra:

- Pipelines ETL
- Orquestación con Apache Airflow
- Streaming con Apache Kafka
- Predicción con Machine Learning
- Contenerización con Docker
- Almacenamiento en MySQL
- Visualización de datos y EDA

# Objetivo del Proyecto

Desarrollar un pipeline predictivo completo capaz de:

1. Procesar datasets históricos de felicidad
2. Entrenar un modelo de Machine Learning
3. Transmitir datos de prueba en tiempo real usando Kafka
4. Predecir índices de felicidad
5. Almacenar predicciones en MySQL
6. Visualizar el rendimiento del modelo

# Tecnologías Utilizadas

- Python
- Apache Airflow
- Apache Kafka
- Docker & Docker Compose
- MySQL
- Pandas
- Scikit-learn
- Matplotlib
- SQLAlchemy


# Dataset

Fuente:
World Happiness Report datasets (2015–2019)

Variable objetivo:

```python
happiness_score
```

Features utilizadas:

```python
economy
family
health
freedom
trust
generosity
```

Esquema final normalizado:

```python
[
    "country",
    "year",
    "happiness_score",
    "economy",
    "family",
    "health",
    "freedom",
    "trust",
    "generosity"
]
```


# Estructura del Proyecto

```text
workshop_ETL_3/
├── README.md
├── datos/
│   ├── crudos/
│   ├── archivos_parquet/
│   └── combined/
├── notebooks/
│   └── eda_y_reportes/
├── modelo/
├── kafka_streaming/
├── scripts/
├── dags/
├── database/
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```


# Flujo del Pipeline ETL

El pipeline realiza el siguiente proceso:

1. Ingesta de archivos CSV
2. Conversión a formato Parquet
3. Normalización de datos
4. Concatenación de datasets
5. Entrenamiento del modelo
6. Serialización del modelo (.pkl)
7. Streaming con Kafka
8. Predicción en tiempo real
9. Almacenamiento en MySQL
10. Visualización mediante dashboard


# DAG de Airflow

Flujo del pipeline:

```text
task_ingest_2015 >> task_normalize_2015
task_ingest_2016 >> task_normalize_2016
task_ingest_2017 >> task_normalize_2017
task_ingest_2018 >> task_normalize_2018
task_ingest_2019 >> task_normalize_2019

[task_normalize_*] >> task_concat

task_concat >> task_entrenamiento >> task_producer
```

# Modelo de Machine Learning

Modelo utilizado:

```python
LinearRegression
```

División entrenamiento/prueba:

```python
70% train
30% test
```

Random state:

```python
random_state = 42
```


# Métricas del Modelo

| Métrica | Valor |
|---|---|
| MAE | 0.4245 |
| RMSE | 0.5454 |
| R² | 0.7598 |
| MAPE | 8.22% |


# Servicios Docker

El proyecto utiliza los siguientes contenedores:

- Airflow Webserver
- Airflow Scheduler
- Kafka
- Zookeeper
- MySQL (metadata de Airflow)
- MySQL (base de datos de predicciones)


# Puertos Importantes

| Servicio | Puerto |
|---|---|
| Airflow | 8080 |
| Kafka External Listener | 9093 |
| MySQL Predictions DB | 3308 |


# Instrucciones de Configuración

## 1. Clonar el repositorio

```bash
git clone <repository_url>
cd workshop_ETL_3
```


## 2. Construir e iniciar contenedores

```bash
docker compose up --build -d
```


## 3. Abrir Airflow

Abrir:

```text
http://localhost:8080/
```

Credenciales:

```text
Usuario: admin
Contraseña: admin
```


## 4. Ejecutar el DAG

Dentro de Airflow:

- Activar el DAG
- Ejecutar el pipeline

El DAG realizará:
- ingesta de datasets,
- normalización,
- concatenación,
- entrenamiento del modelo,
- producción de mensajes Kafka.


## 5. Ejecutar el Consumer de Kafka Localmente

Después de iniciar el DAG, ejecutar el consumer localmente desde la raíz del proyecto:

```bash
python kafka_streaming/kafka_consumer.py
```

El consumer:
- recibe mensajes desde Kafka,
- carga el modelo entrenado,
- genera predicciones,
- almacena resultados en MySQL.


# Dependencias Locales

Instalar estas librerías localmente si es necesario:

```bash
pip install ydata-profiling
pip install pymysql
pip install kafka-python
pip install scikit-learn numpy
pip install dill
pip install sqlalchemy
```


# EDA y Visualizaciones

La carpeta notebooks contiene:

- Exploratory Data Analysis (EDA)
- Visualizaciones del dashboard
- Gráficas de evaluación del modelo

Para ejecutarlos:

1. Abrir los notebooks
2. Ejecutar las celdas en orden


# Configuración de Kafka

Listener interno (comunicación Docker):

```text
kafka:9092
```

Listener externo (consumer local):

```text
localhost:9093
```


# Base de Datos de Predicciones

Base de datos:

```text
predictions_db
```

Tabla:

```sql
predictions
```

Campos almacenados:

- economy
- family
- health
- freedom
- trust
- generosity
- happiness_score
- predicted_score


# Decisiones Clave

- Se utilizó formato Parquet para optimizar almacenamiento y procesamiento.
- Solo el dataset de prueba fue transmitido mediante Kafka porque los datos de entrenamiento ya habían sido utilizados previamente durante el entrenamiento del modelo.
- Se seleccionó Linear Regression debido a las relaciones lineales observadas durante el EDA.
- Docker fue utilizado para simplificar la orquestación y la consistencia del entorno.
- Kafka permitió simular flujos de predicción en tiempo real.


# Resultados

- 235 registros transmitidos exitosamente
- 235 predicciones almacenadas en MySQL
- Integración exitosa entre:
  - ETL
  - Airflow
  - Kafka
  - Machine Learning
  - MySQL
  - Docker