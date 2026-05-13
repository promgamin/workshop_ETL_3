from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.ingesta_general import ingest_source
from scripts.transformaciones_generales import normalize_source
from scripts.concat import run_concat
from modelo.entrenamiento_modelo import run_training
from kafka_streaming.kafka_producer import run_producer

default_args = {
    "owner":           "JuanParedes",
    "retries":         2,
    "retry_delay":     timedelta(minutes=2),
    "depends_on_past": False,
}

with DAG(
    dag_id="workshop_etl_3",
    default_args=default_args,
    description="ETL pipeline workshop",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "workshop", "happiness"],
) as dag:

# Ingesta de las 5 fuentes 

    task_ingest_2015 = PythonOperator(
        task_id="ingest_2015",
        python_callable=ingest_source,
        op_kwargs={
            "csv_path":    "/opt/airflow/datos/crudos/2015.csv",
            "year":        2015,
            "output_path": "/opt/airflow/datos/archivos_parquet/raw_2015.parquet",
        },
    )

    task_ingest_2016 = PythonOperator(
        task_id="ingest_2016",
        python_callable=ingest_source,
        op_kwargs={
            "csv_path":    "/opt/airflow/datos/crudos/2016.csv",
            "year":        2016,
            "output_path": "/opt/airflow/datos/archivos_parquet/raw_2016.parquet",
        },
    )

    task_ingest_2017 = PythonOperator(
        task_id="ingest_2017",
        python_callable=ingest_source,
        op_kwargs={
            "csv_path":    "/opt/airflow/datos/crudos/2017.csv",
            "year":        2017,
            "output_path": "/opt/airflow/datos/archivos_parquet/raw_2017.parquet",
        },
    )

    task_ingest_2018 = PythonOperator(
        task_id="ingest_2018",
        python_callable=ingest_source,
        op_kwargs={
            "csv_path":    "/opt/airflow/datos/crudos/2018.csv",
            "year":        2018,
            "output_path": "/opt/airflow/datos/archivos_parquet/raw_2018.parquet",
        },
    )

    task_ingest_2019 = PythonOperator(
        task_id="ingest_2019",
        python_callable=ingest_source,
        op_kwargs={
            "csv_path":    "/opt/airflow/datos/crudos/2019.csv",
            "year":        2019,
            "output_path": "/opt/airflow/datos/archivos_parquet/raw_2019.parquet",
        },
    )

# Normalización y eliminación de nulos (solo 1)

    task_normalize_2015 = PythonOperator(
        task_id="normalize_2015",
        python_callable=normalize_source,
        op_kwargs={
            "input_path":  "/opt/airflow/datos/archivos_parquet/raw_2015.parquet",
            "output_path": "/opt/airflow/datos/archivos_parquet/normalized_2015.parquet",
        },
    )

    task_normalize_2016 = PythonOperator(
        task_id="normalize_2016",
        python_callable=normalize_source,
        op_kwargs={
            "input_path":  "/opt/airflow/datos/archivos_parquet/raw_2016.parquet",
            "output_path": "/opt/airflow/datos/archivos_parquet/normalized_2016.parquet",
        },
    )

    task_normalize_2017 = PythonOperator(
        task_id="normalize_2017",
        python_callable=normalize_source,
        op_kwargs={
            "input_path":  "/opt/airflow/datos/archivos_parquet/raw_2017.parquet",
            "output_path": "/opt/airflow/datos/archivos_parquet/normalized_2017.parquet",
        },
    )

    task_normalize_2018 = PythonOperator(
        task_id="normalize_2018",
        python_callable=normalize_source,
        op_kwargs={
            "input_path":  "/opt/airflow/datos/archivos_parquet/raw_2018.parquet",
            "output_path": "/opt/airflow/datos/archivos_parquet/normalized_2018.parquet",
        },
    )

    task_normalize_2019 = PythonOperator(
        task_id="normalize_2019",
        python_callable=normalize_source,
        op_kwargs={
            "input_path":  "/opt/airflow/datos/archivos_parquet/raw_2019.parquet",
            "output_path": "/opt/airflow/datos/archivos_parquet/normalized_2019.parquet",
        },
    )

# Concatenación 

    task_concat = PythonOperator(
        task_id="concat_all",
        python_callable=run_concat,
        op_kwargs={
            "source1_path": "/opt/airflow/datos/archivos_parquet/normalized_2015.parquet",
            "source2_path": "/opt/airflow/datos/archivos_parquet/normalized_2016.parquet",
            "source3_path": "/opt/airflow/datos/archivos_parquet/normalized_2017.parquet",
            "source4_path": "/opt/airflow/datos/archivos_parquet/normalized_2018.parquet",
            "source5_path": "/opt/airflow/datos/archivos_parquet/normalized_2019.parquet",
            "output_path":  "/opt/airflow/datos/combined/happiness_combined.parquet",
        },
    )

# Entrenamiento del modelo

    task_entrenamineto = PythonOperator(
        task_id="entrenamiento_modelo",
        python_callable=run_training,
        op_kwargs={
            "parquet_path": "/opt/airflow/datos/combined/happiness_combined.parquet",
            "model_path": "/opt/airflow/modelo/happiness_model.pkl",
        }
    )

    task_producer=PythonOperator(
        task_id="producer_kafka",
        python_callable=run_producer,
        op_kwargs={
            "parquet_path": "/opt/airflow/datos/combined/happiness_combined.parquet", 
            "topic": "happiness-predictions",
        }
    )


# Flujo de tareas 

    task_ingest_2015 >> task_normalize_2015
    task_ingest_2016 >> task_normalize_2016
    task_ingest_2017 >> task_normalize_2017
    task_ingest_2018 >> task_normalize_2018
    task_ingest_2019 >> task_normalize_2019

    [task_normalize_2015, task_normalize_2016, task_normalize_2017, 
     task_normalize_2018, task_normalize_2019,] >> task_concat
    
    task_concat >> task_entrenamineto
    task_entrenamineto >> task_producer