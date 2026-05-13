import pandas as pd
import os

def ingest_source(csv_path: str, year: int, output_path: str) -> None:
    """
    Lee un CSV, agrega el año y lo guarda como parquet.

    Args:
        csv_path    : Ruta al archivo CSV de entrada.
        year        : Año correspondiente al dataset.
        output_path : Ruta donde se guarda el archivo .parquet resultante.
    """
    df = pd.read_csv(csv_path, encoding="latin-1", low_memory=False)
    df["year"] = year

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"Fuente {year}: {len(df)} registros guardados en '{output_path}'")