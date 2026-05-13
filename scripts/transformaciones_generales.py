# Importar librerias necesarias
import pandas as pd
import os

# Diccionario para normalizar las columnas
COLUMN_MAP = {
    2015: {
        "Country":                       "country",
        "Happiness Score":               "happiness_score",
        "Economy (GDP per Capita)":      "economy",
        "Family":                        "family",
        "Health (Life Expectancy)":      "health",
        "Freedom":                       "freedom",
        "Trust (Government Corruption)": "trust",
        "Generosity":                    "generosity",
    },
    2016: {
        "Country":                       "country",
        "Happiness Score":               "happiness_score",
        "Economy (GDP per Capita)":      "economy",
        "Family":                        "family",
        "Health (Life Expectancy)":      "health",
        "Freedom":                       "freedom",
        "Trust (Government Corruption)": "trust",
        "Generosity":                    "generosity",
    },
    2017: {
        "Country":                       "country",
        "Happiness.Score":               "happiness_score",
        "Economy..GDP.per.Capita.":      "economy",
        "Family":                        "family",
        "Health..Life.Expectancy.":      "health",
        "Freedom":                       "freedom",
        "Trust..Government.Corruption.": "trust",
        "Generosity":                    "generosity",
    },
    2018: {
        "Country or region":             "country",
        "Score":                         "happiness_score",
        "GDP per capita":                "economy",
        "Social support":                "family",
        "Healthy life expectancy":       "health",
        "Freedom to make life choices":  "freedom",
        "Perceptions of corruption":     "trust",
        "Generosity":                    "generosity",
    },
    2019: {
        "Country or region":             "country",
        "Score":                         "happiness_score",
        "GDP per capita":                "economy",
        "Social support":                "family",
        "Healthy life expectancy":       "health",
        "Freedom to make life choices":  "freedom",
        "Perceptions of corruption":     "trust",
        "Generosity":                    "generosity",
    },
}

FINAL_COLUMNS = ["country", "year", "happiness_score",
                 "economy", "family", "health",
                 "freedom", "trust", "generosity"]


def normalize_source(input_path: str, output_path: str) -> None:
    """Lee un parquet según el año correspondiente, normaliza nombres
    de columnas e imputa valores nulos antes de
    guardar el resultado procesado.

    Args:
        input_path  : Ruta del archivo parquet de entrada.
        output_path : Ruta donde se guardará el parquet normalizado.

    Raises:
        ValueError:
        - Si el año no tiene un mapeo definido en COLUMN_MAP.
        - Si faltan columnas requeridas después de la normalización."""
    df   = pd.read_parquet(input_path)
    year = int(df["year"].iloc[0])

    if year not in COLUMN_MAP:
        raise ValueError(f"Año {year} no tiene mapeo definido.")

    df = df.rename(columns=COLUMN_MAP[year])

    columnas_faltantes = [c for c in FINAL_COLUMNS if c not in df.columns]
    if columnas_faltantes:
        raise ValueError(f"Año {year}: columnas faltantes → {columnas_faltantes}")

    df = df[FINAL_COLUMNS]

    # Imputación de nulos (solo 1)
    nulos = df.isnull().sum().sum()
    if nulos > 0:
        print(f"Año {year}: {nulos} nulo(s) encontrado(s) — imputando con mediana por columna")
        numeric_cols = df.select_dtypes(include="number").columns
        df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"Año {year}: {len(df)} registros normalizados guardados en '{output_path}'")