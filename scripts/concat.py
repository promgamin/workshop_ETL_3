# Importar librerias necesarias
import pandas as pd
import os

# Funcion que recibe las 5 fuentes 
def run_concat(source1_path, source2_path, source3_path, 
               source4_path, source5_path, output_path):
    """
    Recibe 5 rutas, las concatena y guarda el resultado como parquet.

    Args:
        source1_path: Ruta de la fuente 1
        source2_path: Ruta de la fuente 2
        source3_path: Ruta de la fuente 3
        source4_path: Ruta de la fuente 4
        source5_path: Ruta de la fuente 5
        output_path : Ruta donde se guarda el archivo .parquet resultante.
    """
    df1 = pd.read_parquet(source1_path)
    df2 = pd.read_parquet(source2_path)
    df3 = pd.read_parquet(source3_path)
    df4 = pd.read_parquet(source4_path)
    df5 = pd.read_parquet(source5_path)

    df_final = (pd.concat([df1, df2, df3, df4, df5], ignore_index=True)
                  .sort_values(["year", "country"])
                  .reset_index(drop=True))

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_final.to_parquet(output_path, index=False)

    print(f"Concat completo  : {len(df_final)} registros")