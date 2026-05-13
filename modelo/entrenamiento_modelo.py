#Importar librerias necesarias
import os
import joblib
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score

# Constantes
FEATURES     = ["economy", "family", "health", "freedom", "trust", "generosity"]
TARGET       = "happiness_score"
TEST_SIZE    = 0.30
RANDOM_STATE = 42


# Funciones

def train(X_train: pd.DataFrame, y_train: pd.Series) -> LinearRegression:
    """
    Entrena un modelo de Regresión Lineal Múltiple e imprime sus coeficientes.

    Args:
        X_train : DataFrame con las features del set de entrenamiento.
        y_train : Serie con el happiness_score del set de entrenamiento.

    Returns:
        Modelo entrenado de LinearRegression.
    """
    print("[INFO] Entrenando LinearRegression...")
    model = LinearRegression()
    model.fit(X_train, y_train)

    coef_df = pd.DataFrame({
        "feature"    : FEATURES,
        "coeficiente": model.coef_
    }).sort_values("coeficiente", ascending=False)
    print("\n[INFO] Coeficientes del modelo:")
    print(coef_df.to_string(index=False))
    print(f"       Intercepto: {model.intercept_:.4f}\n")

    return model


def evaluate(model: LinearRegression, X_test: pd.DataFrame, y_test: pd.Series):
    """
    Evalúa el modelo sobre los datos de prueba e imprime MAE y R^2.

    Args:
        model  : Modelo entrenado de LinearRegression.
        X_test : DataFrame con las features del set de prueba.
        y_test : Serie con los valores reales de happiness_score.

    Returns:
        Tupla (y_pred, mae, r2) con las predicciones y las métricas.
    """
    y_pred = model.predict(X_test)
    mae    = mean_absolute_error(y_test, y_pred)
    r2     = r2_score(y_test, y_pred)

    print("─" * 40)
    print("  MÉTRICAS DE EVALUACIÓN (set de prueba)")
    print("─" * 40)
    print(f"  MAE : {mae:.4f}")
    print(f"  R²  : {r2:.4f}")
    print("─" * 40)

    return y_pred, mae, r2


def run_training(parquet_path: str, model_path: str):
    """
    Orquesta el pipeline completo: carga el parquet combinado, separa features
    y target, hace el split 70/30, entrena el modelo, lo evalúa y lo guarda
    como .pkl.

    Args:
        parquet_path : Ruta al parquet combinado (happiness_combined.parquet).
        model_path   : Ruta donde se guarda el modelo serializado (.pkl).
    """
    # Carga
    print(f"Leyendo dataset desde: {parquet_path}")
    df = pd.read_parquet(parquet_path)
    print(f"Registros: {len(df)}")

    # 2. Features y target
    X = df[FEATURES]
    y = df[TARGET]

    # 3. Separar los datos 70/30
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )
    print(f"Train: {len(X_train)} | Test: {len(X_test)}")

    # 4. Entrenamiento y evaluación
    model = train(X_train, y_train)
    evaluate(model, X_test, y_test)

    # 5. Serialización
    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    joblib.dump(model, model_path)
    print(f"Modelo guardado en: {model_path}")
    print("Pipeline de entrenamiento finalizado.")