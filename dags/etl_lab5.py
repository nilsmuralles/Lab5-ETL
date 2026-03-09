from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pymongo
import pandas as pd
from sqlalchemy import create_engine
import os

MONGO_URI = os.getenv("MONGO_URI")
PG_CONN = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:5432/{os.getenv('POSTGRES_DB')}"
)

def extract_sql(**context):
    engine = create_engine(PG_CONN)
    df = pd.read_sql(
        "SELECT nombre_pais, continente, poblacion, tasa_de_envejecimiento FROM pais",
        engine
    )
    print(f"SQL extraído: {len(df)} registros")
    context['ti'].xcom_push(key='df_sql', value=df.to_json())

def extract_mongo(**context):
    client = pymongo.MongoClient(MONGO_URI)
    db = client["lab5"]

    print("Colecciones en Atlas:", db.list_collection_names())

    colecciones = ["costos_africa", "costos_asia", "costos_europa", "costos_america"]
    docs = []
    for col in colecciones:
        resultado = list(db[col].find({}, {"_id": 0}))
        print(f"Docs en {col}: {len(resultado)}")
        docs.extend(resultado)

    df_costos = pd.json_normalize(docs)

    df_bigmac = pd.DataFrame(list(db["precios_bigmac"].find({}, {"_id": 0})))
    print(f"Big Mac extraído: {len(df_bigmac)} registros")

    context['ti'].xcom_push(key='df_costos', value=df_costos.to_json())
    context['ti'].xcom_push(key='df_bigmac', value=df_bigmac.to_json())

def transform(**context):
    ti = context['ti']
    df_sql = pd.read_json(ti.xcom_pull(key='df_sql'))
    df_costos = pd.read_json(ti.xcom_pull(key='df_costos'))
    df_bigmac = pd.read_json(ti.xcom_pull(key='df_bigmac'))

    print("Columnas df_costos:", df_costos.columns.tolist())
    print("Columnas df_sql:", df_sql.columns.tolist())

    # Renombrar columnas de costos
    df_costos.rename(columns={
        "país": "nombre_pais",
        "región": "region",
        "población": "poblacion_json",
        "continente": "continente_json",
        "capital": "capital",
        "costos_diarios_estimados_en_dólares.hospedaje.precio_bajo_usd": "hospedaje_bajo",
        "costos_diarios_estimados_en_dólares.hospedaje.precio_promedio_usd": "hospedaje_promedio",
        "costos_diarios_estimados_en_dólares.hospedaje.precio_alto_usd": "hospedaje_alto",
        "costos_diarios_estimados_en_dólares.comida.precio_bajo_usd": "comida_bajo",
        "costos_diarios_estimados_en_dólares.comida.precio_promedio_usd": "comida_promedio",
        "costos_diarios_estimados_en_dólares.comida.precio_alto_usd": "comida_alto",
        "costos_diarios_estimados_en_dólares.transporte.precio_bajo_usd": "transporte_bajo",
        "costos_diarios_estimados_en_dólares.transporte.precio_promedio_usd": "transporte_promedio",
        "costos_diarios_estimados_en_dólares.transporte.precio_alto_usd": "transporte_alto",
        "costos_diarios_estimados_en_dólares.entretenimiento.precio_bajo_usd": "entretenimiento_bajo",
        "costos_diarios_estimados_en_dólares.entretenimiento.precio_promedio_usd": "entretenimiento_promedio",
        "costos_diarios_estimados_en_dólares.entretenimiento.precio_alto_usd": "entretenimiento_alto",
    }, inplace=True)

    # Renombrar columnas de Big Mac
    df_bigmac.rename(columns={"país": "nombre_pais"}, inplace=True)
    df_bigmac.drop(columns=["continente"], inplace=True, errors="ignore")

    # Merge SQL + costos
    df = df_sql.merge(df_costos, on="nombre_pais", how="left")

    # Completar nulos del SQL con datos del JSON
    df["continente"] = df["continente"].fillna(df["continente_json"])
    df["poblacion"] = df["poblacion"].fillna(df["poblacion_json"])
    df.drop(columns=["continente_json", "poblacion_json"], inplace=True, errors="ignore")

    # Merge con Big Mac
    df = df.merge(df_bigmac, on="nombre_pais", how="left")

    print(f"Registros finales: {len(df)}")
    print(f"Nulos restantes:\n{df.isnull().sum()}")

    context['ti'].xcom_push(key='df_final', value=df.to_json())

def load(**context):
    df = pd.read_json(context['ti'].xcom_pull(key='df_final'))
    engine = create_engine(PG_CONN)
    df.to_sql("hechos_paises", engine, if_exists="replace", index=False, schema="public")
    print(f"✅ {len(df)} registros cargados al data warehouse")

default_args = {
    "owner": "lab5",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_lab5",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="extract_sql", python_callable=extract_sql)
    t2 = PythonOperator(task_id="extract_mongo", python_callable=extract_mongo)
    t3 = PythonOperator(task_id="transform", python_callable=transform)
    t4 = PythonOperator(task_id="load", python_callable=load)

    [t1, t2] >> t3 >> t4
