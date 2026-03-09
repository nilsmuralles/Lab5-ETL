import psycopg2
import pandas as pd

def extract_sql():

    conn = psycopg2.connect(
        host="localhost",
        database="lab5_sql",
        user="user",
        password="password",
        port=5432
    )

    query = """
    SELECT nombre_pais, continente, poblacion, tasa_de_envejecimiento
    FROM pais
    """

    df = pd.read_sql(query, conn)
    conn.close()

    return df