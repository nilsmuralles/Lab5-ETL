from sqlalchemy import create_engine
import pandas as pd

def extract_sql():
    engine = create_engine("postgresql://user:password@localhost:5432/lab5_sql")
    query = "SELECT nombre_pais, continente, poblacion, tasa_de_envejecimiento FROM pais"
    df = pd.read_sql(query, engine)
    return df
