import psycopg2

def load_dw(df):

    conn = psycopg2.connect(
        host="localhost",
        database="lab5_sql",
        user="user",
        password="password",
        port=5432
    )

    cursor = conn.cursor()

    for _, row in df.iterrows():

        cursor.execute("""
        INSERT INTO pais_dw(
            pais,
            continente,
            poblacion,
            tasa_envejecimiento
        )
        VALUES (%s,%s,%s,%s)
        """, (
            row["nombre_pais"],
            row["continente"],
            row["poblacion"],
            row["tasa_de_envejecimiento"]
        ))

    conn.commit()
    conn.close()