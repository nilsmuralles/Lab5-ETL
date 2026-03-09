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
            tasa_envejecimiento,
            precio_big_mac,
            costo_hospedaje,
            costo_comida,
            costo_transporte,
            costo_entretenimiento
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,(
            row["pais"],
            row["continente"],
            row["poblacion"],
            row["tasa_envejecimiento"],
            row["precio_big_mac"],
            row["costo_hospedaje"],
            row["costo_comida"],
            row["costo_transporte"],
            row["costo_entretenimiento"]
        ))

    conn.commit()
    conn.close()
