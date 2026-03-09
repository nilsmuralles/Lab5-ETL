def transform_data_basic(df_sql, df_costos):

    df = df_sql.merge(
        df_costos,
        left_on="nombre_pais",
        right_on="país",
        how="left"
    )

    return df


def transform_data_with_bigmac(df_sql, df_costos, df_bigmac):
    df = df_sql.merge(
        df_costos,
        left_on="nombre_pais",
        right_on="país",
        how="left"
    )

    df = df.merge(
        df_bigmac,
        left_on="nombre_pais",
        right_on="país",
        how="left"
    )

    df["poblacion"] = df["poblacion"].fillna(df["población"])
    df["continente"] = df["continente_x"].fillna(df["continente_y"])

    df = df.rename(columns={
        "nombre_pais": "pais",
        "tasa_de_envejecimiento": "tasa_envejecimiento",
        "precio_big_mac_usd": "precio_big_mac",
        "costos_diarios_estimados_en_dólares.hospedaje.precio_promedio_usd": "costo_hospedaje",
        "costos_diarios_estimados_en_dólares.comida.precio_promedio_usd": "costo_comida",
        "costos_diarios_estimados_en_dólares.transporte.precio_promedio_usd": "costo_transporte",
        "costos_diarios_estimados_en_dólares.entretenimiento.precio_promedio_usd": "costo_entretenimiento"
    })

    df = df.drop(columns=[
        "país_x",
        "país_y",
        "continente_x",
        "continente_y",
        "población"
    ], errors="ignore")

    return df