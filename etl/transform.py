def transform_data(df_sql, df_json):
    df = df_sql.merge(
        df_json,
        left_on="nombre_pais",
        right_on="país",
        how="left"
    )
    
    df.rename(columns={"continente_x": "continente"}, inplace=True)
    df.drop(columns=["continente_y"], inplace=True, errors="ignore")
    
    return df
