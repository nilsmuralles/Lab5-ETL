from extract_sql import extract_sql
from extract_json import extract_json
from extract_json import extract_bigmac
from transform import transform_data_with_bigmac
from load_dw import load_dw

df_sql = extract_sql()
df_costos = extract_json()
df_bigmac = extract_bigmac()


df_final = transform_data_with_bigmac(df_sql, df_costos, df_bigmac)
print("Número de registros:", len(df_final))

print("Primeras filas del dataset integrado:")
print(df_final.head(10))

print("Últimas filas del dataset integrado:")
print(df_final.tail(10))

print("\nColumnas del dataset:")
print(df_final.columns)

load_dw(df_final)


print("\nETL ejecutado correctamente")