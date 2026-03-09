from extract_sql import extract_sql
from extract_json import extract_json
from transform import transform_data
from load_dw import load_dw

df_sql = extract_sql()
df_json = extract_json()

df_final = transform_data(df_sql, df_json)

load_dw(df_final)

print("ETL ejecutado correctamente")