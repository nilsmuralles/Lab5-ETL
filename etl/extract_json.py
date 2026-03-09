import pandas as pd
import json

def extract_json():
    files = [
        "data/raw/no-sql/costos_turisticos_africa.json",
        "data/raw/no-sql/costos_turisticos_america.json",
        "data/raw/no-sql/costos_turisticos_asia.json",
        "data/raw/no-sql/costos_turisticos_europa.json"
    ]

    data = []

    for file in files:
        with open(file, encoding="utf-8") as f:
            data.extend(json.load(f))

    df_costos = pd.json_normalize(data)

    return df_costos


def extract_bigmac():

    file = "data/raw/no-sql/paises_mundo_big_mac.json"

    with open(file, encoding="utf-8") as f:
        data = json.load(f)

    df_bigmac = pd.DataFrame(data)

    return df_bigmac