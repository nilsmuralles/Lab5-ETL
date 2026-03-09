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

    df = pd.json_normalize(data)

    return df