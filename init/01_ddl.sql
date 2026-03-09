CREATE TABLE pais (
    id_pais SERIAL PRIMARY KEY,
    nombre_pais VARCHAR(100) NOT NULL,
    capital VARCHAR(100),
    continente VARCHAR(100),
    region VARCHAR(100),
    poblacion NUMERIC(15,2),
    tasa_de_envejecimiento DECIMAL(5,2)
);

CREATE TABLE pais_poblacion (
    id VARCHAR(30) PRIMARY KEY,
    continente VARCHAR(100),
    pais VARCHAR(100),
    poblacion NUMERIC(15,2),
    costo_bajo_hospedaje DECIMAL(10,2),
    costo_promedio_comida DECIMAL(10,2),
    costo_bajo_transporte DECIMAL(10,2),
    costo_promedio_entretenimiento DECIMAL(10,2)
);

CREATE TABLE pais_dw(
    pais VARCHAR(100),
    continente VARCHAR(100),
    poblacion NUMERIC,
    tasa_envejecimiento DECIMAL,
    precio_big_mac DECIMAL,
    costo_hospedaje DECIMAL,
    costo_comida DECIMAL,
    costo_transporte DECIMAL,
    costo_entretenimiento DECIMAL
);
