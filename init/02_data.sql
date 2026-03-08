COPY pais(id_pais, nombre_pais, capital, continente, region, poblacion, tasa_de_envejecimiento)
FROM '/tmp/data/pais_envejecimiento.csv'
DELIMITER ','
CSV HEADER;

COPY pais_poblacion(id, continente, pais, poblacion, costo_bajo_hospedaje, 
                    costo_promedio_comida, costo_bajo_transporte, costo_promedio_entretenimiento)
FROM '/tmp/data/pais_poblacion.csv'
DELIMITER ','
CSV HEADER;
