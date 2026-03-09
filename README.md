# Lab5-ETL: Integración de Datos SQL + NoSQL mediante ETL

El pipeline ETL permite:

- Extraer datos desde **PostgreSQL**
- Extraer datos desde **archivos JSON**
- Integrar los datos en memoria utilizando **pandas**
- Cargar los datos integrados en un **Data Warehouse**

Los datos integrados contienen información sobre:

- País
- Población
- Tasa de envejecimiento
- Costos turísticos
- Índice Big Mac


---

# Tecnologías Utilizadas

- Python
- Pandas
- SQLAlchemy
- psycopg2
- PostgreSQL
- Docker
- WSL (Windows Subsystem for Linux)


---

# Requisitos

Antes de ejecutar el proyecto es necesario tener instalado:

- Docker
- WSL (si se trabaja desde Windows)
- Python 3

---

# Paso 1: Levantar la base de datos con Docker

Desde la carpeta del proyecto ejecutar:

```bash
docker compose up
```
Esto iniciará un contenedor con PostgreSQL y automáticamente ejecutará:

```bash
init/01_ddl.sql
init/02_data.sql
```

Si se necesita reiniciar completamente la base de datos:
```bash
docker compose down -v
docker compose up
```

# Paso 2: Crear entorno virtual en WSL
Desde WSL navegar a la carpeta del proyecto:
```bash
cd /mnt/c/Users/isabe/OneDrive/Escritorio/Lab5-ETL
```
Crear el entorno virtual:
```bash
python3 -m venv venv
```
Activarlo:
```bash
source venv/bin/activate
```
# Paso 3: Instalar dependencias
Con el entorno virtual activo instalar las librerías necesarias:
```bash
pip install pandas sqlalchemy psycopg2-binary
```

# Paso 4: Ejecutar el pipeline ETL
Con el entorno virtual activo ejecutar:
```bash
python etl/pipeline.py
```
Si todo funciona correctamente se mostrará algo similar a:
```bash
Número de registros: 106
ETL ejecutado correctamente
```
---

# Verificación en la base de datos

Para verificar que los datos fueron cargados correctamente en el Data Warehouse:

Entrar al contenedor de PostgreSQL:
```bash
docker exec -it lab5_sql psql -U user -d lab5_sql
```
Ejecutar:
```bash
SELECT * FROM pais_dw LIMIT 10;
```
