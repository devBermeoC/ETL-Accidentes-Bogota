# ETL-Accidentes de Tráfico Bogotá entre 2007 - 2017

Pipeline ETL desarrollado en Python para la extracción, transformación y carga de datos de acccidentes de tráfico en bogota 
entre los años (2007-2017) desde una API pública hacia una base de datos relacional SQL server.

# Descripcción
Este proyecto immplementa un pipeline de datos completo que:
- Extrae 330.063 registros desde la API publica de OpenDataSoft.
- Limpia y transforma los datos aplicando criterios de calidad.
- Carga la información en una base de datos SQL server.
- Automatiza el proceso mediante scripts de PowerShell.
**Entorno:** Base de datos desplegada en Ubuntu Server virtualizado, accesible remotamente mediante SSH desde  Windows.



## Tecnologías

- Python 3.14 ( requests, pandas, pyodcb, python-dotenv).
- SQL Server.
- SQL Srver sobre Ubuntu Server (VM) - acceso remoto vía SSH.
- PowerShell
- Git / GitHub

# Estructura del Proyecto

| Carpeta / Archivo | Descripción |
|---|---|
| `config/.env` | Credenciales seguras — nunca sube a GitHub |
| `src/extract.py` | Extracción paginada desde API REST |
| `src/transform.py` | Limpieza y normalización de datos |
| `src/load.py` | Carga a SQL Server mediante pyodbc |
| `sql/create_tables.sql` | Modelo relacional con índices optimizados |
| `scripts/run_etl.ps1` | Automatización del pipeline en PowerShell |
| `data/raw/` | Datos crudos en JSON — ignorado por git |
| `data/processed/` | Datos limpios en CSV — ignorado por git |
| `requirements.txt` | Dependencias del proyecto |

## Instalación

```bash
# Clonar el repositorio
git clone https://github.com/devBermeoC/ETL-Accidentes-Bogota.git
cd ETL-Accidentes-Bogota

# Crear entorno virtual
py -m venv venv
.\venv\Scripts\Activate.ps1

# Instalar dependencias
pip install -r requirements.txt

# Configurar variables de entorno
# Crear archivo config/.env con:
# DB_SERVER=tu_servidor
# DB_NAME=accidentes_bogota
# DB_USER=tu_usuario
# DB_PASSWORD=tu_contraseña
# API_URL=https://transport.opendatasoft.com/api/explore/v2.1/catalog/datasets/accidente-de-trafico-en-bogota-entre-2007-y-2017-2/records
```

## Uso

```bash
# 1. Extraer datos desde la API
py src/extract.py

# 2. Transformar y limpiar datos
py src/transform.py

# 3. Cargar datos a SQL Server
py src/load.py
```

## Atributos de Calidad

**Seguridad:** Las credenciales se gestionan mediante variables de entorno 
y nunca se exponen en el código fuente.

**Disponibilidad:** El pipeline implementa manejo de errores y logging 
en cada etapa para garantizar trazabilidad y recuperación ante fallos.

## Dataset

- **Fuente:** [OpenDataSoft — Accidentes de Tráfico Bogotá](https://transport.opendatasoft.com/explore/dataset/accidente-de-trafico-en-bogota-entre-2007-y-2017-2)
- **Registros:** 330.063
- **Período:** 2007 — 2017
- **Ciudad:** Bogotá, Colombia


