import requests
import pandas as pd
from dotenv import load_dotenv
import os
import logging
import json

# Configuración del log - Atributo de disponibilidad
logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s - %(levelname)s - %(message)s'
)

# Carga de las variables del archivo .env - atributo de seguridad
load_dotenv('config/.env')
API_URL = os.getenv('API_URL')


def extraer_datos(limit=100, offset=0):
    """
        Conecta a la API y extrae los registros de los accidentes.
        limit : Nro de registros al traer por llamada
        offset : Registro desde el cual se inicia
    """
    try:
        params = {
            
            'limit' :  limit,
            'offset':  offset
        }
        logging.info(f"Extrayendo registros desde offset {offset}...")
        response = requests.get(API_URL, params= params,timeout=30)
        response.raise_for_status()
        data = response.json()
        registros = data.get('results',[])
        total = data.get('total_count',0)
        logging.info(f"Registros exraídos : {len(registros)} de {total} totales")
        return registros, total
    except requests.exceptions.Timeout:
        logging.error("La API no responde a tiempo")
        return [], 0
    except requests.exceptions.ConnectionError:
        logging.error("No se puede conectar a la API")
        return [], 0
    except Exception as e:
        logging.error(f"Error inesperado: {e}")
        return [], 0


def extraer_todos(batch_size = 100, max_registros=5000):
    """
    Extrae todos los registros de la API en lotes.
    batch_size : Número de registros que se traen por lote
    """
    todos = []
    offset = 0

    # Primera llamada para conocer el total
    registros, total = extraer_datos(limit = batch_size, offset=0)
    if not registros:
        logging.error("No se obtuvieron datos. Verificar conexión a URL. ")
        return pd.DataFrame()
    todos.extend(registros)
    offset += len(registros)

    # Continuar ciclo de extraxión hasta finalizar
    while offset <  total and len(todos) < max_registros:
        registros, _ = extraer_datos(limit=batch_size, offset=offset)
        if not registros:
            break
        todos.extend(registros)
        offset += len(registros)
    
    logging.info(f"Extraxión completa : {len(todos)} registros totales ")
    return pd.DataFrame(todos)

if __name__ == "__main__":
    df = extraer_todos(batch_size=100, max_registros=5000)
    if not df.empty:
        df.to_json('data/raw/accidentes_raw.json',
                   orient='records',
                   force_ascii=False,
                   indent=2)
        logging.info("Datos guardados en data/raw/accidentes_raw.json")
        print(f"Total registros : {len(df)}")
        print(f"\nColumnas: {list(df.columns)}")
       





