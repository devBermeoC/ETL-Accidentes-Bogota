import pandas as pd
import pyodbc
from dotenv import load_dotenv
import os
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

load_dotenv(Path(__file__).parent.parent / 'config' / '.env')

DB_SERVER = os.getenv('DB_SERVER')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

def conectar():
    """ Conectar y retornar la conexión a SQL server en la VM Ubuntu Server"""
    try:
        conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +  DB_SERVER + ',1433;DATABASE=' + DB_NAME + ';UID=' + DB_USER + ';PWD=' + DB_PASSWORD + ';TrustServerCertificate=yes;'
        conn = pyodbc.connect(conn_str)
        logging.info("Conexión Exitosa a SQL Server")
        return conn  
    except Exception as e:
        logging.info(f"Error de conexión: {e}")
        return None

def cargar_datos(conn,ruta_csv):
    """Carga archivo CSV limpio en la tabla de accidentes """
    df = pd.read_csv(ruta_csv)
    logging.info(f"Registros a cargar: {len(df)}")

    cursor = conn.cursor()
    cursor.fast_executemany = True

    sql = """
          INSERT INTO dbo.accidentes(
          objectid, codigo_accidente, fecha_ocurrencia,
          hora_ocurrencia, ano_ocurrencia, mes_ocurrencia,
          dia_ocurrencia, direccion, gravedad, clase,
          localidad, municipio, latitud, longitud         
          ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    registros = [tuple(row) for row in df.itertuples(index=False)]

    try:
        cursor.executemany(sql, registros)
        conn.commit()
        logging.info(f"{len(registros)} Registros cargados exitosamente ")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error al cargar los datos : {e}")
    finally:
        cursor.close()

if __name__ == "__main__":
    from pathlib import Path
    ruta_csv = Path(__file__).parent.parent / 'data' / 'processed' / 'accidentes_clean.csv'
    conn = conectar()
    if conn:
        cargar_datos(conn, ruta_csv)
        conn.close()
        logging.info("Conexión Cerrada ")




