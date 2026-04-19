import pandas as pd
import json
import os
import logging

ruta ="../data/raw/accidentes_raw.json"

def cargar_datos(ruta):
    #Leer datos desde archivo json
    if not os.path.exists(ruta):
        logging.error(f"Archivo no encontrado : {ruta}")
        return pd.DataFrame()
    df = pd.read_json(ruta)
    logging.info(f"Datos cargados: {len(df)} registros")
    return df

def seleccionar_columnas(df):
    """ Elimina columnas innecesarias """
    columnas_a_eliminar = [
        'texto_observaciones',
        'cuadrante_transito',
        'areas_transito',
        'fecha_hora',
        'geo_shape',
        'formulario'
    ]
    df = df.drop(columns=columnas_a_eliminar)
    logging.info(f"Columnas seleccionadas : {list(df.columns)}")
    return df

def limpiar_datos(df):
    """ Limpia y transforma datos"""
    # Rellenar nulos en columna 'clase'
    df['clase'] = df['clase'].fillna('DESCONOCIDO')
    
    # Convertir fecha_ocurrencia a tipo fecha
    df['fecha_ocurrencia'] = pd.to_datetime(
        df['fecha_ocurrencia'],errors='coerce'
    ).dt.date

    #Extraer latitud y longitud de geo_point 
    df['latitud'] = df['geo_point_2d'].apply(
        lambda x: x.get('lat') if isinstance(x,dict)else None
    )
    df['longitud'] = df['geo_point_2d'].apply(
        lambda x: x.get('lon') if isinstance(x, dict) else None
    )
    
    df =df.drop(columns=['geo_point_2d'])

    # Limpiar hora_ocurrencia 
    df['hora_ocurrencia'] = pd.to_datetime(
        df['hora_ocurrencia'],errors='coerce'
    ).dt.strftime('%H:%M:%S')

    # Limpiar texto - mayúsculas y sin espacios extra
    columnas_texto = ['gravedad','clase',
                      'localidad','municipio',
                      'dia_ocurrencia','mes_ocurrencia'
                      ]
    for col in columnas_texto:
        df[col] = df[col].str.strip().str.upper()
    
    logging.info("Limpieza Completada ")
    return df

def guardar_datos(df,ruta):
    """ Guardar el dataframe limiio en CSV."""
    df.to_csv(ruta, index=False, encoding='utf-8')
    logging.info(f"Datos procesados guardados en {ruta}")

# Bloque principal

if __name__ == "__main__":
    df = cargar_datos('../data/raw/accidentes_raw.json')
    if not df.empty:
        df = seleccionar_columnas(df)
        df = limpiar_datos(df)
        guardar_datos(df, '../data/processed/accidentes_clean.csv')
        print(f"\nRegistros procesados: {len(df)}")
        print(f"\nColumnas finales: {list(df.columns)}")
        print(f"\nMuestra:")
        print(df.head())
























