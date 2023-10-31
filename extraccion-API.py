import json
import pandas as pd
from configparser import ConfigParser
import requests
import sqlalchemy as sa

# Defino la ruta de config y secciones
config_path = "config/config.ini"
config_section = "redshift"
config_section_api= "credenciales_api"


""" Función para obtener datos de la API y guardarlos en un DF"""

def get_data(url_base, endpoint, params, headers):
    try:
        # Construyo la URL completa para la API
        endpoint_url = f"{url_base}{endpoint}"
        
        # Realizo la peticion GET a la API
        resp = requests.get(endpoint_url, params=params, headers=headers)
        
        # Verifico si la peticion fue exitosa
        resp.raise_for_status()
        
        # Extraigo el contenido JSON de la respuesta
        data = resp.json()
        
        # Si hay datos, los convierto en un DF
        if data:
            df = pd.DataFrame(data)
            return df
        else:
            print("No se encontraron resultados.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"No se pudo acceder a la API: {e}")
        return None

""" Función para transformar los datos obtenidos de la API en el DF """
def transform_data(df, stock_ticker):
    # Inserto una nueva columna para el 'Ticker' al inicio del DF
    df.insert(0, 'Ticker', stock_ticker)
    # Le cambio el nombre de la columna 'open' a 'open_price' pq open es una palabra clave
    df.rename(columns={'open': 'open_price'}, inplace=True)
    df.rename(columns={'close': 'close_price'}, inplace=True)

    return df

""" Función para construir la cadena de conexión a la base de datos """

def build_conn_string(config_path, config_section):

    #Leo el archivo de config
    parser = ConfigParser()
    parser.read(config_path)


    #Leo la seccion de config para Redshift
    config = parser[config_section]
    host=config['host']
    port=config['port']
    dbname=config['dbname']
    username = config['username']
    pwd=config['pwd']

    #Construyo la cadena de conexión
    conn_string = f'postgresql://{username}:{pwd}@{host}:{port}/{dbname}?sslmode=require'

    return conn_string


""" Función para conectar a la base de datos """
def connect_to_db(conn_string):

    #Intengo conectarme a la BD, si falla me devuelve el error
    try:
        engine = sa.create_engine(conn_string)
        conn = engine.connect()
        print("Conexión exitosa a la base de datos.")
        # Leer el archivo SQL

        return conn, engine
    except sa.exc.SQLAlchemyError as e:
        print(f"No se pudo conectar a la base de datos: {e}")
        return None, None


if __name__ == "__main__":

    # Leo las credenciales del archivo config
    config = ConfigParser()
    config.read(config_path)
    token = config["credenciales_api"]["token"]

    # Defino las variables base para la API
    url_base = "https://api.iex.cloud/v1/data/core/"
    headers = {
        "Accept": "application/json"
        }
    
    # Defino los tickers de las acciones que quiero consultar
    tickers =  ['AAPL', 'GOOGL', 'KO', 'SPY', 'DIA', 'OXY', 'JNJ', 'META', 'NKE', 'QQQ', 'MSFT']
    
    df_stocks = pd.DataFrame()
    
    # Itero a traves de cada stock
    for stock_ticker in tickers:
        endpoint = f"intraday_prices/{stock_ticker}"

        # Establezco los Query parameters de la API
        params = {
            "range": "1d", 
            "token": token
            }
        
        # Obtengo los datos de la API
        df = get_data(url_base, endpoint, params, headers)
        
        # Si obtuve datos, los transformo y los agrego al DF general
        if df is not None:
            df_transformed = transform_data(df, stock_ticker)
            df_stocks = pd.concat([df_stocks, df_transformed], ignore_index=True)
            
    print(df_stocks)

    # Conecto a la base de datos y ejecuto el SQL para crear la tabla
    conn_str=build_conn_string(config_path ,config_section)
    conn, engine = connect_to_db(conn_str)
    with open('create_tables.sql', 'r') as f:
        create_table_query = f.read()

    #Intenta crear la tabla si no existe, si no puede crearla muestra el error.
    try:
        conn.execute(create_table_query)
        print("Tabla creada correctamente (o ya se encontraba creada).")
    except sa.exc.SQLAlchemyError as e:
        print(f"No se pudo crear la tabla: {e}")

    # Cierro la conexión a la base de datos
    conn.close()
    engine.dispose()
