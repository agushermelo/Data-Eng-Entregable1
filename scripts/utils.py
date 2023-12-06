import logging
import json
import pandas as pd
from configparser import ConfigParser
import requests
import sqlalchemy as sa
from sqlalchemy import create_engine

# Defino la ruta de config y secciones
config_path = "/Users/agustinahermelo/Desktop/Coder-Data-Eng/config/config.ini"
config_section = "redshift"
config_section_api= "credenciales_api"

def get_data(url_base, endpoint, params, headers):
    """
    Realiza una solicitud GET a una API y devuelve los datos en un DataFrame.

    Parameters:
    url_base (str): URL base de la API.
    endpoint (str): Endpoint específico de la API.
    params (dict): Parámetros para la solicitud GET.
    headers (dict): Encabezados para la solicitud.

    Returns:
    df: DataFrame con los datos obtenidos de la API, o None si no se encuentran datos o hay un error.
    """
    try:
        logging.info(f"Obteniendo datos de la API..")
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
            logging.info(f"Datos obtenidos exitosamente.")
            return df
        else:
            print("No se encontraron resultados.")
            return None
    except requests.exceptions.RequestException as e:
        logging.info(f"No se pudo acceder a la API: {e}")
        return None

def transform_data(df, stock_ticker):
    """
    Añade una columna de identificador de stock al DataFrame y renombra ciertas columnas.

    Parameters:
    df (pandas.DataFrame): DataFrame con datos de stock.
    stock_ticker (str): Ticker del stock que va a añadir.

    Returns:
    pandas.DataFrame: DataFrame transformado con la columna adicional y columnas renombradas.
    """
    # Inserto una nueva columna para el 'Ticker' al inicio del DF
    df.insert(0, 'Ticker', stock_ticker)
    # Le cambio el nombre de la columna 'open' a 'open_price' pq open es una palabra clave
    df.rename(columns={'open': 'open_price'}, inplace=True)
    df.rename(columns={'close': 'close_price'}, inplace=True)

    return df

def build_conn_string(config_path, config_section):
    """
    Construye una cadena de conexión para una base de datos PostgreSQL a partir de un archivo de configuración INI.

    Parameters:
    config_path (str): Ruta al archivo de configuración INI.
    config_section (str): Sección dentro del archivo de configuración que contiene los parámetros de la base de datos.

    Returns:
    str: Cadena de conexión para la base de datos PostgreSQL.

    El archivo INI debe contener al menos los siguientes parámetros en la sección especificada:
    host, port, dbname, username, pwd.
    """
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

def connect_to_db(config_file, section):
    """
    Establece una conexión con la base de datos utilizando parámetros de configuración.

    Parameters:
    config_file (str): Ruta al archivo de configuración que contiene los detalles de conexión.
    section (str): Sección dentro del archivo de configuración que contiene los parámetros específicos.

    Returns:
    sqlalchemy.engine.base.Engine: Objeto de conexión a la base de datos, o None si la conexión falla.

    Raises:
    Exception: Si la sección especificada no se encuentra en el archivo de configuración.
    """
    try:
        parser = ConfigParser()
        parser.read(config_file)

        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            db = {param[0]: param[1] for param in params}

            logging.info("Conectándose a la base de datos...")
            engine = create_engine(
                f"postgresql://{db['username']}:{db['pwd']}@{db['host']}:{db['port']}/{db['dbname']}"
                )

            logging.info("Conexión a la base de datos establecida exitosamente")
            return engine

        else:
            raise Exception(f"No se encontró la sección {section} en el archivo {config_file}")
    except Exception as e:
        logging.error(f"Error al conectarse a la base de datos: {e}")
        return None

def load_to_sql(df, table_name, engine, if_exists="replace"):
    """
    Carga un DataFrame en una tabla de base de datos especificada.

    Parameters:
    df (pandas.DataFrame): DataFrame a cargar.
    table_name (str): Nombre de la tabla destino.
    engine (sqlalchemy.engine.base.Engine): Motor de conexión a la base de datos.
    if_exists (str): Acción a realizar si la tabla existe ('replace', 'append', etc.).

    """
    try:
        logging.info("Cargando datos en la base de datos...")
        df.to_sql(
            table_name,
            engine,
            if_exists=if_exists,
            index=False,
            method="multi"
            )
        logging.info("Datos cargados exitosamente en la base de datos")
    except Exception as e:
        logging.error(f"Error al cargar los datos en la base de datos: {e}")

def load_to_db(df):
    """
    Carga datos en una tabla de la base de datos y ejecuta una instrucción MERGE.

    Parameters:
    df (pandas.DataFrame): DataFrame con los datos a cargar.


    Utiliza la función 'connect_to_db' para establecer la conexión y 'load_to_sql' para la carga de datos.
    La función ejecuta operaciones de TRUNCATE y MERGE en la base de datos.
    """
    engine = connect_to_db(config_path, "redshift")
    if engine is not None:
        with engine.connect() as conn:
            with conn.begin():
                conn.execute("TRUNCATE TABLE stocksinfo_stg")

                load_to_sql(df,"stocksinfo_stg",conn,"append")

                conn.execute("""
                    MERGE INTO stocksinfo
                    USING stocksinfo_stg
                    ON stocksinfo.Ticker = stocksinfo_stg.Ticker AND stocksinfo.date = stocksinfo_stg.date AND stocksinfo.minute = stocksinfo_stg.minute
                    WHEN MATCHED THEN
                        UPDATE SET
                            label = stocksinfo_stg.label,
                            high = stocksinfo_stg.high,
                            low = stocksinfo_stg.low,
                            open_price = stocksinfo_stg.low,
                            close_price = stocksinfo_stg.close_price,
                            average = stocksinfo_stg.average,
                            volume = stocksinfo_stg.volume,
                            notional = stocksinfo_stg.notional,
                            numberOfTrades = stocksinfo_stg.numberOfTrades
                    WHEN NOT MATCHED THEN
                        INSERT (Ticker, date, minute, label, high, low, open_price, close_price, average, volume, notional, numberOfTrades)
                        VALUES (stocksinfo_stg.Ticker, stocksinfo_stg.date, stocksinfo_stg.minute, stocksinfo_stg.label, stocksinfo_stg.high, stocksinfo_stg.low, stocksinfo_stg.open_price, stocksinfo_stg.close_price, stocksinfo_stg.average, stocksinfo_stg.volume, stocksinfo_stg.notional, stocksinfo_stg.numberOfTrades)
                            """)

def load_table_to_df(engine, table_name, limit=None):
    """
    Carga datos de una tabla de base de datos en un DataFrame.

    Parameters:
    engine (sqlalchemy.engine.base.Engine): Motor de conexión a la base de datos.
    table_name (str): Nombre de la tabla de donde cargar los datos.
    limit (int, opcional): Número máximo de filas a cargar.

    Returns:
    pandas.DataFrame: DataFrame con los datos cargados de la tabla especificada.

    Si 'limit' es None, carga todas las filas de la tabla.
    """
    query = f"SELECT * FROM {table_name}"
    if limit is not None:
        query += f" LIMIT {limit}"

    df = pd.read_sql(query, engine)
    return df