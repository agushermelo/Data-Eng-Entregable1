import logging
import json
import pandas as pd
from configparser import ConfigParser
import requests
import sqlalchemy as sa
from utils import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Defino la ruta de config y secciones
config_path = "/Users/agustinahermelo/Desktop/Coder-Data-Eng/config/config.ini"
config_section = "redshift"
config_section_api= "credenciales_api"

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
engine = connect_to_db(config_path, config_section)
if engine is None:
    print("No se pudo crear el objeto Engine.")
    exit()  # Salir del script si no se puede establecer la conexión
conn = engine.connect()
with open('create_tables.sql', 'r') as f:
    create_table_query = f.read()
    #Intenta crear la tabla si no existe, si no puede crearla muestra el error.
try:
    conn.execute(create_table_query)
    print("Tabla creada correctamente (o ya se encontraba creada).")
except sa.exc.SQLAlchemyError as e:
    print(f"No se pudo crear la tabla: {e}")

""" Cargo los datos en la base de datos"""

#Primero verifico los types
print(df_stocks.dtypes)

# Convierto los tipos de datos
df_stocks['Ticker'] = df_stocks['Ticker'].astype('object')
df_stocks['date'] = pd.to_datetime(df_stocks['date']).dt.date
df_stocks['minute'] = pd.to_datetime(df_stocks['minute'], format='%H:%M').dt.time
df_stocks['label'] = df_stocks['label'].astype('object')
df_stocks[['high', 'low', 'open_price', 'close_price', 'average']] = df_stocks[['high', 'low', 'open_price', 'close_price', 'average']].apply(pd.to_numeric, errors='coerce')
df_stocks[['volume', 'notional', 'numberOfTrades']] = df_stocks[['volume', 'notional', 'numberOfTrades']].apply(pd.to_numeric, errors='coerce', downcast='float')

#Reemplazo los valores Nan por 0
df_stocks.fillna(0, inplace=True)
#Elimino las filas donde no hay datos
cols_to_check = ['high', 'low', 'open_price', 'close_price', 'average', 'volume', 'notional', 'numberOfTrades']
df_stocks = df_stocks.loc[~(df_stocks[cols_to_check] == 0).all(axis=1)]
print(df_stocks)

#Cargo los datos en la base de datos
load_to_db(df_stocks)

#Quiero verificar que se cargo la informacion y realizo un print
df_stocks = load_table_to_df(engine, "stocksinfo", limit=20)
print(df_stocks)
# Cierro la conexión a la base de datos
conn.close()
engine.dispose()