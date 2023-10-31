# README - Obtención y Almacenamiento de Datos de Acciones
## Descripción General
Este script Python realiza las siguientes tareas:

1. Obtiene datos de acciones de la API de IEX Cloud, específicamente precios intradiarios.
2. Transforma los datos obtenidos añadiendo un 'ticker' para identificación.
3. Se conecta a una base de datos Redshift para almacenar los datos.
4. Crea una nueva tabla en Redshift si no existe.
5. Almacena los datos transformados en la tabla.

## Requisitos
Bibliotecas: pandas, requests, json, configparser, sqlalchemy, os

## Limitaciones de la API
Con el Token utilizado, la API de IEX Cloud tiene un límite de llamados 5 Requests por segundo

## Estructura del código
get_data: Función para realizar peticiones GET a la API y obtener datos de stock.
transform_data: Función para transformar los datos recogidos, como añadir una columna de 'ticker' y renombrar columnas.
build_conn_string: Función para construir la cadena de conexión para Redshift a partir del archivo config.ini.
connect_to_db: Función para establecer una conexión con la base de datos Redshift.

## Diseño de la Base de Datos
El script SQL utilizado para crear la tabla en Redshift especifica `DISTKEY (Ticker)` y `SORTKEY (date, minute)` por las siguientes razones:

#### DISTKEY (Ticker)
- Al utilizar 'Ticker' como la clave de distribución, aseguramos que todos los datos relacionados con un determinado símbolo bursátil se almacenen juntos, lo que facilita y acelera las operaciones de join y reduce el movimiento de datos entre nodos.

#### SORTKEY (date, minute)
- Al ordenar los datos primero por 'date' y luego por 'minute', se mejora el rendimiento de las consultas que filtran por estos campos, ya que Redshift puede saltar rápidamente a los bloques de datos que contienen los registros relevantes.

