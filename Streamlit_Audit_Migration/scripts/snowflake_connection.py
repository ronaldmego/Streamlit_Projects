# scripts/snowflake_connection.py

import snowflake.connector
import os
from dotenv import load_dotenv
import pandas as pd

# Cargar variables de entorno
load_dotenv()

def get_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('USER_SNOW'),
            password=os.getenv('PASSWORD_SNOW'),
            account=os.getenv('ACCOUNT_SNOW'),
            warehouse=os.getenv('WAREHOUSE_SNOW'),
            database=os.getenv('DATABASE_SNOW'),
            schema=os.getenv('SCHEMA_SNOW')
        )
        return conn, None
    except Exception as e:
        print(f"Error al conectar con Snowflake: {e}")
        return None, str(e)

def check_table_exists_snowflake(full_table_name):
    """
    Verifica si una tabla existe en Snowflake.
    El formato de full_table_name puede ser:
    - table_name
    - schema.table_name
    - database.schema.table_name
    """
    conn, error = get_snowflake_connection()
    if not conn:
        return False, error
    
    try:
        cs = conn.cursor()
        
        # Construir la consulta para obtener la información de la tabla
        parts = full_table_name.split('.')
        if len(parts) == 1:
            # Solo nombre de la tabla
            table_name = parts[0]
            schema_name = os.getenv('SCHEMA_SNOW')
            database_name = os.getenv('DATABASE_SNOW')
        elif len(parts) == 2:
            # Esquema y tabla
            schema_name, table_name = parts
            database_name = os.getenv('DATABASE_SNOW')
        elif len(parts) == 3:
            # Base de datos, esquema y tabla
            database_name, schema_name, table_name = parts
        else:
            return False, "Formato de nombre de tabla inválido"
        
        query = """
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = %s 
              AND TABLE_NAME = %s
        """
        cs.execute(query, (schema_name.upper(), table_name.upper()))
        count = cs.fetchone()[0]
        exists = count > 0
        return exists, None
    except Exception as e:
        return False, str(e)
    finally:
        cs.close()
        conn.close()

def query_snowflake_sample(table_name, date_value, date_column='time_extracted', limit=10):
    """
    Realiza un SELECT * con filtro de fecha y límite en Snowflake.
    """
    conn, error = get_snowflake_connection()
    if not conn:
        return None, error
    
    try:
        cs = conn.cursor()
        # Construir la consulta con parámetros para evitar inyección SQL
        query = f"""
            SELECT * 
            FROM {table_name} 
            WHERE DATE({date_column}) = %s 
            LIMIT %s
        """
        cs.execute(query, (date_value, limit))
        df = pd.DataFrame.from_records(iter(cs), columns=[desc[0] for desc in cs.description])
        return df, None
    except Exception as e:
        return None, str(e)
    finally:
        cs.close()
        conn.close()

def get_total_record_count_snowflake(table_name):
    """
    Obtiene el conteo total de registros en la tabla especificada en Snowflake.
    """
    conn, error = get_snowflake_connection()
    if not conn:
        return None, error
    
    try:
        cs = conn.cursor()
        query = f"SELECT COUNT(*) AS total_records FROM {table_name}"
        cs.execute(query)
        count = cs.fetchone()[0]
        return count, None
    except Exception as e:
        return None, str(e)
    finally:
        cs.close()
        conn.close()

def get_record_count_by_date_snowflake(table_name, date_column='time_extracted', sample_date=None, days=5):
    """
    Obtiene el conteo de registros agrupados por fecha para los últimos 'days' días a partir de 'sample_date' en Snowflake.
    """
    conn, error = get_snowflake_connection()
    if not conn:
        return None, error

    if sample_date is None:
        return None, "El parámetro 'sample_date' es requerido."

    try:
        cs = conn.cursor()
        # Convertir sample_date a string en formato 'YYYY-MM-DD'
        sample_date_str = sample_date.strftime('%Y-%m-%d')

        # Construir la consulta con parámetros
        query = f"""
            SELECT DATE({date_column}) AS extraction_date, COUNT(*) AS count
            FROM {table_name}
            WHERE DATE({date_column}) BETWEEN DATEADD(day, -%s, %s::DATE) AND %s::DATE
            GROUP BY DATE({date_column})
            ORDER BY extraction_date DESC
        """
        # Ejecutar la consulta con parámetros
        cs.execute(query, (days-1, sample_date_str, sample_date_str))
        results = cs.fetchall()
        df = pd.DataFrame(results, columns=['extraction_date', 'count'])
        return df, None
    except Exception as e:
        return None, str(e)
    finally:
        cs.close()
        conn.close()

def get_columns_snowflake(table_name):
    """
    Obtiene la estructura de las columnas de una tabla en Snowflake.
    """
    conn, error = get_snowflake_connection()
    if not conn:
        return None, error

    try:
        cs = conn.cursor()
        query = """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = %s AND TABLE_SCHEMA = %s
            ORDER BY ORDINAL_POSITION
        """
        # Extraer esquema y tabla
        parts = table_name.split('.')
        if len(parts) == 3:
            _, schema_name, table_name_only = parts
        elif len(parts) == 2:
            schema_name, table_name_only = parts
        elif len(parts) == 1:
            schema_name = os.getenv('SCHEMA_SNOW')
            table_name_only = parts[0]
        else:
            return None, "Formato de nombre de tabla inválido"
        
        cs.execute(query, (table_name_only.upper(), schema_name.upper()))
        results = cs.fetchall()
        df = pd.DataFrame(results, columns=['column_name', 'data_type'])
        return df, None
    except Exception as e:
        return None, str(e)
    finally:
        cs.close()
        conn.close()

def get_top_frequent_data_snowflake(table_name, date_column, sample_date, column, top_n=5):
    """
    Obtiene los top_n datos más frecuentes para una columna específica en Snowflake para una fecha dada.
    
    Args:
        table_name (str): Nombre completo de la tabla.
        date_column (str): Nombre de la columna de fecha.
        sample_date (datetime.date): Fecha de muestreo.
        column (str): Nombre de la columna a analizar.
        top_n (int): Número de datos más frecuentes a obtener.
    
    Returns:
        tuple: (DataFrame, error)
    """
    conn, error = get_snowflake_connection()
    if not conn:
        return None, error
    
    query = f"""
    SELECT {column} AS value, COUNT(*) AS count
    FROM {table_name}
    WHERE DATE({date_column}) = '{sample_date.strftime('%Y-%m-%d')}'
    GROUP BY {column}
    ORDER BY count DESC, value asc
    LIMIT {top_n};
    """
    
    try:
        df = pd.read_sql(query, conn)
        conn.close()
        return df, None
    except Exception as e:
        conn.close()
        return None, str(e)
