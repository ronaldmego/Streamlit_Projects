# scripts/redshift_connection.py

import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv
import pandas as pd

# Cargar variables de entorno
load_dotenv()

def get_redshift_connection():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('DBNAME'),
            user=os.getenv('USERNAMERS'),
            password=os.getenv('PASSWORD'),
            host=os.getenv('HOST'),
            port=os.getenv('PORT')
        )
        return conn, None
    except Exception as e:
        print(f"Error al conectar con Redshift: {e}")
        return None, str(e)

def check_table_exists_redshift(full_table_name):
    """
    Verifica si una tabla existe en Redshift.
    El formato de full_table_name puede ser:
    - table_name
    - schema.table_name
    - database.schema.table_name
    Nota: Redshift no utiliza el concepto de base de datos como Snowflake, así que generalmente se usa schema.table_name
    """
    conn, error = get_redshift_connection()
    if not conn:
        return False, error
    
    try:
        cur = conn.cursor()
        
        # Construir la consulta para obtener la información de la tabla
        parts = full_table_name.split('.')
        if len(parts) == 1:
            # Solo nombre de la tabla
            table_name = parts[0].lower()
            schema_name = 'public'  # Esquema por defecto en Redshift
        elif len(parts) == 2:
            # Esquema y tabla
            schema_name, table_name = parts
            schema_name = schema_name.lower()
            table_name = table_name.lower()
        elif len(parts) == 3:
            # Redshift generalmente no utiliza la base de datos en el nombre de la tabla
            # Se asume que la primera parte es la base de datos y se ignora
            _, schema_name, table_name = parts
            schema_name = schema_name.lower()
            table_name = table_name.lower()
        else:
            return False, "Formato de nombre de tabla inválido"
        
        query = """
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = %s 
              AND table_name = %s
        """
        cur.execute(query, (schema_name, table_name))
        count = cur.fetchone()[0]
        exists = count > 0
        return exists, None
    except Exception as e:
        return False, str(e)
    finally:
        cur.close()
        conn.close()

def query_redshift_sample(table_name, date_value, date_column='time_extracted', limit=10):
    """
    Realiza un SELECT * con filtro de fecha y límite en Redshift.
    """
    conn, error = get_redshift_connection()
    if not conn:
        return None, error
    
    try:
        cur = conn.cursor()
        # Construir la consulta de manera segura
        query = sql.SQL("""
            SELECT * 
            FROM {} 
            WHERE DATE({}) = %s 
            LIMIT %s
        """).format(
            sql.Identifier(*table_name.split('.')),
            sql.Identifier(date_column)
        )
        cur.execute(query, (date_value, limit))
        columns = [desc[0] for desc in cur.description]
        results = cur.fetchall()
        df = pd.DataFrame(results, columns=columns)
        return df, None
    except Exception as e:
        return None, str(e)
    finally:
        cur.close()
        conn.close()

def get_total_record_count_redshift(table_name):
    """
    Obtiene el conteo total de registros en la tabla especificada en Redshift.
    """
    conn, error = get_redshift_connection()
    if not conn:
        return None, error
    
    try:
        cur = conn.cursor()
        query = sql.SQL("SELECT COUNT(*) AS total_records FROM {}").format(
            sql.Identifier(*table_name.split('.'))
        )
        cur.execute(query)
        count = cur.fetchone()[0]
        return count, None
    except Exception as e:
        return None, str(e)
    finally:
        cur.close()
        conn.close()

def get_record_count_by_date_redshift(table_name, date_column='time_extracted', sample_date=None, days=5):
    """
    Obtiene el conteo de registros agrupados por fecha para los últimos 'days' días a partir de 'sample_date' en Redshift.
    """
    conn, error = get_redshift_connection()
    if not conn:
        return None, error

    if sample_date is None:
        return None, "El parámetro 'sample_date' es requerido."

    try:
        cur = conn.cursor()
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
        cur.execute(query, (days-1, sample_date_str, sample_date_str))
        results = cur.fetchall()
        df = pd.DataFrame(results, columns=['extraction_date', 'count'])
        return df, None
    except Exception as e:
        return None, str(e)
    finally:
        cur.close()
        conn.close()

def get_columns_redshift(table_name):
    """
    Obtiene la estructura de las columnas de una tabla en Redshift.
    """
    conn, error = get_redshift_connection()
    if not conn:
        return None, error

    try:
        cur = conn.cursor()
        query = sql.SQL("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = %s
            ORDER BY ordinal_position
        """)
        # Extraer esquema y tabla
        parts = table_name.split('.')
        if len(parts) == 3:
            _, schema_name, table_name_only = parts
        elif len(parts) == 2:
            schema_name, table_name_only = parts
        elif len(parts) == 1:
            schema_name = 'public'  # Esquema por defecto en Redshift
            table_name_only = parts[0]
        else:
            return None, "Formato de nombre de tabla inválido"
        
        cur.execute(query, (table_name_only.lower(), schema_name.lower()))
        results = cur.fetchall()
        df = pd.DataFrame(results, columns=['column_name', 'data_type'])
        return df, None
    except Exception as e:
        return None, str(e)
    finally:
        cur.close()
        conn.close()

def get_top_frequent_data_redshift(table_name, date_column, sample_date, column, top_n=5):
    """
    Obtiene los top_n datos más frecuentes para una columna específica en Redshift para una fecha dada.
    
    Args:
        table_name (str): Nombre completo de la tabla.
        date_column (str): Nombre de la columna de fecha.
        sample_date (datetime.date): Fecha de muestreo.
        column (str): Nombre de la columna a analizar.
        top_n (int): Número de datos más frecuentes a obtener.
    
    Returns:
        tuple: (DataFrame, error)
    """
    conn, error = get_redshift_connection()
    if not conn:
        return None, error
    
    query = f"""
    SELECT {column} AS value, COUNT(*) AS count
    FROM {table_name}
    WHERE DATE({date_column}) = '{sample_date.strftime('%Y-%m-%d')}'
    GROUP BY {column}
    ORDER BY count DESC
    LIMIT {top_n};
    """
    
    try:
        df = pd.read_sql(query, conn)
        conn.close()
        return df, None
    except Exception as e:
        conn.close()
        return None, str(e)