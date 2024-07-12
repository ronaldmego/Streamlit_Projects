#RUN: "streamlit run StreamLit_statuscargas.py"

import os
import psycopg2
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuración de la conexión
conn_params = {
    'host': os.getenv('HOST'),
    'port': os.getenv('PORT'),
    'dbname': os.getenv('DBNAME'),
    'user': os.getenv('USERNAMERS'),
    'password': os.getenv('PASSWORD'),
}

# Conectar a Redshift y obtener datos
def get_data():
    conn = None
    try:
        conn = psycopg2.connect(**conn_params)
        print("Conexión exitosa")

        # Crear un cursor
        cursor = conn.cursor()

        # Ejecutar la consulta
        query = "SELECT country, fuente, source, fecha, diferencia_dias, status FROM INFORMATION_DELIVERY_PROD.mfs_marketing.rm_lending_status;"
        cursor.execute(query)

        # Obtener los resultados y convertirlos a un DataFrame de Pandas
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=columns)

        return df

    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()  # Devolver un DataFrame vacío en caso de error

    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Conexión cerrada")

# Llamar a la función para obtener los datos
df = get_data()

# Configuración de Streamlit
st.title("Reporte de Actualización de Fuentes Lending")
st.write("Data Team Control")

# Función para aplicar color basado en el valor de status
def color_status(val):
    color = 'green' if val == 1 else 'red'
    return f'color: {color}'

# Mostrar el DataFrame en una tabla con formato
st.table(df.style.applymap(color_status, subset=['status']))
