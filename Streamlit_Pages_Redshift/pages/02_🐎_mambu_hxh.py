import os
import psycopg2
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
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
        query = "SELECT * FROM INFORMATION_DELIVERY_PROD.mfs_marketing.lending_hxh;"
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

# Calcular la fecha y hora más recientes
max_date = df['fecha'].max()
max_hour = df[df['fecha'] == max_date]['hora'].max()

# Configuración de Streamlit
st.title("Reporte de Disbursements por Fecha")
st.write("Data Team Control")

# Mostrar la fecha y hora más recientes
st.write(f"Most recent update: Date = {max_date}, Hour = {max_hour}")

# Selector de tipo de negocio
business_list = df['business'].unique().tolist()
selected_business = st.multiselect('Selecciona el tipo de negocio', business_list, default=['B2C'])

# Filtrar por tipo de negocio seleccionado
df_filtered = df[df['business'].isin(selected_business)]

# Selector de producto
product_list = df_filtered['product_id'].unique().tolist()
selected_products = st.multiselect('Selecciona los productos', product_list, default=product_list)

# Filtrar por productos seleccionados
df_filtered = df_filtered[df_filtered['product_id'].isin(selected_products)]

# Selector de rango de horas
hour_range = st.slider('Selecciona el rango de horas', 0, 23, (0, 23))

# Filtrar por rango de horas
df_filtered = df_filtered[(df_filtered['hora'] >= hour_range[0]) & (df_filtered['hora'] <= hour_range[1])]

# Agrupar por fecha y sumar los disbursements
df_grouped = df_filtered.groupby('fecha')['disbursements'].sum().reset_index()

# Crear gráfico de barras
fig, ax = plt.subplots()
bars = ax.barh(df_grouped['fecha'], df_grouped['disbursements'], color='blue')
ax.set_xlabel('Sum of Disbursements')
ax.set_ylabel('Fecha')
ax.set_title('Sum of Disbursements by Fecha')

# Agregar etiquetas en la parte superior de las barras
for bar in bars:
    width = bar.get_width()
    label_y = bar.get_y() + bar.get_height() / 2
    ax.text(width, label_y, f'{width:.0f}', ha='left', va='center')

# Mostrar gráfico en Streamlit
st.pyplot(fig)