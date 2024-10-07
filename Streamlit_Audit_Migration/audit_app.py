# audit_app.py

import streamlit as st
import os
from sections.verify_table import verify_table
from sections.sample_query import run_sample_query
from sections.compare_total_records import compare_total_records
from sections.compare_records_by_date import compare_records_by_date
from sections.compare_columns import compare_columns
from sections.generate_report import generate_report
from sections.top_frequent_data import top_frequent_data  # Importar el nuevo módulo
import pandas as pd
from datetime import datetime, timedelta

# Obtener la fecha actual
today = datetime.today().date()

# Restar un día
default_sample_date = today - timedelta(days=1)

# CSS para ampliar el ancho del sidebar
def set_sidebar_width(width=500):
    st.markdown(
        f"""
        <style>
        /* Ampliar el ancho del sidebar */
        .css-1d391kg {{  /* Clase CSS para el sidebar en Streamlit */
            width: {width}px;
        }}
        /* Ajustar el margen principal para acomodar el sidebar más ancho */
        .css-1y4p8pa {{  /* Clase CSS para el contenido principal */
                margin-left: {width}px;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

# Inicializar la aplicación
def init_app():
    set_sidebar_width()
    st.title("Auditoría de Migración de Tablas: Snowflake a Redshift")
    st.sidebar.header("Configuración de Auditoría")

# Entrada de configuración
def get_user_inputs():
    full_table_name = st.sidebar.text_input(
        "Nombre Completo de la Tabla",
        value="INFORMATION_DELIVERY_PROD.mfs_lending.info_mmb_py_loan_accounts",
        help="Ingrese el nombre de la tabla en uno de los siguientes formatos:\n- table_name\n- schema.table_name\n- database.schema.table_name"
    )
    
    date_column = st.sidebar.text_input(
        "Columna de Fecha",
        value="time_extracted",
        help="Ingrese el nombre de la columna que contiene la fecha de extracción."
    )
    
    sample_date = st.sidebar.date_input(
        "Fecha de Muestreo",
        value=default_sample_date,
        min_value=datetime(2000, 1, 1).date(),
        max_value=datetime(2100, 12, 31).date(),
        help="Seleccione la fecha para la cual desea realizar la muestra de datos."
    )

    
    return full_table_name, date_column, sample_date

# Función principal
def main():
    init_app()
    full_table_name, date_column, sample_date = get_user_inputs()
    
    # Botón para verificar existencia de la tabla
    if st.button("Verificar Tabla"):
        if not full_table_name:
            st.error("Por favor, ingresa el nombre de una tabla.")
        else:
            verify_table(full_table_name)
    
    # Separador
    st.markdown("---")
    
    # Sección para consultas de muestra
    st.header("Prueba de Consulta de Muestra: SELECT * WHERE DATE(time_extracted) = 'YYYY-MM-DD' LIMIT 10")
    
    if st.button("Ejecutar Consulta de Muestra"):
        if not full_table_name:
            st.error("Por favor, ingresa el nombre de una tabla para ejecutar la consulta de muestra.")
        elif not date_column:
            st.error("Por favor, ingresa el nombre de la columna de fecha.")
        else:
            run_sample_query(full_table_name, date_column, sample_date)
    
    # Separador
    st.markdown("---")
    
    # Sección para comparación de la cantidad total de registros
    st.header("Comparación de la Cantidad Total de Registros")
    
    if st.button("Comparar Cantidad Total de Registros"):
        if not full_table_name:
            st.error("Por favor, ingresa el nombre de una tabla para comparar la cantidad total de registros.")
        else:
            compare_total_records(full_table_name)
    
    # Separador
    st.markdown("---")
    
    # Sección para comparación de registros por fecha
    st.header("Comparación de Registros por Fecha (Últimos 5 Días)")
    
    if st.button("Comparar Registros por Fecha"):
        if not full_table_name:
            st.error("Por favor, ingresa el nombre de una tabla para comparar los registros por fecha.")
        elif not date_column:
            st.error("Por favor, ingresa el nombre de la columna de fecha.")
        else:
            compare_records_by_date(full_table_name, date_column, sample_date)
    
    # Separador
    st.markdown("---")
    
    # Nueva Sección: Comparación de Columnas
    st.header("Comparación de Columnas entre Snowflake y Redshift")
    
    if st.button("Comparar Columnas"):
        if not full_table_name:
            st.error("Por favor, ingresa el nombre de una tabla para comparar las columnas.")
        else:
            compare_columns(full_table_name)
    
    # Separador
    st.markdown("---")
    
    # Nueva Sección: Análisis de Datos Más Frecuentes
    st.header("Análisis de los 3 Datos Más Frecuentes por Columna")
    
    if st.button("Analizar Datos Más Frecuentes"):
        if not full_table_name:
            st.error("Por favor, ingresa el nombre de una tabla para analizar los datos más frecuentes.")
        elif not date_column:
            st.error("Por favor, ingresa el nombre de la columna de fecha.")
        else:
            top_frequent_data(full_table_name, date_column, sample_date, top_n=3, max_columns=None)  # max_columns=10 para pruebas
    
    # Separador
    st.markdown("---")
    
    # Nueva Sección: Generación de Informe
    st.sidebar.header("Generación de Informe de Auditoría")
    
    if st.sidebar.button("Generar Informe"):
        if not full_table_name:
            st.sidebar.error("Por favor, ingresa el nombre de una tabla para generar el informe.")
        else:
            generate_report(full_table_name, date_column, sample_date)

if __name__ == "__main__":
    main()