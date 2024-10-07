# sections/generate_report.py

import streamlit as st
import os
from scripts.report_generation import generate_audit_report
from scripts.snowflake_connection import (
    check_table_exists_snowflake, 
    get_total_record_count_snowflake,
    get_record_count_by_date_snowflake,
    get_columns_snowflake
)
from scripts.redshift_connection import (
    check_table_exists_redshift, 
    get_total_record_count_redshift,
    get_record_count_by_date_redshift,
    get_columns_redshift
)
from sections.top_frequent_data import get_frequent_data_for_report  # Corregido
from utils.helpers import format_date, handle_error, run_parallel_queries

def generate_report(full_table_name, date_column, sample_date):
    st.sidebar.write(f"## Generando informe para la tabla **{full_table_name}**...")
    
    # Verificar existencia de la tabla
    try:
        snowflake_exists, snowflake_error = check_table_exists_snowflake(full_table_name)
    except Exception as e:
        snowflake_exists, snowflake_error = False, str(e)

    try:
        redshift_exists, redshift_error = check_table_exists_redshift(full_table_name)
    except Exception as e:
        redshift_exists, redshift_error = False, str(e)

    # Obtener conteo total de registros
    try:
        snowflake_total, error_snowflake_total = get_total_record_count_snowflake(full_table_name)
    except Exception as e:
        snowflake_total, error_snowflake_total = None, str(e)

    try:
        redshift_total, error_redshift_total = get_total_record_count_redshift(full_table_name)
    except Exception as e:
        redshift_total, error_redshift_total = None, str(e)

    # Obtener conteo de registros por fecha (últimos 5 días)
    try:
        snowflake_dates_df, error_snowflake_dates = get_record_count_by_date_snowflake(
            table_name=full_table_name,
            date_column=date_column,
            sample_date=sample_date,
            days=5
        )
    except Exception as e:
        snowflake_dates_df, error_snowflake_dates = None, str(e)

    try:
        redshift_dates_df, error_redshift_dates = get_record_count_by_date_redshift(
            table_name=full_table_name,
            date_column=date_column,
            sample_date=sample_date,
            days=5
        )
    except Exception as e:
        redshift_dates_df, error_redshift_dates = None, str(e)

    # Obtener estructura de columnas
    try:
        columns_snowflake_df, error_snowflake_columns = get_columns_snowflake(full_table_name)
    except Exception as e:
        columns_snowflake_df, error_snowflake_columns = None, str(e)

    try:
        columns_redshift_df, error_redshift_columns = get_columns_redshift(full_table_name)
    except Exception as e:
        columns_redshift_df, error_redshift_columns = None, str(e)

    # Obtener los datos más frecuentes (top 5 por columna)
    try:
        frequent_data = get_frequent_data_for_report(full_table_name, date_column, sample_date, top_n=3)
    except Exception as e:
        frequent_data = None
        st.sidebar.error(f"Error al obtener los datos más frecuentes: {e}")

    # Generar el informe PDF
    try:
        report_path = generate_audit_report(
            table_name=full_table_name,
            analysis_date=format_date(sample_date),
            snowflake_exists=snowflake_exists,
            redshift_exists=redshift_exists,
            snowflake_total=snowflake_total,
            redshift_total=redshift_total,
            snowflake_dates_df=snowflake_dates_df,
            redshift_dates_df=redshift_dates_df,
            columns_snowflake_df=columns_snowflake_df,
            columns_redshift_df=columns_redshift_df,
            frequent_data=frequent_data  # Pasar los datos frecuentes
        )
        st.sidebar.success("El informe se ha generado exitosamente.")

        # Proporcionar enlace de descarga
        with open(report_path, "rb") as file:
            btn = st.sidebar.download_button(
                label="Descargar Informe PDF",
                data=file,
                file_name=os.path.basename(report_path),  # Solo el nombre del archivo
                mime="application/pdf"
            )
    except Exception as e:
        st.sidebar.error(f"Error al generar el informe: {e}")