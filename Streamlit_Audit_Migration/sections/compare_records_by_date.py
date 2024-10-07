# sections/compare_records_by_date.py

import streamlit as st
import pandas as pd
from scripts.snowflake_connection import get_record_count_by_date_snowflake
from scripts.redshift_connection import get_record_count_by_date_redshift
from utils.helpers import display_dataframe, handle_error, format_date

def compare_records_by_date(full_table_name, date_column, sample_date):
    st.write(f"## Comparando registros por fecha para los últimos 5 días")
    
    # Obtener el conteo de registros agrupados por fecha en Snowflake
    st.subheader("Registros por Fecha en Snowflake")
    try:
        df_snowflake_dates, error_snowflake_dates = get_record_count_by_date_snowflake(
            table_name=full_table_name,
            date_column=date_column,
            sample_date=sample_date,
            days=5
        )
        display_dataframe(df_snowflake_dates, "Registros por Fecha en Snowflake")
        if error_snowflake_dates:
            handle_error(error_snowflake_dates, "Snowflake")
    except Exception as e:
        st.error(f"Error inesperado al consultar Snowflake: {e}")
    
    # Obtener el conteo de registros agrupados por fecha en Redshift
    st.subheader("Registros por Fecha en Redshift")
    try:
        df_redshift_dates, error_redshift_dates = get_record_count_by_date_redshift(
            table_name=full_table_name,
            date_column=date_column,
            sample_date=sample_date,
            days=5
        )
        display_dataframe(df_redshift_dates, "Registros por Fecha en Redshift")
        if error_redshift_dates:
            handle_error(error_redshift_dates, "Redshift")
    except Exception as e:
        st.error(f"Error inesperado al consultar Redshift: {e}")
    
    # Comparar los registros por fecha
    st.markdown("---")
    st.header("Resultado de la Comparación por Fecha")
    
    if (df_snowflake_dates is not None) and (df_redshift_dates is not None):
        # Unir los DataFrames para comparar
        comparison_df = pd.merge(
            df_snowflake_dates,
            df_redshift_dates,
            on='extraction_date',
            how='outer',
            suffixes=('_snowflake', '_redshift')
        ).fillna(0)
        
        # Mostrar la comparación
        st.write("## Comparación de Registros por Fecha")
        st.dataframe(comparison_df)
        
        # Verificar si los conteos coinciden por fecha
        # Actualizar los nombres de las columnas aquí
        comparison_df['match'] = comparison_df['count_snowflake'] == comparison_df['count_redshift']
        
        if comparison_df['match'].all():
            st.success("Los conteos de registros por fecha coinciden en ambas bases de datos.")
        else:
            st.warning("Existen discrepancias en los conteos de registros por fecha entre Snowflake y Redshift.")
    else:
        st.error("No se pudieron comparar los registros por fecha debido a errores anteriores.")