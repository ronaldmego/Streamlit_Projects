# sections/compare_columns.py

import streamlit as st
import pandas as pd
from scripts.snowflake_connection import get_columns_snowflake
from scripts.redshift_connection import get_columns_redshift
from utils.helpers import handle_error

def compare_columns(full_table_name):
    st.write(f"## Comparando las columnas de **{full_table_name}** en Snowflake y Redshift...")
    
    # Obtener columnas en Snowflake
    st.subheader("Estructura de Columnas en Snowflake")
    try:
        df_snowflake_columns, error_snowflake_columns = get_columns_snowflake(full_table_name)
        if df_snowflake_columns is not None and not df_snowflake_columns.empty:
            st.dataframe(df_snowflake_columns)
        elif df_snowflake_columns is not None and df_snowflake_columns.empty:
            st.info("No se encontraron columnas en Snowflake.")
        else:
            handle_error(error_snowflake_columns, "Snowflake")
    except Exception as e:
        st.error(f"Error inesperado al obtener columnas en Snowflake: {e}")
    
    # Obtener columnas en Redshift
    st.subheader("Estructura de Columnas en Redshift")
    try:
        df_redshift_columns, error_redshift_columns = get_columns_redshift(full_table_name)
        if df_redshift_columns is not None and not df_redshift_columns.empty:
            st.dataframe(df_redshift_columns)
        elif df_redshift_columns is not None and df_redshift_columns.empty:
            st.info("No se encontraron columnas en Redshift.")
        else:
            handle_error(error_redshift_columns, "Redshift")
    except Exception as e:
        st.error(f"Error inesperado al obtener columnas en Redshift: {e}")
    
    # Comparar las columnas
    st.markdown("---")
    st.header("Resultado de la Comparación de Columnas")
    
    if (df_snowflake_columns is not None) and (df_redshift_columns is not None):
        # Convertir los nombres de columnas a mayúsculas para una comparación insensible a mayúsculas
        snowflake_columns = set(df_snowflake_columns['column_name'].str.upper())
        redshift_columns = set(df_redshift_columns['column_name'].str.upper())
        
        # Columnas en Snowflake pero no en Redshift
        columns_only_in_snowflake = snowflake_columns - redshift_columns
        # Columnas en Redshift pero no en Snowflake
        columns_only_in_redshift = redshift_columns - snowflake_columns
        
        # Comparar tipos de datos para columnas comunes
        type_mismatches = []
        for col in snowflake_columns & redshift_columns:
            snowflake_type = df_snowflake_columns[df_snowflake_columns['column_name'].str.upper() == col]['data_type'].values[0].lower()
            redshift_type = df_redshift_columns[df_redshift_columns['column_name'].str.upper() == col]['data_type'].values[0].lower()
            if snowflake_type != redshift_type:
                type_mismatches.append((col, snowflake_type, redshift_type))
        
        # Mostrar diferencias
        if columns_only_in_snowflake:
            st.warning("**Columnas presentes en Snowflake pero no en Redshift:**")
            st.write(", ".join(sorted(columns_only_in_snowflake)))
        else:
            st.success("No hay columnas exclusivas en Snowflake.")
        
        if columns_only_in_redshift:
            st.warning("**Columnas presentes en Redshift pero no en Snowflake:**")
            st.write(", ".join(sorted(columns_only_in_redshift)))
        else:
            st.success("No hay columnas exclusivas en Redshift.")
        
        if type_mismatches:
            st.warning("**Columnas con tipos de datos diferentes:**")
            mismatch_df = pd.DataFrame(type_mismatches, columns=['Columna', 'Tipo en Snowflake', 'Tipo en Redshift'])
            st.dataframe(mismatch_df)
        else:
            st.success("No hay discrepancias en los tipos de datos de las columnas comunes.")
        
        if not columns_only_in_snowflake and not columns_only_in_redshift and not type_mismatches:
            st.success("Las estructuras de las columnas coinciden perfectamente entre Snowflake y Redshift.")
        else:
            st.info("Se han encontrado diferencias en las estructuras de las columnas entre Snowflake y Redshift.")
    else:
        st.error("No se pudieron comparar las columnas debido a errores anteriores.")
