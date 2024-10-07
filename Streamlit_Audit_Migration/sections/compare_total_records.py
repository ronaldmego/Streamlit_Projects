# sections/compare_total_records.py

import streamlit as st
from scripts.snowflake_connection import get_total_record_count_snowflake
from scripts.redshift_connection import get_total_record_count_redshift
from utils.helpers import handle_error

def compare_total_records(full_table_name):
    st.write(f"## Comparando la cantidad total de registros en **{full_table_name}**")
    
    # Obtener el conteo total de registros en Snowflake
    st.subheader("Cantidad Total de Registros en Snowflake")
    try:
        snowflake_total, error_snowflake_total = get_total_record_count_snowflake(full_table_name)
        if snowflake_total is not None:
            st.write(f"**Total de registros en Snowflake:** {snowflake_total}")
        else:
            handle_error(error_snowflake_total, "Snowflake")
    except Exception as e:
        st.error(f"Error inesperado al obtener el conteo total en Snowflake: {e}")
    
    # Obtener el conteo total de registros en Redshift
    st.subheader("Cantidad Total de Registros en Redshift")
    try:
        redshift_total, error_redshift_total = get_total_record_count_redshift(full_table_name)
        if redshift_total is not None:
            st.write(f"**Total de registros en Redshift:** {redshift_total}")
        else:
            handle_error(error_redshift_total, "Redshift")
    except Exception as e:
        st.error(f"Error inesperado al obtener el conteo total en Redshift: {e}")
    
    # Comparar los totales
    st.markdown("---")
    st.header("Resultado de la Comparación de Totales")
    
    if (snowflake_total is not None) and (redshift_total is not None):
        st.write(f"- **Total en Snowflake:** {snowflake_total}")
        st.write(f"- **Total en Redshift:** {redshift_total}")
        
        if snowflake_total == redshift_total:
            st.success("La cantidad total de registros en ambas bases de datos es **idéntica**.")
        else:
            st.warning("**Diferencia en la cantidad total de registros** entre Snowflake y Redshift.")
    else:
        st.error("No se pudieron comparar los totales debido a errores anteriores.")
