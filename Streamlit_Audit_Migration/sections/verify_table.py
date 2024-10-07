# sections/verify_table.py

import streamlit as st
from scripts.snowflake_connection import check_table_exists_snowflake
from scripts.redshift_connection import check_table_exists_redshift
from utils.helpers import handle_error

def verify_table(full_table_name):
    st.write(f"## Verificando la tabla **{full_table_name}** en Snowflake y Redshift...")
    
    # Verificar en Snowflake
    try:
        snowflake_exists, snowflake_error = check_table_exists_snowflake(full_table_name)
        if snowflake_exists:
            st.success(f"La tabla **{full_table_name}** existe en **Snowflake**.")
        else:
            handle_error(snowflake_error, "Snowflake") if snowflake_error else st.error(f"La tabla **{full_table_name}** NO existe en **Snowflake**.")
    except Exception as e:
        st.error(f"Error inesperado al conectar con Snowflake: {e}")
        snowflake_exists = False
    
    # Verificar en Redshift
    try:
        redshift_exists, redshift_error = check_table_exists_redshift(full_table_name)
        if redshift_exists:
            st.success(f"La tabla **{full_table_name}** existe en **Redshift**.")
        else:
            handle_error(redshift_error, "Redshift") if redshift_error else st.error(f"La tabla **{full_table_name}** NO existe en **Redshift**.")
    except Exception as e:
        st.error(f"Error inesperado al conectar con Redshift: {e}")
        redshift_exists = False
    
    # Comparar existencia
    if 'snowflake_exists' in locals() and 'redshift_exists' in locals():
        if snowflake_exists and redshift_exists:
            st.balloons()
            st.success("¡La tabla existe en ambos ambientes!")
        elif not snowflake_exists and not redshift_exists:
            st.warning("La tabla no existe en ninguno de los ambientes.")
        else:
            st.warning("La tabla existe en uno de los ambientes, pero no en el otro. Revisa la migración.")
