# sections/sample_query.py

import streamlit as st
from scripts.snowflake_connection import query_snowflake_sample
from scripts.redshift_connection import query_redshift_sample
from utils.helpers import display_dataframe, handle_error, format_date, run_parallel_queries

def run_sample_query(full_table_name, date_column, sample_date):
    st.write(f"## Ejecutando `SELECT * WHERE DATE({date_column}) = '{format_date(sample_date)}' LIMIT 10` en la tabla **{full_table_name}**")
    date_str = format_date(sample_date)
    
    # Definir funciones internas para las consultas
    def execute_snowflake():
        return query_snowflake_sample(
            table_name=full_table_name, 
            date_value=date_str, 
            date_column=date_column,
            limit=10
        )
    
    def execute_redshift():
        return query_redshift_sample(
            table_name=full_table_name, 
            date_value=date_str, 
            date_column=date_column,
            limit=10
        )
    
    # Ejecutar consultas en paralelo utilizando helpers
    with st.spinner('Ejecutando consultas en Snowflake y Redshift...'):
        results = run_parallel_queries([execute_snowflake, execute_redshift])
    
    # Desempaquetar los resultados
    df_snowflake, error_snowflake = results[0]
    df_redshift, error_redshift = results[1]
    
    # Mostrar resultados en Snowflake
    st.subheader("Resultados en Snowflake")
    display_dataframe(df_snowflake, "Snowflake")
    if error_snowflake:
        handle_error(error_snowflake, "Snowflake")
    
    # Mostrar resultados en Redshift
    st.subheader("Resultados en Redshift")
    display_dataframe(df_redshift, "Redshift")
    if error_redshift:
        handle_error(error_redshift, "Redshift")