# sections/top_frequent_data.py

import streamlit as st
from scripts.snowflake_connection import get_top_frequent_data_snowflake
from scripts.redshift_connection import get_top_frequent_data_redshift
from utils.helpers import display_dataframe, handle_error, run_parallel_queries, format_date
import pandas as pd

def top_frequent_data(full_table_name, date_column, sample_date, top_n=5, max_columns=None):
    st.write(f"## Análisis de los {top_n} Datos Más Frecuentes por Columna en **{full_table_name}** para la Fecha {sample_date.strftime('%Y-%m-%d')}")
    
    # Obtener la lista de columnas
    from scripts.redshift_connection import get_columns_redshift  # Usamos Redshift para obtener columnas
    try:
        columns_df, error_columns = get_columns_redshift(full_table_name)
        if columns_df is not None and not columns_df.empty:
            columns = columns_df['column_name'].tolist()
            if max_columns is not None:
                columns = columns[:max_columns]
                st.write(f"**Columnas Analizadas (Primeras {max_columns}):** {', '.join(columns)}")
            else:
                st.write(f"**Columnas Analizadas (Total {len(columns)}):** {', '.join(columns)}")
        elif columns_df is not None and columns_df.empty:
            st.info("No se encontraron columnas en Redshift para la tabla especificada.")
            return
        else:
            handle_error(error_columns, "Redshift")
            return
    except Exception as e:
        st.error(f"Error inesperado al obtener columnas en Redshift: {e}")
        return
    
    discrepant_columns = []

    for column in columns:
        st.markdown(f"### Columna: **{column}**")
        
        # Definir funciones internas para las consultas
        def execute_snowflake():
            return get_top_frequent_data_snowflake(full_table_name, date_column, sample_date, column, top_n)
        
        def execute_redshift():
            return get_top_frequent_data_redshift(full_table_name, date_column, sample_date, column, top_n)
        
        # Ejecutar consultas en paralelo
        with st.spinner(f'Ejecutando consultas para la columna "{column}"...'):
            results = run_parallel_queries([execute_snowflake, execute_redshift])
        
        # Desempaquetar los resultados
        df_snowflake, error_snowflake = results[0]
        df_redshift, error_redshift = results[1]
        
        # Mostrar resultados en Snowflake
        st.subheader("Top Datos en Snowflake")
        display_dataframe(df_snowflake, "Snowflake")
        if error_snowflake:
            handle_error(error_snowflake, "Snowflake")
            continue  # Saltar a la siguiente columna en caso de error
        
        # Mostrar resultados en Redshift
        st.subheader("Top Datos en Redshift")
        display_dataframe(df_redshift, "Redshift")
        if error_redshift:
            handle_error(error_redshift, "Redshift")
            continue  # Saltar a la siguiente columna en caso de error
        
        # Comparar los datos más frecuentes
        st.subheader("Comparación de Datos Más Frecuentes")
        comparison_df = compare_top_frequent(df_snowflake, df_redshift, column, top_n)
        display_dataframe(comparison_df, "Comparación")
        
        # Verificar discrepancias
        if comparison_df is not None and not comparison_df['match'].all():
            discrepant_columns.append(column)
            st.warning(f"Diferencias encontradas en la columna **{column}**.")
        elif comparison_df is not None:
            st.success(f"No se encontraron diferencias en la columna **{column}**.")
        
        st.markdown("---")
    
    # Resumen de Discrepancias
    if discrepant_columns:
        st.markdown("## Resumen de Discrepancias")
        st.warning(f"Las siguientes columnas presentaron diferencias en los datos más frecuentes: {', '.join(discrepant_columns)}")
    else:
        st.success("No se encontraron discrepancias en los datos más frecuentes de las columnas analizadas.")

def compare_top_frequent(df_snowflake, df_redshift, column, top_n):
    """
    Compara los top_n datos más frecuentes de Snowflake y Redshift para una columna específica.
    """
    if df_snowflake is None or df_redshift is None:
        return None
    
    # Depuración: Imprimir los nombres de las columnas
    st.write(f"**Columnas en Snowflake para {column}:** {df_snowflake.columns.tolist()}")
    st.write(f"**Columnas en Redshift para {column}:** {df_redshift.columns.tolist()}")
    
    # Asegurarse de que ambos DataFrames tienen las columnas 'value' y 'count'
    if not {'value', 'count'}.issubset([col.lower() for col in df_snowflake.columns]) or not {'value', 'count'}.issubset([col.lower() for col in df_redshift.columns]):
        st.error(f"Formato inesperado de datos para la columna **{column}**.")
        return None
    
    # Renombrar columnas para diferenciarlas (asegurarse de usar el nombre correcto)
    df_snowflake = df_snowflake.rename(columns=lambda x: x.lower())
    df_redshift = df_redshift.rename(columns=lambda x: x.lower())
    
    df_snowflake = df_snowflake.rename(columns={'count': 'count_snowflake'})
    df_redshift = df_redshift.rename(columns={'count': 'count_redshift'})
    
    #cambiar a string todo antes de hacer merge
    df_snowflake['value'] = df_snowflake['value'].astype(str)
    df_redshift['value'] = df_redshift['value'].astype(str)

    # Realizar una unión completa
    comparison_df = pd.merge(df_snowflake, df_redshift, on='value', how='outer').fillna(0)
    
    # Ordenar por el valor para facilitar la lectura
    comparison_df = comparison_df.sort_values(by='count_snowflake', ascending=False).reset_index(drop=True)
    
    # Añadir una columna de comparación
    comparison_df['match'] = comparison_df['count_snowflake'] == comparison_df['count_redshift']
    
    # Seleccionar solo las 5 principales (en caso de más debido a la unión)
    comparison_df = comparison_df.head(top_n)
    
    return comparison_df

def get_frequent_data_for_report(full_table_name, date_column, sample_date, top_n=5, max_columns=None):
    """
    Obtiene los datos más frecuentes por columna para incluir en el informe.
    
    Args:
        full_table_name (str): Nombre completo de la tabla.
        date_column (str): Nombre de la columna de fecha.
        sample_date (datetime.date): Fecha de muestreo.
        top_n (int): Número de datos más frecuentes.
        max_columns (int): Número máximo de columnas a analizar.
    
    Returns:
        dict: Diccionario con datos frecuentes por columna.
    """
    from scripts.redshift_connection import get_columns_redshift  # Usar Redshift para obtener columnas
    import pandas as pd
    # No es necesario importar compare_top_frequent aquí, ya está en top_frequent_data.py
    
    # Obtener las columnas
    columns_df, error_columns = get_columns_redshift(full_table_name)
    if columns_df is not None and not columns_df.empty:
        columns = columns_df['column_name'].tolist()[:max_columns]
    elif columns_df is not None and columns_df.empty:
        st.sidebar.info("No se encontraron columnas en Redshift para la tabla especificada.")
        return {}
    else:
        handle_error(error_columns, "Redshift")
        return {}
    
    frequent_data = {}

    for column in columns:
        # Definir funciones internas para las consultas
        def execute_snowflake():
            return get_top_frequent_data_snowflake(full_table_name, date_column, sample_date, column, top_n)
        
        def execute_redshift():
            return get_top_frequent_data_redshift(full_table_name, date_column, sample_date, column, top_n)
        
        # Ejecutar consultas en paralelo
        results = run_parallel_queries([execute_snowflake, execute_redshift])
        
        # Desempaquetar los resultados
        df_snowflake, error_snowflake = results[0]
        df_redshift, error_redshift = results[1]
        
        if error_snowflake or error_redshift:
            frequent_data[column] = {
                'comparison': None,
                'discrepancy': False
            }
            continue
        
        # Comparar los datos más frecuentes
        comparison_df = compare_top_frequent(df_snowflake, df_redshift, column, top_n)
        
        # Verificar discrepancias
        discrepancy = not comparison_df['match'].all() if comparison_df is not None else False
        
        frequent_data[column] = {
            'comparison': comparison_df,
            'discrepancy': discrepancy
        }
    
    return frequent_data