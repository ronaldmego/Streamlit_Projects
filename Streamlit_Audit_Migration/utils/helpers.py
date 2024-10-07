# utils/helpers.py

import streamlit as st
import pandas as pd
import concurrent.futures

def display_dataframe(df, title="DataFrame"):
    """
    Muestra un DataFrame en Streamlit con un título.
    """
    if df is not None and not df.empty:
        st.dataframe(df)
    elif df is not None and df.empty:
        st.info(f"La consulta no devolvió resultados para {title}.")
    else:
        st.error(f"Error al obtener datos para {title}.")

def handle_error(error, context=""):
    """
    Muestra un mensaje de error en Streamlit.
    """
    if error:
        st.error(f"Error en {context}: {error}")
    else:
        st.error(f"Error inesperado en {context}.")

def format_date(date):
    """
    Formatea una fecha en formato YYYY-MM-DD.
    """
    return date.strftime("%Y-%m-%d")

def run_parallel_queries(funcs):
    """
    Ejecuta múltiples funciones en paralelo y devuelve sus resultados.
    
    Args:
        funcs (list): Lista de funciones sin argumentos.
        
    Returns:
        list: Lista de resultados en el mismo orden que las funciones.
    """
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(funcs)) as executor:
        futures = [executor.submit(func) for func in funcs]
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
            except Exception as e:
                result = (None, str(e))
            results.append(result)
    return results