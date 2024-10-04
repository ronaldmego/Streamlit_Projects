import streamlit as st
import os
from scripts.snowflake_connection import (
    check_table_exists_snowflake, 
    query_snowflake_sample,
    get_total_record_count_snowflake,
    get_record_count_by_date_snowflake,
    get_columns_snowflake
)
from scripts.redshift_connection import (
    check_table_exists_redshift, 
    query_redshift_sample,
    get_total_record_count_redshift,
    get_record_count_by_date_redshift,
    get_columns_redshift
)
from scripts.report_generation import generate_audit_report
import pandas as pd

# CSS para ampliar el ancho del sidebar
st.markdown(
    """
    <style>
    /* Ampliar el ancho del sidebar */
    .css-1d391kg {  /* Clase CSS para el sidebar en Streamlit */
        width: 500px;
    }
    /* Ajustar el margen principal para acomodar el sidebar más ancho */
    .css-1y4p8pa {  /* Clase CSS para el contenido principal */
        margin-left: 500px;
    }
    </style>
    """,
    unsafe_allow_html=True
)

st.title("Auditoría de Migración de Tablas: Snowflake a Redshift")

st.sidebar.header("Configuración de Auditoría")

# Entrada para el nombre completo de la tabla
full_table_name = st.sidebar.text_input(
    "Nombre Completo de la Tabla",
    value="INFORMATION_DELIVERY_PROD.mfs_lending.info_mmb_py_loan_accounts",
    help="Ingrese el nombre de la tabla en uno de los siguientes formatos:\n- table_name\n- schema.table_name\n- database.schema.table_name"
)

# Entrada para la columna de fecha (con valor por defecto 'time_extracted')
date_column = st.sidebar.text_input(
    "Columna de Fecha",
    value="time_extracted",
    help="Ingrese el nombre de la columna que contiene la fecha de extracción."
)

# Entrada para la fecha de muestreo
sample_date = st.sidebar.date_input(
    "Fecha de Muestreo",
    value=pd.to_datetime("2024-09-30"),
    min_value=pd.to_datetime("2000-01-01"),
    max_value=pd.to_datetime("2100-12-31"),
    help="Seleccione la fecha para la cual desea realizar la muestra de datos."
)

# Botón para verificar existencia de la tabla
if st.button("Verificar Tabla"):
    if not full_table_name:
        st.error("Por favor, ingresa el nombre de una tabla.")
    else:
        st.write(f"## Verificando la tabla **{full_table_name}** en Snowflake y Redshift...")
        
        # Verificar en Snowflake
        try:
            snowflake_exists, snowflake_error = check_table_exists_snowflake(full_table_name)
            if snowflake_exists:
                st.success(f"La tabla **{full_table_name}** existe en **Snowflake**.")
            else:
                if snowflake_error:
                    st.error(f"Error al verificar en Snowflake: {snowflake_error}")
                else:
                    st.error(f"La tabla **{full_table_name}** NO existe en **Snowflake**.")
        except Exception as e:
            st.error(f"Error inesperado al conectar con Snowflake: {e}")
        
        # Verificar en Redshift
        try:
            redshift_exists, redshift_error = check_table_exists_redshift(full_table_name)
            if redshift_exists:
                st.success(f"La tabla **{full_table_name}** existe en **Redshift**.")
            else:
                if redshift_error:
                    st.error(f"Error al verificar en Redshift: {redshift_error}")
                else:
                    st.error(f"La tabla **{full_table_name}** NO existe en **Redshift**.")
        except Exception as e:
            st.error(f"Error inesperado al conectar con Redshift: {e}")
        
        # Comparar existencia
        if snowflake_exists and redshift_exists:
            st.balloons()
            st.success("¡La tabla existe en ambos ambientes!")
        elif not snowflake_exists and not redshift_exists:
            st.warning("La tabla no existe en ninguno de los ambientes.")
        else:
            st.warning("La tabla existe en uno de los ambientes, pero no en el otro. Revisa la migración.")

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
        date_str = sample_date.strftime("%Y-%m-%d")
        st.write(f"## Ejecutando `SELECT * WHERE DATE({date_column}) = '{date_str}' LIMIT 10` en la tabla **{full_table_name}**")
        
        # Consulta en Snowflake
        st.subheader("Resultados en Snowflake")
        try:
            df_snowflake, error_snowflake = query_snowflake_sample(
                table_name=full_table_name, 
                date_value=date_str, 
                date_column=date_column,
                limit=10
            )
            if df_snowflake is not None and not df_snowflake.empty:
                st.dataframe(df_snowflake)
            elif df_snowflake is not None and df_snowflake.empty:
                st.info("La consulta en Snowflake no devolvió resultados.")
            else:
                st.error(f"Error al ejecutar la consulta en Snowflake: {error_snowflake}")
        except Exception as e:
            st.error(f"Error inesperado al consultar Snowflake: {e}")
        
        # Consulta en Redshift
        st.subheader("Resultados en Redshift")
        try:
            df_redshift, error_redshift = query_redshift_sample(
                table_name=full_table_name, 
                date_value=date_str, 
                date_column=date_column,
                limit=10
            )
            if df_redshift is not None and not df_redshift.empty:
                st.dataframe(df_redshift)
            elif df_redshift is not None and df_redshift.empty:
                st.info("La consulta en Redshift no devolvió resultados.")
            else:
                st.error(f"Error al ejecutar la consulta en Redshift: {error_redshift}")
        except Exception as e:
            st.error(f"Error inesperado al consultar Redshift: {e}")

# Separador
st.markdown("---")

# Sección para comparación de la cantidad total de registros
st.header("Comparación de la Cantidad Total de Registros")

if st.button("Comparar Cantidad Total de Registros"):
    if not full_table_name:
        st.error("Por favor, ingresa el nombre de una tabla para comparar la cantidad total de registros.")
    else:
        st.write(f"## Comparando la cantidad total de registros en **{full_table_name}**")
        
        # Obtener el conteo total de registros en Snowflake
        st.subheader("Cantidad Total de Registros en Snowflake")
        try:
            snowflake_total, error_snowflake_total = get_total_record_count_snowflake(full_table_name)
            if snowflake_total is not None:
                st.write(f"**Total de registros en Snowflake:** {snowflake_total}")
            else:
                st.error(f"Error al obtener el conteo total en Snowflake: {error_snowflake_total}")
        except Exception as e:
            st.error(f"Error inesperado al obtener el conteo total en Snowflake: {e}")
        
        # Obtener el conteo total de registros en Redshift
        st.subheader("Cantidad Total de Registros en Redshift")
        try:
            redshift_total, error_redshift_total = get_total_record_count_redshift(full_table_name)
            if redshift_total is not None:
                st.write(f"**Total de registros en Redshift:** {redshift_total}")
            else:
                st.error(f"Error al obtener el conteo total en Redshift: {error_redshift_total}")
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
        date_str = sample_date.strftime("%Y-%m-%d")
        st.write(f"## Comparando registros por fecha para los últimos 5 días desde **{date_str}**")
        
        # Obtener el conteo de registros agrupados por fecha en Snowflake
        st.subheader("Registros por Fecha en Snowflake")
        try:
            df_snowflake_dates, error_snowflake_dates = get_record_count_by_date_snowflake(
                table_name=full_table_name,
                date_column=date_column,
                days=5
            )
            if df_snowflake_dates is not None and not df_snowflake_dates.empty:
                st.dataframe(df_snowflake_dates)
            elif df_snowflake_dates is not None and df_snowflake_dates.empty:
                st.info("La consulta en Snowflake no devolvió resultados.")
            else:
                st.error(f"Error al ejecutar la consulta en Snowflake: {error_snowflake_dates}")
        except Exception as e:
            st.error(f"Error inesperado al consultar Snowflake: {e}")
        
        # Obtener el conteo de registros agrupados por fecha en Redshift
        st.subheader("Registros por Fecha en Redshift")
        try:
            df_redshift_dates, error_redshift_dates = get_record_count_by_date_redshift(
                table_name=full_table_name,
                date_column=date_column,
                days=5
            )
            if df_redshift_dates is not None and not df_redshift_dates.empty:
                st.dataframe(df_redshift_dates)
            elif df_redshift_dates is not None and df_redshift_dates.empty:
                st.info("La consulta en Redshift no devolvió resultados.")
            else:
                st.error(f"Error al ejecutar la consulta en Redshift: {error_redshift_dates}")
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
            comparison_df['match'] = comparison_df['record_count_snowflake'] == comparison_df['record_count_redshift']
            
            if comparison_df['match'].all():
                st.success("Los conteos de registros por fecha coinciden en ambas bases de datos.")
            else:
                st.warning("Existen discrepancias en los conteos de registros por fecha entre Snowflake y Redshift.")
        else:
            st.error("No se pudieron comparar los registros por fecha debido a errores anteriores.")

# Separador
st.markdown("---")

# Nueva Sección: Comparación de Columnas
st.header("Comparación de Columnas entre Snowflake y Redshift")

if st.button("Comparar Columnas"):
    if not full_table_name:
        st.error("Por favor, ingresa el nombre de una tabla para comparar las columnas.")
    else:
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
                st.error(f"Error al obtener las columnas en Snowflake: {error_snowflake_columns}")
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
                st.error(f"Error al obtener las columnas en Redshift: {error_redshift_columns}")
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

# Separador
st.markdown("---")

# Nueva Sección: Generación de Informe
st.sidebar.header("Generación de Informe de Auditoría")

if st.sidebar.button("Generar Informe"):
    if not full_table_name:
        st.sidebar.error("Por favor, ingresa el nombre de una tabla para generar el informe.")
    else:
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
                days=5
            )
        except Exception as e:
            snowflake_dates_df, error_snowflake_dates = None, str(e)

        try:
            redshift_dates_df, error_redshift_dates = get_record_count_by_date_redshift(
                table_name=full_table_name,
                date_column=date_column,
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

        # Generar el informe PDF
        try:
            report_path = generate_audit_report(
                table_name=full_table_name,
                analysis_date=sample_date.strftime("%Y-%m-%d"),
                snowflake_exists=snowflake_exists,
                redshift_exists=redshift_exists,
                snowflake_total=snowflake_total,
                redshift_total=redshift_total,
                snowflake_dates_df=snowflake_dates_df,
                redshift_dates_df=redshift_dates_df,
                columns_snowflake_df=columns_snowflake_df,
                columns_redshift_df=columns_redshift_df
            )
            st.sidebar.success("El informe se ha generado exitosamente.")

            # Proporcionar enlace de descarga
            with open(report_path, "rb") as file:
                btn = st.sidebar.download_button(
                    label="Descargar Informe PDF",
                    data=file,
                    #file_name=report_path,
                    file_name=os.path.basename(report_path),  # Solo el nombre del archivo
                    mime="application/pdf"
                )
        except Exception as e:
            st.sidebar.error(f"Error al generar el informe: {e}")
