# scripts/report_generation.py

from fpdf import FPDF
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

class PDFReport(FPDF):
    def __init__(self, analysis_date, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.analysis_date = analysis_date
        self.set_auto_page_break(auto=True, margin=15)

    def header(self):
        self.set_font('Arial', 'B', 15)
        self.cell(0, 10, 'Informe de Auditoría de Migración', 0, 1, 'C')
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        page_number = f'Página {self.page_no()}'
        self.cell(0, 10, page_number, 0, 0, 'C')

    def chapter_title(self, title):
        self.set_font('Arial', 'B', 12)
        self.multi_cell(0, 10, title, 0, 'L')
        self.ln(2)

    def chapter_body(self, body):
        self.set_font('Arial', '', 12)
        self.multi_cell(0, 10, body)
        self.ln()

    def add_table(self, df, title, col_widths=None):
        self.set_font('Arial', 'B', 12)
        self.multi_cell(0, 10, title, 0, 'L')
        self.ln(1)

        if col_widths is None:
            # Distribuir el ancho de la página entre las columnas
            col_width = self.epw / len(df.columns)  # self.epw = effective page width
            col_widths = [col_width] * len(df.columns)

        # Header
        self.set_font('Arial', 'B', 10)
        for i, col in enumerate(df.columns):
            self.multi_cell(col_widths[i], 10, col, border=1, align='C', ln=3, max_line_height=self.font_size)
        self.ln()

        # Data
        self.set_font('Arial', '', 10)
        for index, row in df.iterrows():
            for i, item in enumerate(row):
                # Convertir item a string y reemplazar NaN
                item_str = str(item) if pd.notna(item) else ''
                # Ajustar texto largo a múltiples líneas
                self.multi_cell(col_widths[i], 10, item_str, border=1, align='C', ln=3, max_line_height=self.font_size)
            self.ln()

    def add_text(self, text):
        self.set_font('Arial', '', 12)
        self.multi_cell(0, 10, text)
        self.ln()

def generate_audit_report(
    table_name,
    analysis_date,  # Fecha seleccionada en el sidebar
    snowflake_exists,
    redshift_exists,
    snowflake_total,
    redshift_total,
    snowflake_dates_df,
    redshift_dates_df,
    columns_snowflake_df,
    columns_redshift_df
):
    # Formatear la fecha y hora de creación del informe
    creation_datetime = datetime.now().strftime("%Y-%m-%d-%H%M%S")

    # Crear instancia de PDFReport con la fecha de análisis
    pdf = PDFReport(analysis_date=analysis_date)
    pdf.add_page()

    # Resumen Ejecutivo
    pdf.chapter_title("Resumen Ejecutivo")
    summary = f"""
    Este informe presenta una auditoría de migración de la tabla **{table_name}** desde Snowflake a Redshift.
    Fecha de Análisis: **{analysis_date}**
    Fecha y Hora de Creación del Informe: **{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}**
    
    A continuación se detallan los resultados de las verificaciones realizadas:
    """
    pdf.chapter_body(summary)

    # Verificación de Existencia de la Tabla
    pdf.chapter_title("Verificación de Existencia de la Tabla")
    existence_text = f"""
    - **Snowflake:** {"Existe" if snowflake_exists else "No existe"}.
    - **Redshift:** {"Existe" if redshift_exists else "No existe"}.
    """
    pdf.chapter_body(existence_text)

    # Comparación de la Cantidad Total de Registros
    pdf.chapter_title("Comparación de la Cantidad Total de Registros")
    totals_text = f"""
    - **Total de registros en Snowflake:** {snowflake_total if snowflake_total is not None else 'Error'}.
    - **Total de registros en Redshift:** {redshift_total if redshift_total is not None else 'Error'}.
    """
    pdf.chapter_body(totals_text)
    if snowflake_total is not None and redshift_total is not None:
        comparison = "idéntica" if snowflake_total == redshift_total else "diferente"
        pdf.add_text(f"La cantidad total de registros en ambas bases de datos es **{comparison}**.")
    else:
        pdf.add_text("No se pudo realizar la comparación de la cantidad total de registros debido a errores en los conteos.")

    # Comparación de Registros por Fecha
    pdf.chapter_title("Comparación de Registros por Fecha (Últimos 5 Días)")
    pdf.add_table(snowflake_dates_df, "Registros por Fecha en Snowflake")
    pdf.add_table(redshift_dates_df, "Registros por Fecha en Redshift")
    pdf.add_text("Se han comparado los conteos de registros agrupados por fecha para los últimos 5 días.")
    pdf.add_text("Las discrepancias se resaltan en las tablas anteriores.")

    # ***Aquí Agregaremos la Comparación y Conclusión***

    # Comparar los registros por fecha
    if (snowflake_dates_df is not None) and (redshift_dates_df is not None):
        # Unir los DataFrames para comparar
        comparison_df = pd.merge(
            snowflake_dates_df,
            redshift_dates_df,
            on='extraction_date',
            how='outer',
            suffixes=('_snowflake', '_redshift')
        ).fillna(0)

        # Añadir columna de comparación
        comparison_df['match'] = comparison_df['record_count_snowflake'] == comparison_df['record_count_redshift']

        # Añadir la tabla de comparación al informe
        pdf.add_table(comparison_df, "Resultado de la Comparación por Fecha")

        # Añadir conclusión basada en la comparación
        if comparison_df['match'].all():
            pdf.add_text("Los conteos de registros por fecha **coinciden** en ambas bases de datos.")
        else:
            pdf.add_text("Existen **discrepancias** en los conteos de registros por fecha entre Snowflake y Redshift.")
    else:
        pdf.add_text("No se pudieron comparar los registros por fecha debido a errores en las consultas.")

    # Comparación de Columnas
    pdf.chapter_title("Comparación de Columnas entre Snowflake y Redshift")
    # Columnas Exclusivas en Snowflake
    columns_only_in_snowflake = set(columns_snowflake_df['column_name'].str.upper()) - set(columns_redshift_df['column_name'].str.upper())
    columns_only_in_redshift = set(columns_redshift_df['column_name'].str.upper()) - set(columns_snowflake_df['column_name'].str.upper())

    # Columnas en ambas pero con tipos de datos diferentes
    common_columns = set(columns_snowflake_df['column_name'].str.upper()) & set(columns_redshift_df['column_name'].str.upper())
    type_mismatches = []
    for col in common_columns:
        snowflake_type = columns_snowflake_df[columns_snowflake_df['column_name'].str.upper() == col]['data_type'].values[0].lower()
        redshift_type = columns_redshift_df[columns_redshift_df['column_name'].str.upper() == col]['data_type'].values[0].lower()
        if snowflake_type != redshift_type:
            type_mismatches.append((col, snowflake_type, redshift_type))

    # Crear DataFrame de Mismatches
    if type_mismatches:
        mismatch_df = pd.DataFrame(type_mismatches, columns=['Columna', 'Tipo en Snowflake', 'Tipo en Redshift'])
    else:
        mismatch_df = pd.DataFrame(columns=['Columna', 'Tipo en Snowflake', 'Tipo en Redshift'])

    # Resumen de Comparaciones
    summary_comparisons = f"""
    - **Columnas exclusivas en Snowflake:** {', '.join(columns_only_in_snowflake) if columns_only_in_snowflake else 'Ninguna'}.
    - **Columnas exclusivas en Redshift:** {', '.join(columns_only_in_redshift) if columns_only_in_redshift else 'Ninguna'}.
    """
    pdf.chapter_body(summary_comparisons)

    # Añadir Tabla de Mismatches
    if not mismatch_df.empty:
        pdf.add_table(mismatch_df, "Columnas con Tipos de Datos Diferentes")
        pdf.add_text("Se han encontrado discrepancias en los tipos de datos de las columnas comunes.")
    else:
        pdf.add_text("No se encontraron discrepancias en los tipos de datos de las columnas comunes.")


    # Crear la carpeta "audit_reports" si no existe
    os.makedirs("audit_reports", exist_ok=True)

    # Guardar el PDF con el nombre que incluye las fechas
    safe_table_name = table_name.replace('.', '_')
    report_filename = os.path.join("audit_reports", f"audit_report_{safe_table_name}_{analysis_date}-{creation_datetime}.pdf")
    pdf.output(report_filename)
    return report_filename