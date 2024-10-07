# scripts/report_generation.py

import os
from datetime import datetime
from fpdf import FPDF
import pandas as pd

class PDFReport(FPDF):
    def __init__(self, analysis_date, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.analysis_date = analysis_date
        self.set_auto_page_break(auto=True, margin=15)
        self.set_margins(left=10, top=10, right=10)  # Ajustar los márgenes

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

    def add_section_title(self, title):
        """Agrega un título de sección al PDF."""
        self.set_font('Arial', 'B', 12)
        self.multi_cell(0, 10, title, 0, 'L')
        self.ln(1)

    def get_num_lines(self, col_width, text):
        """
        Calcula el número de líneas que ocupará el texto en una celda de ancho dado.
        """
        effective_width = col_width - 2 * self.c_margin
        words = text.split(' ')
        lines = []
        current_line = ''
        for word in words:
            test_line = current_line + ' ' + word if current_line else word
            if self.get_string_width(test_line) <= effective_width:
                current_line = test_line
            else:
                lines.append(current_line)
                current_line = word
        if current_line:
            lines.append(current_line)
        return len(lines)

    def add_table(self, df, title, col_widths=None):
        """
        Agrega una tabla al PDF con celdas alineadas correctamente y alturas uniformes en cada fila.

        Args:
            df (pd.DataFrame): DataFrame que contiene los datos de la tabla.
            title (str): Título de la tabla.
            col_widths (list, optional): Lista de anchos de columnas. Si es None, se distribuye equitativamente.
        """
        self.add_section_title(title)
        if col_widths is None:
            # Distribuir el ancho efectivo de la página entre las columnas
            col_width = self.epw / len(df.columns)  # self.epw = effective page width
            col_widths = [col_width] * len(df.columns)

        # Encabezado de la tabla
        self.set_font('Arial', 'B', 10)
        th = self.font_size + 2  # Altura de la celda de encabezado
        for i, col in enumerate(df.columns):
            self.cell(col_widths[i], th, str(col), border=1, align='C')
        self.ln(th)

        # Datos de la tabla
        self.set_font('Arial', '', 8)
        for index, row in df.iterrows():
            # Calcular la altura máxima de la fila
            max_height = 0
            cell_texts = []
            cell_heights = []
            for i, item in enumerate(row):
                item_str = str(item) if pd.notna(item) else ''
                cell_texts.append(item_str)
                # Calcular el número de líneas que ocupará este texto
                num_lines = self.get_num_lines(col_widths[i], item_str)
                # Calcular la altura de la celda
                cell_height = (self.font_size + 2) * num_lines
                cell_heights.append(cell_height)
                if cell_height > max_height:
                    max_height = cell_height
            # Verificar si necesitamos agregar una nueva página
            if self.y + max_height > self.page_break_trigger:
                self.add_page()
                # Reimprimir el encabezado de la tabla en la nueva página
                self.set_font('Arial', 'B', 8)
                for i, col in enumerate(df.columns):
                    self.cell(col_widths[i], th, str(col), border=1, align='C')
                self.ln(th)
                self.set_font('Arial', '', 8)
            x_start = self.get_x()
            y_start = self.get_y()
            # Imprimir las celdas de la fila con alturas uniformes
            for i, item_str in enumerate(cell_texts):
                self.set_xy(x_start + sum(col_widths[:i]), y_start)
                # Dibujar la celda (rectángulo)
                self.multi_cell(col_widths[i], max_height, '', border=1, ln=0)
                # Volver al inicio de la celda para imprimir el texto
                self.set_xy(x_start + sum(col_widths[:i]), y_start)
                # Ajustar el ancho efectivo de la celda
                cell_width = col_widths[i] - 2 * self.c_margin
                # Imprimir el texto dentro de la celda
                self.multi_cell(col_widths[i], self.font_size + 2, item_str, border=0, align='L', ln=0)
            # Mover el cursor al final de la fila
            self.set_xy(x_start, y_start + max_height)
        # No agregamos espacio adicional después de la tabla
        # self.ln(5)

    def add_text(self, text):
        """Agrega un bloque de texto al PDF."""
        self.set_font('Arial', '', 12)
        self.multi_cell(0, 10, text)
        self.ln()

    def add_frequent_data_section(self, frequent_data, top_n=5):
        """
        Agrega una sección de datos más frecuentes al PDF.

        Args:
            frequent_data (dict): Diccionario con los datos más frecuentes por columna.
            top_n (int, optional): Número de top datos a mostrar.
        """
        self.add_section_title("Análisis de los Datos Más Frecuentes por Columna")
        for column, data in frequent_data.items():
            self.set_font('Arial', 'B', 12)  # Negrita para el título de la columna
            self.multi_cell(0, 10, f"Columna: {column}", ln=1)
            self.set_font('Arial', '', 12)  # Fuente normal para el contenido
            if data['comparison'] is not None:
                self.add_table(
                    data['comparison'],
                    f"Comparación de los Top {top_n} Datos para la Columna {column}"
                )
            else:
                self.add_text("No se encontraron datos para esta columna.")
        # Agregar resumen de discrepancias
        discrepant_columns = [col for col, data in frequent_data.items() if data['discrepancy']]
        if discrepant_columns:
            self.add_section_title("Resumen de Discrepancias en Datos Más Frecuentes")
            self.add_text(f"Las siguientes columnas presentaron diferencias en los datos más frecuentes: {', '.join(discrepant_columns)}")
        else:
            self.add_section_title("Resumen de Discrepancias en Datos Más Frecuentes")
            self.add_text("No se encontraron discrepancias en los datos más frecuentes de las columnas analizadas.")

def generate_audit_report(
    table_name,
    analysis_date,
    snowflake_exists,
    redshift_exists,
    snowflake_total,
    redshift_total,
    snowflake_dates_df,
    redshift_dates_df,
    columns_snowflake_df,
    columns_redshift_df,
    frequent_data=None  # Añadir parámetro para datos frecuentes
):
    """
    Genera un informe de auditoría en PDF con los datos proporcionados.

    Args:
        table_name (str): Nombre de la tabla auditada.
        analysis_date (str): Fecha de análisis.
        snowflake_exists (bool): Existencia de la tabla en Snowflake.
        redshift_exists (bool): Existencia de la tabla en Redshift.
        snowflake_total (int): Conteo total de registros en Snowflake.
        redshift_total (int): Conteo total de registros en Redshift.
        snowflake_dates_df (pd.DataFrame): Conteo de registros por fecha en Snowflake.
        redshift_dates_df (pd.DataFrame): Conteo de registros por fecha en Redshift.
        columns_snowflake_df (pd.DataFrame): Estructura de columnas en Snowflake.
        columns_redshift_df (pd.DataFrame): Estructura de columnas en Redshift.
        frequent_data (dict): Datos más frecuentes por columna.

    Returns:
        str: Ruta al archivo PDF generado.
    """
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
    if snowflake_dates_df is not None and redshift_dates_df is not None:
        # Renombrar columnas para evitar conflictos y facilitar la comparación
        comparison_dates = pd.merge(
            snowflake_dates_df.rename(columns={'count': 'count_snowflake'}),
            redshift_dates_df.rename(columns={'count': 'count_redshift'}),
            on='extraction_date',
            how='outer'
        ).fillna(0)
        comparison_dates['match'] = comparison_dates['count_snowflake'] == comparison_dates['count_redshift']
        pdf.add_table(comparison_dates, "Comparación de Registros por Fecha")
        if comparison_dates['match'].all():
            pdf.add_text("Los conteos de registros por fecha coinciden en ambas bases de datos.")
        else:
            pdf.add_text("Existen discrepancias en los conteos de registros por fecha entre Snowflake y Redshift.")
    else:
        pdf.add_text("No se pudieron comparar los registros por fecha debido a errores anteriores.")

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

    # Análisis de Datos Más Frecuentes
    if frequent_data:
        pdf.add_frequent_data_section(frequent_data, top_n=3)

    # Crear la carpeta "audit_reports" si no existe
    os.makedirs("audit_reports", exist_ok=True)

    # Definir la fecha y hora de creación para el nombre del archivo
    creation_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Generar un nombre de archivo seguro
    safe_table_name = table_name.replace('.', '_').replace(' ', '_')  # Reemplazar espacios si es necesario

    # Construir la ruta completa del archivo
    report_filename = os.path.join("audit_reports", f"audit_report_{safe_table_name}_{analysis_date}_{creation_datetime}.pdf")

    # Guardar el PDF
    pdf.output(report_filename)

    return report_filename