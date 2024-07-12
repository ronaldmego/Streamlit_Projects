import psycopg2
import pandas as pd
import os
import streamlit as st
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Configuration for Redshift connection
conn_params = {
    'host': os.getenv('HOST'),
    'port': os.getenv('PORT'),
    'dbname': os.getenv('DBNAME'),
    'user': os.getenv('USERNAMERS'),
    'password': os.getenv('PASSWORD'),
}

# Function to query Redshift
def query_redshift():
    conn = None
    try:
        conn = psycopg2.connect(**conn_params)
        print("Conexión exitosa")

        # Create a cursor
        cursor = conn.cursor()

        # Execute the query
        query = """
        SELECT 
            localcreationdate,
            loanname,
            CASE WHEN ncred > 1 THEN 1 ELSE 0 END AS recu,
            COUNT(DISTINCT idmmbloan) AS q_loans,
            COUNT(DISTINCT accountholderkey) AS q_users
        FROM INFORMATION_DELIVERY_PROD.MFS_MARKETING.FC_LENDING_LOAN
        WHERE idcountry = '1'
        GROUP BY localcreationdate, loanname, recu
        ORDER BY localcreationdate DESC
        """
        cursor.execute(query)

        # Fetch the results and convert them to a Pandas DataFrame
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=columns)

        return df

    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error

    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Conexión cerrada")

# Call the function to get the data
df = query_redshift()

# Convert localcreationdate to datetime
df['localcreationdate'] = pd.to_datetime(df['localcreationdate'])

# Determine the max date available in the data
max_date = df['localcreationdate'].max()
current_year = max_date.year

# Add a multi-select filter for years
years = st.sidebar.multiselect("Select Years", options=df['localcreationdate'].dt.year.unique(), default=[current_year])

# Filter the data by selected years
df = df[df['localcreationdate'].dt.year.isin(years)]

# Add a filter for MTD or Total
filter_type = st.sidebar.selectbox("Select Filter", ["MTD", "Total"])

# Group the data by month and filter based on MTD or Total
if filter_type == "MTD":
    df['Month'] = df['localcreationdate'].dt.to_period('M').astype(str)
    df['Day'] = df['localcreationdate'].dt.day
    mtd_data = df[df['Day'] <= max_date.day]
    df_grouped = mtd_data.groupby(['Month', 'recu']).agg({'q_loans': 'sum'}).reset_index()
else:
    df['Month'] = df['localcreationdate'].dt.to_period('M').astype(str)
    df_grouped = df.groupby(['Month', 'recu']).agg({'q_loans': 'sum'}).reset_index()

# Pivot the data to get first loans and recurrent loans in separate columns
df_pivot = df_grouped.pivot(index='Month', columns='recu', values='q_loans').fillna(0)
df_pivot.columns = ['First Loan', 'Recurrent Loan']

# Calculate total loans by month
df_pivot['Total Loan'] = df_pivot['First Loan'] + df_pivot['Recurrent Loan']

# Create a bar chart
fig, ax = plt.subplots(figsize=(10, 8))

# Plot the first loans
bars1 = ax.bar(df_pivot.index, df_pivot['First Loan'], color='blue', label='First Loan')

# Plot the recurrent loans
bars2 = ax.bar(df_pivot.index, df_pivot['Recurrent Loan'], bottom=df_pivot['First Loan'], color='orange', label='Recurrent Loan')

# Add labels and title
ax.set_xlabel('Month')
ax.set_ylabel('Number of Loans')
ax.set_title('Loans by Month')
ax.legend()

# Rotate x-axis labels to vertical
ax.set_xticklabels(df_pivot.index, rotation=90)

# Add total loan labels to the bars
for bar1, bar2, total in zip(bars1, bars2, df_pivot['Total Loan']):
    height1 = bar1.get_height()
    height2 = bar2.get_height()
    ax.text(bar1.get_x() + bar1.get_width() / 2, height1 + height2, f'{int(total)}', ha='center', va='bottom', color='black', fontsize=10)

# Display the plot in Streamlit
st.title("Monthly Report: Loans by Month PY")
st.pyplot(fig)

# Add a table showing the numbers by month and totals, sorted in descending order
df_table = df_pivot.reset_index()
df_table['Month'] = df_table['Month'].astype(str)
df_table = df_table.sort_values(by='Month', ascending=False)
st.write("Detailed Table of Loans by Month")
st.table(df_table.style.format({"First Loan": "{:.0f}", "Recurrent Loan": "{:.0f}", "Total Loan": "{:.0f}"}))

# Add a summary table showing the overall totals
df_summary = df_pivot[['First Loan', 'Recurrent Loan', 'Total Loan']].sum().reset_index()
df_summary.columns = ['Loan Type', 'Total Loans']

st.write("Summary Table of Loans")
st.table(df_summary.style.format({"Total Loans": "{:.0f}"}))