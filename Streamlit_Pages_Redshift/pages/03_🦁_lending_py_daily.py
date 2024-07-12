import psycopg2
import pandas as pd
import os
import streamlit as st
import matplotlib.pyplot as plt
from dotenv import load_dotenv

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

# Add a filter for the user to select a month
min_date = df['localcreationdate'].min()
max_date = df['localcreationdate'].max()

# Sidebar for date input
selected_month = st.sidebar.date_input("Select month", value=max_date, min_value=min_date, max_value=max_date)
start_date = pd.Timestamp(selected_month.year, selected_month.month, 1)
end_date = (start_date + pd.DateOffset(months=1)) - pd.DateOffset(days=1)

# Filter the data for the selected month
df = df[(df['localcreationdate'] >= start_date) & (df['localcreationdate'] <= end_date)]

# Process the data for visualization
df = df.groupby(['localcreationdate', 'recu']).agg({'q_loans': 'sum'}).reset_index()

# Pivot the data to get first loans and recurrent loans in separate columns
df_pivot = df.pivot(index='localcreationdate', columns='recu', values='q_loans').fillna(0)
df_pivot.columns = ['First Loan', 'Recurrent Loan']

# Calculate total loans by day
df_pivot['Total Loan'] = df_pivot['First Loan'] + df_pivot['Recurrent Loan']

# Create a horizontal bar chart
fig, ax = plt.subplots(figsize=(10, 8))

# Plot the first loans
bars1 = ax.barh(df_pivot.index.strftime('%Y-%m-%d'), df_pivot['First Loan'], color='blue', label='First Loan')

# Plot the recurrent loans
bars2 = ax.barh(df_pivot.index.strftime('%Y-%m-%d'), df_pivot['Recurrent Loan'], left=df_pivot['First Loan'], color='orange', label='Recurrent Loan')

# Add labels and title
ax.set_xlabel('Number of Loans')
ax.set_ylabel('Date')
ax.set_title('Loans by Day')
ax.legend()

# Add total loan labels to the bars
for i, total in enumerate(df_pivot['Total Loan']):
    ax.text(total, i, f'{int(total)}', va='center', ha='left', color='black', fontsize=10)

# Display the plot in Streamlit
st.title("Daily Report: Loans by day PY")
st.pyplot(fig)

# Add a table showing the numbers by date and totals, sorted in descending order
df_table = df_pivot.reset_index()
df_table['localcreationdate'] = df_table['localcreationdate'].dt.strftime('%Y-%m-%d')
df_table = df_table.sort_values(by='localcreationdate', ascending=False)
st.write("Detailed Table of Loans by Date")
st.table(df_table)

# Add a summary table showing the overall totals
df_summary = df_pivot[['First Loan', 'Recurrent Loan', 'Total Loan']].sum().reset_index()
df_summary.columns = ['Loan Type', 'Total Loans']

st.write("Summary Table of Loans")
st.table(df_summary)