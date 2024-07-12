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

# Debug: Print the column names and first few rows to ensure 'localcreationdate' and 'recu' are present
st.write("Column names of the DataFrame:", df.columns)
st.write("First few rows of the DataFrame:", df.head())

# Process the data for visualization
if 'localcreationdate' in df.columns:
    df['localcreationdate'] = pd.to_datetime(df['localcreationdate'])
    df = df.groupby(['localcreationdate', 'recu']).agg({'q_loans': 'sum'}).reset_index()

    # Separate first loans and recurrent loans
    df_first = df[df['recu'] == 0].set_index('localcreationdate')
    df_recurrent = df[df['recu'] == 1].set_index('localcreationdate')

    # Align indices by reindexing
    all_dates = df_first.index.union(df_recurrent.index)
    df_first = df_first.reindex(all_dates, fill_value=0)
    df_recurrent = df_recurrent.reindex(all_dates, fill_value=0)

    # Create a horizontal bar chart
    fig, ax = plt.subplots(figsize=(10, 8))

    # Plot the first loans
    ax.barh(df_first.index.strftime('%A, %B %d, %Y'), df_first['q_loans'], color='blue', label='First Loan')

    # Plot the recurrent loans
    ax.barh(df_recurrent.index.strftime('%A, %B %d, %Y'), df_recurrent['q_loans'], left=df_first['q_loans'], color='orange', label='Recurrent Loan')

    # Add labels and title
    ax.set_xlabel('Number of Loans')
    ax.set_ylabel('Date')
    ax.set_title('Loans by Day')
    ax.legend()

    # Display the plot in Streamlit
    st.title("Reporte de Préstamos desde Redshift")
    st.write("Data Team Control")
    st.write("Primeras 10 filas del resultado de la consulta:")
    st.dataframe(df.head(10))
    st.write("Loans by Day")
    st.pyplot(fig)
else:
    st.error("Column 'localcreationdate' not found in the DataFrame")