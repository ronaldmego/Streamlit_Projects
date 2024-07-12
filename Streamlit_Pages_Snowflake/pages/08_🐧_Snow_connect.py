import snowflake.connector
import pandas as pd
import os
import streamlit as st
import matplotlib.pyplot as plt
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def query_snowflake():
    user = os.getenv('USER_SNOW')
    password = os.getenv('PASSWORD_SNOW')
    account = os.getenv('ACCOUNT_SNOW')
    warehouse = os.getenv('WAREHOUSE_SNOW')
    database = os.getenv('DATABASE_SNOW')
    schema = os.getenv('SCHEMA_SNOW')

    ctx = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

    cs = ctx.cursor()

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

    cs.execute(query)
    df = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])
    cs.close()
    ctx.close()

    return df

# Call the function to get the data
df = query_snowflake()

# Debug: Print the column names and first few rows to ensure 'localcreationdate' and 'recu' are present
st.write("Column names of the DataFrame:", df.columns)
st.write("First few rows of the DataFrame:", df.head())

# Process the data for visualization
df['LOCALCREATIONDATE'] = pd.to_datetime(df['LOCALCREATIONDATE'])
df = df.groupby(['LOCALCREATIONDATE', 'RECU']).agg({'Q_LOANS': 'sum'}).reset_index()

# Separate first loans and recurrent loans
df_first = df[df['RECU'] == 0].set_index('LOCALCREATIONDATE')
df_recurrent = df[df['RECU'] == 1].set_index('LOCALCREATIONDATE')

# Create a horizontal bar chart
fig, ax = plt.subplots(figsize=(10, 8))

# Plot the first loans
ax.barh(df_first.index.strftime('%A, %B %d, %Y'), df_first['Q_LOANS'], color='blue', label='First Loan')

# Plot the recurrent loans
ax.barh(df_recurrent.index.strftime('%A, %B %d, %Y'), df_recurrent['Q_LOANS'], left=df_first['Q_LOANS'], color='orange', label='Recurrent Loan')

# Add labels and title
ax.set_xlabel('Number of Loans')
ax.set_ylabel('Date')
ax.set_title('Loans by Day')
ax.legend()

# Display the plot in Streamlit
st.title("Reporte de Préstamos desde Snowflake")
st.write("Data Team Control")
st.write("Primeras 10 filas del resultado de la consulta:")
st.dataframe(df.head(10))
st.write("Loans by Day")
st.pyplot(fig)