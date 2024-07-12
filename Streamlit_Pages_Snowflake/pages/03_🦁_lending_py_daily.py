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

# Convert LOCALCREATIONDATE to datetime
df['LOCALCREATIONDATE'] = pd.to_datetime(df['LOCALCREATIONDATE'])

# Add a filter for the user to select a month
min_date = df['LOCALCREATIONDATE'].min()
max_date = df['LOCALCREATIONDATE'].max()

# Sidebar for date input
selected_month = st.sidebar.date_input("Select month", value=max_date, min_value=min_date, max_value=max_date)
start_date = pd.Timestamp(selected_month.year, selected_month.month, 1)
end_date = (start_date + pd.DateOffset(months=1)) - pd.DateOffset(days=1)

# Filter the data for the selected month
df = df[(df['LOCALCREATIONDATE'] >= start_date) & (df['LOCALCREATIONDATE'] <= end_date)]

# Process the data for visualization
df = df.groupby(['LOCALCREATIONDATE', 'RECU']).agg({'Q_LOANS': 'sum'}).reset_index()

# Pivot the data to get first loans and recurrent loans in separate columns
df_pivot = df.pivot(index='LOCALCREATIONDATE', columns='RECU', values='Q_LOANS').fillna(0)
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
df_table['LOCALCREATIONDATE'] = df_table['LOCALCREATIONDATE'].dt.strftime('%Y-%m-%d')
df_table = df_table.sort_values(by='LOCALCREATIONDATE', ascending=False)
st.write("Detailed Table of Loans by Date")
st.table(df_table)

# Add a summary table showing the overall totals
df_summary = df_pivot[['First Loan', 'Recurrent Loan', 'Total Loan']].sum().reset_index()
df_summary.columns = ['Loan Type', 'Total Loans']

st.write("Summary Table of Loans")
st.table(df_summary)