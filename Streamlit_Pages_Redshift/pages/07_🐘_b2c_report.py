#run public: "streamlit run 04_Streamlit_b2c_report.py"
#run private ips: "streamlit run 04_Streamlit_b2c_report.py --server.ipWhitelist 192.168.1.100,192.168.1.101"

# Import necessary libraries
import streamlit as st
from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
import pyminizip
import tempfile

# Load environment variables
load_dotenv()

# Define the function to query Redshift
def query_redshift(query, start_date, end_date):
    user = os.getenv('USERNAMERS')
    password = os.getenv('PASSWORD')
    host = os.getenv('HOST')
    port = os.getenv('PORT')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )

    cs = conn.cursor()

    # Format the query with the dates
    formatted_query = query.format(start_date=start_date, end_date=end_date)

    cs.execute(formatted_query)
    df = pd.DataFrame(cs.fetchall(), columns=[desc[0] for desc in cs.description])
    cs.close()
    conn.close()

    return df

# Function to mask PII columns
def mask_pii(df, columns_to_mask):
    df_masked = df.copy()
    for column in columns_to_mask:
        if column in df_masked.columns:
            df_masked[column] = 'PII'
    return df_masked

# SQL query
query = """
SELECT *
FROM information_delivery_prod.mfs_lending.lending_mmb_py_b2c_report_pii_vw
WHERE creation_date BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'
"""

# Streamlit app
def main():
    st.title("Lending - PY | B2C | Disbursements - Collections - Adjustment | Report")

    # Date pickers for start and end dates
    start_date = st.date_input("Start date")
    end_date = st.date_input("End date")

    # Optional date pickers for disbursement_date
    disbursement_start_date = st.date_input("Disbursement start date (optional)", value=None)
    disbursement_end_date = st.date_input("Disbursement end date (optional)", value=None)

    # Columns to mask
    columns_to_mask = ['account_holder_name', 'mobile_phone']

    # Run button to execute the query
    if st.button("Run"):
        if start_date and end_date:
            st.session_state.start_date = start_date
            st.session_state.end_date = end_date
            st.session_state.data = query_redshift(query, str(start_date), str(end_date))

            df_full = st.session_state.data

            # Convert disbursement_date to date only
            df_full['disbursement_date'] = pd.to_datetime(df_full['disbursement_date']).dt.date

            # Filter by disbursement_date if specified
            if disbursement_start_date and disbursement_end_date:
                df_full = df_full[(df_full['disbursement_date'] >= disbursement_start_date) & (df_full['disbursement_date'] <= disbursement_end_date)]

            st.session_state.filtered_data = df_full
            st.write("Data loaded successfully. You can now apply additional filters and download the data.")

    if 'filtered_data' in st.session_state:
        df_full = st.session_state.filtered_data
        df_sample = df_full.head(10)  # Get the first 10 rows for preview

        # Button to show preview
        if st.button("Show preview"):
            df_sample_masked = mask_pii(df_sample, columns_to_mask)
            st.dataframe(df_sample_masked)

        # Multiselect for account_id
        account_ids = ['All'] + df_full['account_id'].unique().tolist()
        selected_account_ids = st.multiselect("Select account_id(s)", account_ids, default='All')

        # Multiselect for type
        types = ['All'] + df_full['type'].unique().tolist()
        selected_types = st.multiselect("Select type(s)", types, default='All')

        # Filter data based on selected account_ids
        if 'All' in selected_account_ids:
            df_filtered = df_full
        else:
            df_filtered = df_full[df_full['account_id'].isin(selected_account_ids)]

        # Further filter data based on selected types
        if 'All' not in selected_types:
            df_filtered = df_filtered[df_filtered['type'].isin(selected_types)]

        # Button to download filtered data
        if st.button("Download Data"):
            # Convert DataFrame to CSV
            csv = df_filtered.to_csv(index=False).encode('utf-8')
            
            # Create a password-protected ZIP file
            zip_password = os.getenv('ZIP_PASSWORD')
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as csv_file:
                csv_file.write(csv)
                csv_file_path = csv_file.name

            zip_file_path = csv_file_path + '.zip'
            pyminizip.compress(csv_file_path, None, zip_file_path, zip_password, 0)
            
            # Read the ZIP file
            with open(zip_file_path, 'rb') as zip_file:
                zip_data = zip_file.read()

            # Offer the ZIP file for download
            st.download_button(
                label="Download data as ZIP",
                data=zip_data,
                file_name='data.zip',
                mime='application/zip',
            )

if __name__ == "__main__":
    main()