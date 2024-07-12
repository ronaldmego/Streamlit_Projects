#run public: "streamlit run main.py"
#run private ips: "streamlit run main.py --server.ipWhitelist 192.168.1.100,192.168.1.101"

# Import necessary libraries
import streamlit as st
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Import the query_snowflake function from the snowflake_queries.py file
from function_con_snowflake import query_snowflake

# Streamlit app
def main():
    st.title("IFRS9 - Data Download App")

    # Date pickers for start and end dates
    start_date = st.date_input("Start date")
    end_date = st.date_input("End date")

    if start_date and end_date:
        # Button to show preview
        if st.button("Show preview"):
            df_sample = query_snowflake(str(start_date), str(end_date)).head(10)  # Adjusted to get sample data
            st.dataframe(df_sample)

        # Button to download full data
        if st.button("Download Data"):
            df_full = query_snowflake(str(start_date), str(end_date))
            # Convert DataFrame to CSV for download
            csv = df_full.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download data as CSV",
                data=csv,
                file_name='data.csv',
                mime='text/csv',
            )

if __name__ == "__main__":
    main()
