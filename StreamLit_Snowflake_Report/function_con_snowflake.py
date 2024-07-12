import snowflake.connector
import pandas as pd
import os

def query_snowflake(start_date, end_date):
    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    account = os.getenv('ACCOUNT')
    warehouse = os.getenv('WAREHOUSE')
    database = os.getenv('DATABASE')
    schema = os.getenv('SCHEMA')

    ctx = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

    cs = ctx.cursor()

    # Read the SQL query from the file
    with open('query.sql', 'r') as file:
        query = file.read()

    # Format the query with the dates
    formatted_query = query.format(start_date=start_date, end_date=end_date)

    cs.execute(formatted_query)
    df = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])
    cs.close()
    ctx.close()

    return df
