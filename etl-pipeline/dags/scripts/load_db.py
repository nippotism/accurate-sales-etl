import pandas as pd
import numpy as np
import psycopg2.extras as extras
from airflow.providers.postgres.hooks.postgres import PostgresHook

RAW_PATH = "/opt/airflow/tmp"


def load_data_to_db(data_interval_start, data_interval_end, **_):

    hook = PostgresHook(postgres_conn_id="olap-db")
    engine = hook.get_sqlalchemy_engine()


    # Load sales_invoices csv
    df = pd.read_csv(
        f"{RAW_PATH}/sales_invoices_{data_interval_start.strftime('%Y%m%d')}_{data_interval_end.strftime('%Y%m%d')}.csv"
    )

    # Parse date columns with DD/MM/YYYY format
    date_columns_invoices = ['invoice_date', 'due_date', 'ship_date']
    for col in date_columns_invoices:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce', dayfirst=True)
    
    # Parse datetime columns (already in ISO format from extraction)
    datetime_columns = ['printed_time', 'extracted_at']
    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Validate required date columns are not null
    required_dates = ['invoice_date', 'due_date', 'ship_date']
    for col in required_dates:
        if col in df.columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                print(f"WARNING: {null_count} rows have null {col}")
                # Optional: drop rows with null required dates
                # df = df.dropna(subset=[col])

    df.to_sql(
        name="sales_invoices",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

    # Load sales_invoice_details csv
    df_details = pd.read_csv(
        f"{RAW_PATH}/sales_invoice_details_{data_interval_start.strftime('%Y%m%d')}_{data_interval_end.strftime('%Y%m%d')}.csv"
    )

    date_columns_details = ['extracted_at']
    for col in date_columns_details:
        if col in df_details.columns:
            df_details[col] = pd.to_datetime(df_details[col], errors='coerce')

    df_details.to_sql(
        name="sales_invoice_details",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )