import os
import sys
import tempfile
import sqlite3
import pandas as pd
from typing import Tuple
from kaggle.api.kaggle_api_extended import KaggleApi


def get_kaggle_credentials() -> Tuple[str, str]:
    """
    Retrieve Kaggle credentials from environment variables.

    :return: A tuple containing the Kaggle username and API key.
    """
    username: str = os.getenv("KAGGLE_USERNAME", "")
    key: str = os.getenv("KAGGLE_KEY", "")

    if not username or not key:
        print("Error: KAGGLE_USERNAME or KAGGLE_KEY not found in environment variables.")
        print("Please set them before running the script.")
        sys.exit(1)

    return username, key


def configure_kaggle_api(username: str, key: str) -> KaggleApi:
    """
    Configure the Kaggle API with the provided credentials.

    :param username: Kaggle username.
    :param key: Kaggle API key.
    :return: An authenticated KaggleApi instance.
    """
    os.environ["KAGGLE_USERNAME"] = username
    os.environ["KAGGLE_KEY"] = key

    api: KaggleApi = KaggleApi()
    api.authenticate()
    return api


def load_dataset_to_dataframe(api: KaggleApi, dataset_ref: str, file_name: str) -> pd.DataFrame:
    """
    Load a Kaggle dataset into a pandas DataFrame.

    :param api: Authenticated KaggleApi instance.
    :param dataset_ref: Dataset reference (e.g., 'aungpyaeap/supermarket-sales').
    :param file_name: Specific file to load from the dataset.
    :return: A pandas DataFrame containing the dataset.
    """
    # Create a temporary directory for downloading the dataset
    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"Downloading dataset '{dataset_ref}' to a temporary directory...")
        api.dataset_download_files(dataset_ref, path=tmpdir, unzip=True)
        print("Download completed.")

        # Construct the path to the specific file
        file_path: str = os.path.join(tmpdir, file_name)
        if not os.path.exists(file_path):
            print(f"Error: File '{file_name}' not found in the dataset.")
            sys.exit(1)

        # Load the file into a pandas DataFrame
        print(f"Loading '{file_name}' into a pandas DataFrame...")
        df: pd.DataFrame = pd.read_csv(file_path)
        print("DataFrame loaded successfully.")
        return df


def dimension_table_date(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Time-based attributes such as date, day, month, and year are critical for aggregating sales data over time.

    :param dataframe: A pandas DataFrame containing the raw dataset.
    :return: A pandas DataFrame containing the time based dataset.
    """
    dim_date = dataframe[['Date', 'Time']].drop_duplicates().copy()
    dim_date['date_id'] = range(1, len(dim_date) + 1)
    dim_date['day'] = pd.to_datetime(dim_date['Date']).dt.day
    dim_date['month'] = pd.to_datetime(dim_date['Date']).dt.month
    dim_date['year'] = pd.to_datetime(dim_date['Date']).dt.year
    dim_date['weekday'] = pd.to_datetime(dim_date['Date']).dt.day_name()
    dim_date['hour'] = pd.to_datetime(dim_date['Time']).dt.hour

    return dim_date

def dimension_table_product(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Information about the product category is essential for analyzing sales across different product lines.

    :param dataframe: A pandas DataFrame containing the raw dataset.
    :return: A pandas DataFrame containing the product based dataset.
    """
    dim_product = dataframe[['Product line', 'Unit price']].drop_duplicates().copy()
    dim_product = dim_product.rename(columns={
        'Product line': 'product_line',
        'Unit price': 'unit_price'
    })
    dim_product['product_id'] = range(1, len(dim_product) + 1)

    return dim_product


def fact_table_sales(dataframe: pd.DataFrame, dim_date: pd.DataFrame, dim_product: pd.DataFrame) -> pd.DataFrame:
    """
    Quantitative metrics (e.g., sales, quantity) and foreign keys linking to the dimension tables.

    :param dataframe: A pandas DataFrame containing the raw dataset.
    :param dim_date: A pandas DataFrame containing the time based dataset.
    :param dim_product: A pandas DataFrame containing the product based dataset.
    :return: A pandas DataFrame containing the product based dataset.
    """
    print(dataframe.head())
    print(dim_date.head())
    print(dim_product.head())
    dataframe = dataframe.rename(columns={
        'Invoice ID': 'invoice_id',
        'Branch': 'branch',
        'City': 'city',
        'Customer type': 'customer_type',
        'Gender': 'gender',
        'Quantity': 'quantity',
        'Total': 'total',
        'Tax 5%': 'tax_5_percent',
        'gross income': 'gross_income',
        'Rating': 'rating',
        'Product line': 'product_line',
        'Unit price': 'unit_price'
    })
    fact_sales = dataframe.merge(dim_date, on=['Date', 'Time']).merge(dim_product, on=['product_line', 'unit_price'])
    print(fact_sales.head())
    
    duplicates = fact_sales[fact_sales.duplicated(subset=['product_id'], keep=False)]
    duplicates = duplicates.sort_values(by='product_id')
    print(duplicates.head())
    fact_sales = fact_sales[['invoice_id', 'date_id', 'product_id', 'branch', 'city', 'customer_type', 'gender',
                             'quantity', 'total', 'tax_5_percent', 'gross_income', 'rating']]
    print(fact_sales.head())
    return fact_sales


def load_sqlite(dim_date_df: pd.DataFrame, dim_product_df: pd.DataFrame, fact_sales_df: pd.DataFrame) -> None:
    """
    Load data into a sqlite database

    :param dim_date_df: A pandas DataFrame containing the raw dataset.
    :param dim_product_df: A pandas DataFrame containing the time based dataset.
    :param fact_sales_df: A pandas DataFrame containing the product based dataset.
    :return: None
    """
    conn = sqlite3.connect('supermarket_sales.db')
    # Load dimensions
    dim_date_df.to_sql('dim_date', conn, if_exists='replace', index=False)
    dim_product_df.to_sql('dim_product', conn, if_exists='replace', index=False)

    # Load fact table
    fact_sales_df.to_sql('fact_sales', conn, if_exists='replace', index=False)
    print("Data successfully loaded into SQLite database.")


def query_sqlite(query: str) -> None:
    """
    Submit a query to the sqlite database

    :param query: A query string
    :return: None
    """
    conn = sqlite3.connect('supermarket_sales.db')
    report = pd.read_sql_query(query, conn)
    print(report)


def main() -> None:
    """
    Main function to execute the Kaggle dataset extraction process.
    """
    # Step 1: Get Kaggle credentials
    username, key = get_kaggle_credentials()

    # Step 2: Configure the Kaggle API
    api: KaggleApi = configure_kaggle_api(username, key)

    # Step 3: Load the dataset into a pandas DataFrame
    dataset_ref: str = "aungpyaeap/supermarket-sales"  # Kaggle dataset reference
    file_name: str = "supermarket_sales - Sheet1.csv"  # File to load from the dataset
    df: pd.DataFrame = load_dataset_to_dataframe(api, dataset_ref, file_name)

    # Step 4: Transform the data into Dimension and Fact Tables
    dim_date_df: pd.DataFrame = dimension_table_date(df)
    dim_product_df: pd.DataFrame = dimension_table_product(df)
    fact_sales_df: pd.DataFrame = fact_table_sales(df, dim_date_df, dim_product_df)

    # Step 5: Load data into sqlite database
    load_sqlite(dim_date_df, dim_product_df, fact_sales_df)

    #Calculate total sales and average gross income by product line and month.
    query: str = open('report.sql', 'r').read()

    # Step 6: Query data in sqlite database
    query_sqlite(query)


if __name__ == "__main__":
    main()