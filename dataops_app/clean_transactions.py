import io
import pandas as pd
import boto3
from sqlalchemy import create_engine


RUSTFS_ENDPOINT = "http://rustfs:9000"
RUSTFS_ACCESS_KEY = "dataops"
RUSTFS_SECRET_KEY = "Ankara06"

BUCKET_NAME = "dataops-bronze"
OBJECT_KEY = "raw/dirty_store_transactions.csv"

POSTGRES_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/traindb"
TARGET_TABLE = "clean_data_transactions"


def read_csv_from_rustfs():
    s3 = boto3.client(
        "s3",
        endpoint_url=RUSTFS_ENDPOINT,
        aws_access_key_id=RUSTFS_ACCESS_KEY,
        aws_secret_access_key=RUSTFS_SECRET_KEY,
    )

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY)
    csv_bytes = obj["Body"].read()

    return pd.read_csv(io.BytesIO(csv_bytes))


def clean_data(df):
    df = df.copy()

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )

    before_count = len(df)

    df = df.drop_duplicates()
    df = df.dropna(how="all")

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"nan": None, "": None})

    after_count = len(df)

    print(f"Before cleaning row count: {before_count}")
    print(f"After cleaning row count: {after_count}")
    print(f"Columns: {list(df.columns)}")

    return df


def write_to_postgres(df):
    engine = create_engine(POSTGRES_URL)

    df.to_sql(
        TARGET_TABLE,
        engine,
        schema="public",
        if_exists="replace",
        index=False,
    )

    print(f"Loaded table: public.{TARGET_TABLE}")
    print(f"Loaded row count: {len(df)}")


def main():
    print("Reading CSV from RustFS...")
    df = read_csv_from_rustfs()

    print("Cleaning data...")
    cleaned_df = clean_data(df)

    print("Writing cleaned data to PostgreSQL...")
    write_to_postgres(cleaned_df)

    print("Pipeline completed successfully.")


if __name__ == "__main__":
    main()