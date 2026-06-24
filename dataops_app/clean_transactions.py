import pandas as pd

df = pd.read_csv("/tmp/dirty_store_transactions.csv")

print("Before Cleaning:", len(df))

df = df.drop_duplicates()

df = df.dropna(how="all")

print("After Cleaning:", len(df))

print(df.head())