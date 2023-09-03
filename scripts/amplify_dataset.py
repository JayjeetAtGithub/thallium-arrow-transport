import pyarrow.parquet as pq
import pandas as pd

if __name__ == "__main__":
    df = pq.read_table("nyc.parquet").to_pandas()
    for i in range(5):
        print("Appending ", i)
        df = df.append(df)
    df.to_parquet("nyc.parquet", compression=None)
