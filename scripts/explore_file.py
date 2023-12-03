import sys
import pyarrow.parquet as pq


if __name__ == "__main__":
    filename = str(sys.argv[1])
    table = pq.read_table(filename)
    print("Table size in Bytes: ", table.nbytes)
    print("Table size in MB: ", table.nbytes / 1024 / 1024, " MB")
    print("Number of rows: ", table.num_rows)
    print("Number of columns: ", table.num_columns)
    print("Schema: ", table.schema)
    print("Column names: ", table.column_names)
    print("Column types: ", table.column_types)
