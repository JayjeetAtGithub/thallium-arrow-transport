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


# 10% total_amount >= 63, 350000
# 1%  total_amount >= 104, 35000
# 0.1% total_amount >= 178, 3500
# 0.01% total_amount >= 335, 350
# 0.001% total_amount >= 530, 35
# 0.0001% total_amount >= 1030, 3
