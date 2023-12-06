import sys
import pyarrow.parquet as pq


def describe_table(table):
    print("Table size in Bytes: ", table.nbytes)
    table_size_in_mb = table.nbytes / 1024 / 1024
    print("Table size in MB: ", table_size_in_mb, " MB")
    print("Number of rows: ", table.num_rows)
    print("Number of columns: ", table.num_columns)
    print("Column names: ", table.column_names)
    print("Total dataset size for 105 files: ", table_size_in_mb * 105, " MB")
    print("Total number of rows: ", table.num_rows * 105)


if __name__ == "__main__":
    filename = str(sys.argv[1])

    filters_expr = [
        ('total_amount', '>=', 63),
        ('total_amount', '>=', 104),
        ('total_amount', '>=', 178),
        ('total_amount', '>=', 335),
        ('total_amount', '>=', 530),
        ('total_amount', '>=', 1030),
    ]


    table = pq.read_table(filename)
    describe_table(table)

    for expr in filters_expr:
        print("Filter: ", expr)
        filtered_table = pq.read_table(filename, filters=[expr])
        describe_table(filtered_table)

# 10% total_amount >= 63, 350000
# 1%  total_amount >= 104, 35000
# 0.1% total_amount >= 178, 3500
# 0.01% total_amount >= 335, 350
# 0.001% total_amount >= 530, 35
# 0.0001% total_amount >= 1030, 3
