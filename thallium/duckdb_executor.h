#include <duckdb.hpp>
#include <duckdb/common/arrow/arrow_converter.hpp>

#include <arrow/api.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>


void ExecuteDuckDB() {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    con.Query("INSTALL parquet; LOAD parquet;");

    auto statement = con.Prepare("SELECT * FROM read_parquet('16MB.uncompressed.parquet')");
    auto res = statement->Execute();

    ArrowArray res_arr;
    ArrowSchema res_schema;
    duckdb::ArrowConverter::ToArrowSchema(&res_schema, res->types, res->names);

    res->Fetch()->ToArrowArray(&res_arr);
    auto result = arrow::ImportRecordBatch(&res_arr, &res_schema).ValueOrDie();
}
