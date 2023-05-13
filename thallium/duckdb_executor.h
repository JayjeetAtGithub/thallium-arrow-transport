#include <duckdb.hpp>
#include <duckdb/common/arrow/arrow_converter.hpp>

#include <arrow/api.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>

class DuckDBRecordBatchReader : public arrow::RecordBatchReader {
    public:
        DuckDBRecordBatchReader(std::unique_ptr<duckdb::QueryResult> result) : result(result) {
            auto timezone_config = duckdb::QueryResult::GetConfigTimezone(*result);
            duckdb::ArrowConverter::ToArrowSchema(&arrow_schema, result->types, result->names, timezone_config);
            chunk = result->Fetch();
        }

        arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
            if (chunk == nullptr) {
                *out = nullptr;
                return arrow::Status::OK();
            }

            duckdb::ArrowConverter::ToArrowArray(*chunk, &arrow_array);
            *out = arrow::ImportRecordBatch(&arrow_array, &arrow_schema).ValueOrDie();
            chunk = result->Fetch();
            return arrow::Status::OK();
        }

        std::shared_ptr<arrow::Schema> schema() override {
            return arrow::ImportSchema(&arrow_schema).ValueOrDie();
        }

    private:
        std::shared_ptr<duckdb::QueryResult> result;
        std::unique_ptr<duckdb::DataChunk> chunk;
        ArrowArray arrow_array;
        ArrowSchema arrow_schema;
};

std::shared_ptr<DuckDBRecordBatchReader> ExecuteDuckDB(std::string &query) {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    con.Query("INSTALL parquet; LOAD parquet;");

    auto statement = con.Prepare("SELECT * FROM read_parquet('16MB.uncompressed.parquet')");
    auto result = statement->Execute();
    return std::make_shared<DuckDBRecordBatchReader>(result);
}