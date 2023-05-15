#include <duckdb.hpp>
#include <duckdb/common/arrow/arrow_converter.hpp>

#include <arrow/api.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>


class DuckDBRecordBatchReader : public arrow::RecordBatchReader {
    public:
        DuckDBRecordBatchReader(std::shared_ptr<duckdb::QueryResult> result) : result(result) {
            auto timezone_config = duckdb::QueryResult::GetConfigTimezone(*result);
            ArrowSchema arrow_schema;
            duckdb::ArrowConverter::ToArrowSchema(&arrow_schema, result->types, result->names, timezone_config);
            imported_schema = arrow::ImportSchema(&arrow_schema).ValueOrDie();
            chunk = result->Fetch();
        }

        arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
            if (chunk == nullptr) {
                *out = nullptr;
                return arrow::Status::OK();
            }

            ArrowArray arrow_array;
            duckdb::ArrowConverter::ToArrowArray(*chunk, &arrow_array);
            *out = arrow::ImportRecordBatch(&arrow_array, imported_schema).ValueOrDie();
            chunk = result->Fetch();
            return arrow::Status::OK();
        }

        std::shared_ptr<arrow::Schema> schema() const override {
            return imported_schema;
        }

    protected:
        std::shared_ptr<duckdb::QueryResult> result;
        std::unique_ptr<duckdb::DataChunk> chunk;
        std::shared_ptr<arrow::Schema> imported_schema;
};


class DuckDBEngine {
    public:
        DuckDBEngine() {
            db = std::make_shared<duckdb::DuckDB>(nullptr);
            con = std::make_shared<duckdb::Connection>(*db);
        }

        void Create(const std::string &path) {
            con->Query("INSTALL parquet; LOAD parquet;");
            std::string table_create_query = "CREATE TABLE dataset AS SELECT * FROM read_parquet('" + path + "');";
            con->Query(table_create_query);    
        }

        std::shared_ptr<arrow::RecordBatchReader> Execute(const std::string &query) {
            auto statement = con->Prepare(query);
            auto result = statement->Execute();
            return std::make_shared<DuckDBRecordBatchReader>(std::move(result));
        }

    private:
        std::shared_ptr<duckdb::DuckDB> db;
        std::shared_ptr<duckdb::Connection> con;
};
