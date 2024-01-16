#include <duckdb.hpp>
#include <duckdb/common/arrow/arrow_converter.hpp>
#include <duckdb/common/arrow/arrow_wrapper.hpp>

#include <arrow/api.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>

#include "constants.h"


class DuckDBRecordBatchReader : public arrow::RecordBatchReader {
    public:
        DuckDBRecordBatchReader(std::shared_ptr<duckdb::QueryResult> result) : result_(result) {
            auto timezone_config = duckdb::QueryResult::GetConfigTimezone(*result);
            ArrowSchema arrow_schema;
            duckdb::ArrowConverter::ToArrowSchema(&arrow_schema, result->types, result->names, timezone_config);
            imported_schema = arrow::ImportSchema(&arrow_schema).ValueOrDie();
        }

        arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
            ArrowArray arrow_array;
            idx_t count = duckdb::ArrowUtil::FetchChunk(result_.get(), BATCH_SIZE, &arrow_array);
            if (count == 0) {
                *out = nullptr;
                return arrow::Status::OK();
            }

            *out = arrow::ImportRecordBatch(&arrow_array, imported_schema).ValueOrDie();
            return arrow::Status::OK();
        }

        std::shared_ptr<arrow::Schema> schema() const override {
            return imported_schema;
        }

    protected:
        std::shared_ptr<duckdb::QueryResult> result_;
        std::shared_ptr<arrow::Schema> imported_schema;
};


class QueryEngine {
    public:
        virtual void Create(const std::string &path) = 0;
        virtual std::shared_ptr<arrow::RecordBatchReader> Execute(const std::string &query) = 0;
};


class DuckDBEngine : public QueryEngine {
    public:
        DuckDBEngine() {
            db = std::make_shared<duckdb::DuckDB>(nullptr);
            con = std::make_shared<duckdb::Connection>(*db);
        }

        void Create(const std::string &path) {
            con->Query("INSTALL parquet; LOAD parquet;");
            con->Query("PRAGMA threads=32;");
            std::string table_create_query = "CREATE VIEW dataset AS SELECT * FROM '" + path + "';";
            con->Query(table_create_query);    
        }

        std::shared_ptr<arrow::RecordBatchReader> Execute(const std::string &query) {
            auto statement = con->Prepare(query);
            auto result = statement->Execute();
            auto reader = std::make_shared<DuckDBRecordBatchReader>(std::move(result));
            return reader;
        }

    private:
        std::shared_ptr<duckdb::DuckDB> db;
        std::shared_ptr<duckdb::Connection> con;
};
