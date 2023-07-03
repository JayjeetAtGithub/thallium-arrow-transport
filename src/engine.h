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
        virtual std::shared_ptr<arrow::RecordBatchReader> ExecuteEager(const std::string &query) = 0;
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
            std::string table_create_query = "CREATE TEMP TABLE dataset AS SELECT * FROM read_parquet('" + path + "');";
            con->Query(table_create_query);    
        }

        std::shared_ptr<arrow::RecordBatchReader> Execute(const std::string &query) {
            auto statement = con->Prepare(query);
            auto result = statement->Execute();
            return std::make_shared<DuckDBRecordBatchReader>(std::move(result));
        }

        std::shared_ptr<arrow::RecordBatchReader> ExecuteEager(const std::string &query) {
            auto statement = con->Prepare(query);
            auto result = statement->Execute();
            auto reader = std::make_shared<DuckDBRecordBatchReader>(std::move(result));

            std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
            std::shared_ptr<arrow::RecordBatch> batch;
            while (reader->ReadNext(&batch).ok() && batch != nullptr) {
                batches.push_back(batch);
            }

            auto table = arrow::Table::FromRecordBatches(batches).ValueOrDie();
            auto ds = std::make_shared<arrow::dataset::InMemoryDataset>(table);
            auto scanner_builder = ds->NewScan().ValueOrDie();
            auto scanner = scanner_builder->Finish().ValueOrDie();
            return scanner->ToRecordBatchReader().ValueOrDie();
        }

    private:
        std::shared_ptr<duckdb::DuckDB> db;
        std::shared_ptr<duckdb::Connection> con;
};

class AceroEngine : public QueryEngine {
    public:
        AceroEngine() {}

        void Create(const std::string &path) {
            uri = "file://" + path;
        }

        std::shared_ptr<arrow::RecordBatchReader> Execute(const std::string &query) {
           return ExecuteEager(query);
        }

        std::shared_ptr<arrow::RecordBatchReader> ExecuteEager(const std::string &query) {
            std::string path;
            auto fs = arrow::fs::FileSystemFromUri(uri, &path).ValueOrDie();
            arrow::fs::FileSelector s;
            s.base_dir = std::move(path);
            s.recursive = true;

            arrow::dataset::FileSystemFactoryOptions options;
            auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();

            auto factory =
                arrow::dataset::FileSystemDatasetFactory::Make(std::move(fs), s, std::move(format), options).ValueOrDie();
            arrow::dataset::FinishOptions finish_options;
            auto dataset = factory->Finish(finish_options).ValueOrDie();

            auto scanner_builder, dataset->NewScan();
            scanner_builder->Filter(GetFilter());
            scanner_builder->UseThreads(true);
            scanner_builder->Project(schema->field_names());
            auto scanner = scanner_builder->Finish().ValueOrDie();
            auto table = scanner->ToTable().ValueOrDie();

            auto ds = std::make_shared<arrow::dataset::InMemoryDataset>(table);
            scanner_builder = ds->NewScan().ValueOrDie();
            scanner = scanner_builder->Finish().ValueOrDie();
            return scanner->ToRecordBatchReader().ValueOrDie();
        }

    protected:
        std::string uri;
};
