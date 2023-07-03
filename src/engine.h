#include <duckdb.hpp>
#include <duckdb/common/arrow/arrow_converter.hpp>
#include <duckdb/common/arrow/arrow_wrapper.hpp>

#include <arrow/api.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>

#include "constants.h"

auto schema = arrow::schema({
    arrow::field("VendorID", arrow::int64()),
    arrow::field("tpep_pickup_datetime", arrow::timestamp(arrow::TimeUnit::MICRO)),
    arrow::field("tpep_dropoff_datetime", arrow::timestamp(arrow::TimeUnit::MICRO)),
    arrow::field("passenger_count", arrow::int64()),
    arrow::field("trip_distance", arrow::float64()),
    arrow::field("RatecodeID", arrow::int64()),
    arrow::field("store_and_fwd_flag", arrow::utf8()),
    arrow::field("PULocationID", arrow::int64()),
    arrow::field("DOLocationID", arrow::int64()),
    arrow::field("payment_type", arrow::int64()),
    arrow::field("fare_amount", arrow::float64()),
    arrow::field("extra", arrow::float64()),
    arrow::field("mta_tax", arrow::float64()),
    arrow::field("tip_amount", arrow::float64()),
    arrow::field("tolls_amount", arrow::float64()),
    arrow::field("improvement_surcharge", arrow::float64()),
    arrow::field("total_amount", arrow::float64())
});

arrow::compute::Expression GetFilter(const std::string &query) {
    if (query == "SELECT * FROM dataset;") {
        return arrow::compute::greater(arrow::compute::field_ref("total_amount"),
                                        arrow::compute::literal(-200));
    } else if (query == "SELECT * FROM dataset WHERE total_amount > 27;") {
        return arrow::compute::greater(arrow::compute::field_ref("total_amount"),
                                        arrow::compute::literal(27));
    } else if (query == "SELECT * FROM dataset WHERE total_amount > 69;") {
        return arrow::compute::greater(arrow::compute::field_ref("total_amount"),
                                        arrow::compute::literal(69));
    } else if (query == "SELECT * FROM dataset WHERE total_amount > 104;") {
        return arrow::compute::greater(arrow::compute::field_ref("total_amount"),
                                        arrow::compute::literal(104));
    } else if (query == "SELECT * FROM dataset WHERE total_amount > 199;") {
        return arrow::compute::greater(arrow::compute::field_ref("total_amount"),
                                        arrow::compute::literal(199));
    } else if (query == "SELECT * FROM dataset WHERE total_amount > 520;") {
        return arrow::compute::greater(arrow::compute::field_ref("total_amount"),
                                        arrow::compute::literal(520));
    } else if (query == "SELECT total_amount, fare_amount, tip_amount FROM dataset WHERE total_amount > 520;") {
        schema = arrow::schema({
            arrow::field("total_amount", arrow::float64()),
            arrow::field("fare_amount", arrow::float64()),
            arrow::field("tip_amount", arrow::float64())
        });
        return arrow::compute::greater(arrow::compute::field_ref("total_amount"),
                                        arrow::compute::literal(520));
    }
}


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
            std::string table_create_query = "CREATE TABLE dataset AS SELECT * FROM read_parquet('" + path + "');";
            con->Query(table_create_query);    
        }

        std::shared_ptr<arrow::RecordBatchReader> Execute(const std::string &query) {
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
            dataset_path = path;
            std::string path_non_const = const_cast<std::string&>(path);
            path_non_const = path_non_const.substr(0, path_non_const.length() - 2);
            uri = "file://" + path_non_const;
        }

        std::shared_ptr<arrow::RecordBatchReader> Execute(const std::string &query) {
            if (query == "SELECT sum(total_amount) FROM dataset;") {
                std::shared_ptr<DuckDBEngine> db = std::make_shared<DuckDBEngine>();
                db->Create(dataset_path);
                return db->Execute(query);
            }

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

            auto scanner_builder = dataset->NewScan().ValueOrDie();
            scanner_builder->Filter(GetFilter(query));
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
        std::string dataset_path;
        std::string uri;
};
