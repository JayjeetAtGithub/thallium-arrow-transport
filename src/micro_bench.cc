#include <thallium.hpp>
#include <iostream>

#include "utils.h"
#include "headers.h"
#include "engine.h"
#include "constants.h"

int main(int argc, char **argv) {
    // read the parquet file into arrow tables
    std::shared_ptr<DuckDBEngine> db = std::make_shared<DuckDBEngine>();
    db->Create("/mnt/dataset/nyc.parquet");
    std::string query = "SELECT * FROM dataset WHERE total_amount >= 6304.9;"; 
    std::shared_ptr<arrow::RecordBatchReader> reader = db->Execute(query);

    // read out batches from the tables and serializd each of them
    std::shared_ptr<arrow::RecordBatch> batch;
    auto output_stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
    while (reader->ReadNext(&batch).ok()) {
        arrow::ipc::IpcWriteOptions options;
        arrow::ipc::WriteRecordBatchStream(std::vector<std::shared_ptr<arrow::RecordBatch>>{batch}, options, output_stream.get());
    }
}
