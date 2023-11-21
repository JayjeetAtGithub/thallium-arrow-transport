#include <thallium.hpp>
#include <iostream>

#include "utils.h"
#include "headers.h"
#include "engine.h"
#include "constants.h"

void WriteToStream(std::shared_ptr<arrow::io::OutputStream> output_stream, std::shared_ptr<arrow::RecordBatchReader> &reader) {
    std::shared_ptr<arrow::RecordBatch> batch;
    auto writer = arrow::ipc::MakeStreamWriter(output_stream, reader->schema()).ValueOrDie();
    while (reader->ReadNext(&batch).ok()) {
        if (!batch) {
            break;
        }
        std::cout << "Batch size: " << batch->num_rows() << std::endl;
        writer->WriteRecordBatch(*batch);
    }
    writer->Close();
}

int main(int argc, char **argv) {
    // read the parquet file into arrow tables
    std::shared_ptr<DuckDBEngine> db = std::make_shared<DuckDBEngine>();
    db->Create("/mnt/dataset/nyc.parquet");
    std::string query = "SELECT * FROM dataset WHERE total_amount >= 6304.9;"; 
    std::shared_ptr<arrow::RecordBatchReader> reader = db->Execute(query);

    // just write to a buffer output stream
    auto output_stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto start = std::chrono::high_resolution_clock::now();
    WriteToStream(output_stream, reader);
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Time taken: " << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << std::endl;

    return 0;
}
