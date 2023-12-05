#include <thallium.hpp>
#include <iostream>

#include "utils.h"
#include "headers.h"
#include "engine.h"
#include "constants.h"

void WriteToStream(std::shared_ptr<arrow::io::OutputStream> output_stream, std::shared_ptr<arrow::RecordBatchReader> &reader) {
    std::shared_ptr<arrow::RecordBatch> batch;
    auto writer = arrow::ipc::MakeStreamWriter(output_stream, reader->schema()).ValueOrDie();
    int64_t total_rows = 0;
    while (reader->ReadNext(&batch).ok()) {
        if (!batch) {
            break;
        }
        total_rows += batch->num_rows();
        writer->WriteRecordBatch(*batch);
    }
    writer->Close();
    std::cout << "Total number of rows: " << total_rows << std::endl;
}

int main(int argc, char **argv) {
    // read the parquet file into arrow tables
    std::shared_ptr<DuckDBEngine> db = std::make_shared<DuckDBEngine>();
    db->Create("/mnt/dataset/nyc.1.parquet");
    std::string query = "SELECT * FROM dataset;"; 
    std::shared_ptr<arrow::RecordBatchReader> reader = db->Execute(query);

    // just write to a buffer output stream
    auto output_stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto start = std::chrono::high_resolution_clock::now();
    WriteToStream(output_stream, reader);
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Time taken: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " ms" << std::endl;

    auto buff = output_stream->Finish().ValueOrDie();
    std::cout << "Size of buffer: " << (double)(buff->size() / (1000 * 1000)) << " MB" << std::endl;

    return 0;
}
