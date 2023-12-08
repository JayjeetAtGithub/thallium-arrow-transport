#include <iostream>
#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

#include "utils.h"
#include "headers.h"
#include "constants.h"
#include "engine.h"

namespace tl = thallium;

int main(int argc, char** argv) {
    // Define the thallium server
    tl::engine engine("ofi+verbs", THALLIUM_SERVER_MODE);

    // Declare the `do_rdma` remote procedure
    tl::remote_procedure do_rdma = engine.define("do_rdma");


    // DuckDB query
    std::string query = "SELECT * FROM dataset WHERE total_amount >= 1030;";
    std::string path = "/mnt/dataset/nyc.1.parquet";
    std::shared_ptr<DuckDBEngine> db = std::make_shared<DuckDBEngine>();
    db->Create(path);
    std::shared_ptr<arrow::RecordBatchReader> reader = db->Execute(query);
    std::shared_ptr<arrow::RecordBatch> batch;
    reader->ReadNext(&batch);
    auto buff = PackBatch(batch);
    std::cout << "Batch size: " << buff->size() << std::endl;


    // Define the `get_data_bytes` procedure
    std::function<void(const tl::request&, const int&)> get_data_bytes = 
    [&do_rdma, &engine, &buff](const tl::request &req, const int& warmup) {
        // If warmup, then just return
        if (warmup == 1) {
            std::cout << "Warmup" << std::endl;
            return req.respond(0);
        }

        std::cout << "get_data_bytes" << std::endl;
        
        // Reserve a single segment
        std::vector<std::pair<void*,std::size_t>> segments;
        segments.reserve(1);
        
        segments.emplace_back(std::make_pair((void*)buff->data(), buff->size()));
        // Expose the segment and send it as argument to `do_rdma`
        tl::bulk bulk = engine.expose(segments, tl::bulk_mode::read_only);
        do_rdma.on(req.get_endpoint())(bulk);

        // Respond back with 0
        return req.respond(0);
    };

    // Define the `get_data_bytes` procedure
    engine.define("get_data_bytes", get_data_bytes);

    // Write the server uri to a file
    WriteToFile(engine.self(), TL_URI_PATH, false);
    std::cout << "Server running at address " << engine.self() << std::endl;
    return 0;
}
