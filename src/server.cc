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

    std::unordered_map<int, std::shared_ptr<arrow::RecordBatch>> reader_map;

    // Define the `init_scan` procedure
    // This procedure reads out a single batch from the result iterator
    std::function<void(const tl::request&, const std::string&, const std::string&, const int64_t&)> init_scan = 
        [&reader_map](const tl::request &req, const std::string& query, const std::string& path, const int64_t& warmup) {
            if (warmup == 1) {
                std::cout << "Warmup init_scan" << std::endl;
                return req.respond(0);
            }
            std::cout << "init_scan" << std::endl;
            std::shared_ptr<DuckDBEngine> db = std::make_shared<DuckDBEngine>();
            db->Create(path);
            std::shared_ptr<arrow::RecordBatchReader> reader = db->Execute(query);
            reader_map[0] = reader;
            return req.respond(0);
    };

    // Define the `get_data_bytes` procedure
    std::function<void(const tl::request&, const int&)> get_data_bytes = 
    [&do_rdma, &engine, &reader_map](const tl::request &req, const int& warmup) {
        // If warmup, then just return
        if (warmup == 1) {
            std::cout << "Warmup" << std::endl;
            return req.respond(0);
        }

        // Reserve a single segment
        std::vector<std::pair<void*,std::size_t>> segments;
        segments.reserve(1);

        std::shared_ptr<arrow::RecordBatch> batch;
        reader_map[0]->ReadNext(&batch);

        // Read out a single batch
        auto s2 = std::chrono::high_resolution_clock::now();
        auto buff = PackBatch(reader_map[0]);
        auto e2 = std::chrono::high_resolution_clock::now();
        std::cout << "pack took " << std::chrono::duration_cast<std::chrono::microseconds>(e2-s2).count() << " microseconds" << std::endl;

        segments.emplace_back(std::make_pair((void*)buff->data(), buff->size()));

        // Expose the segment and send it as argument to `do_rdma`
        auto s = std::chrono::high_resolution_clock::now();
        tl::bulk bulk = engine.expose(segments, tl::bulk_mode::read_only);
        auto e = std::chrono::high_resolution_clock::now();
        auto d = std::chrono::duration_cast<std::chrono::microseconds>(e-s).count();
        std::cout << "expose took " << d << " microseconds" << std::endl;
        do_rdma.on(req.get_endpoint())(buff->size(), bulk);


        // Respond back with 0
        return req.respond(0);
    };

    // Define the `init_scan` procedure
    engine.define("init_scan", init_scan);

    // Define the `get_data_bytes` procedure
    engine.define("get_data_bytes", get_data_bytes);

    // Write the server uri to a file
    WriteToFile(engine.self(), TL_URI_PATH, false);
    std::cout << "Server running at address " << engine.self() << std::endl;
    return 0;
}
