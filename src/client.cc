#include <thallium.hpp>
#include <chrono>
#include <thallium/serialization/stl/string.hpp>
#include "utils.h"
#include "headers.h"
#include "constants.h"
#include "engine.h"

namespace tl = thallium;

std::shared_ptr<arrow::Schema> read_schema() {
    std::string query = "SELECT * FROM dataset WHERE total_amount >= 1030;";
    std::string path = "/mnt/dataset/nyc.parquet";
    std::shared_ptr<DuckDBEngine> db = std::make_shared<DuckDBEngine>();
    db->Create(path);
    std::shared_ptr<arrow::RecordBatchReader> reader = db->Execute(query);
    return reader->schema();
}

int main(int argc, char** argv) {
    if(argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <address>" << std::endl;
        exit(0);
    }

    // Read the server uri from the user
    std::string uri = argv[1];

    std::shared_ptr<arrow::Schema> schema = read_schema();

    // Define thallium client engine and lookup server endpoint
    tl::engine engine("ofi+verbs", THALLIUM_SERVER_MODE);
    tl::endpoint endpoint = engine.lookup(uri);

    // Declare the `init_scan` remote procedure
    tl::remote_procedure init_scan = engine.define("init_scan");
    
    // Declare the `get_data_bytes` remote procedure
    tl::remote_procedure get_data_bytes = engine.define("get_data_bytes");

    // Define the `do_rdma` remote procedure
    std::function<void(const tl::request&, int64_t&, tl::bulk&)> do_rdma = 
        [&engine, &schema](const tl::request &req, int64_t& data_size, tl::bulk &bulk) {
        std::cout << "do_rdma : " << data_size << std::endl;

        // Reserve a single segment
        std::vector<std::pair<void*,std::size_t>> segments;
        segments.reserve(1);

        // Allocate memory for a single char and add it to the segment
        std::shared_ptr<arrow::Buffer> buff = arrow::AllocateBuffer(data_size).ValueOrDie();
        segments.emplace_back(std::make_pair((void*)buff->mutable_data(), buff->size()));
        
        // Expose the segment as a local bulk handle
        auto s = std::chrono::high_resolution_clock::now();
        tl::bulk local = engine.expose(segments, tl::bulk_mode::write_only);
        auto e = std::chrono::high_resolution_clock::now();
        std::cout << "expose took " << std::chrono::duration_cast<std::chrono::microseconds>(e-s).count() << " microseconds" << std::endl;


        // Pull the single byte from the remote bulk handle
        auto s2 = std::chrono::high_resolution_clock::now();
        bulk.on(req.get_endpoint()) >> local;
        auto e2 = std::chrono::high_resolution_clock::now();
        std::cout << "rdma pull took " << std::chrono::duration_cast<std::chrono::microseconds>(e2-s2).count() << " microseconds" << std::endl;

        auto s3 = std::chrono::high_resolution_clock::now();
        auto batch = UnpackBatch(buff, schema);
        auto e3 = std::chrono::high_resolution_clock::now();
        std::cout << "unpack took " << std::chrono::duration_cast<std::chrono::microseconds>(e3-s3).count() << " microseconds" << std::endl;

        std::cout << "Batch: " << batch->ToString() << std::endl;

        // Respond back with 0
        return req.respond(0);
    };
    // Define the `do_rdma` procedure
    engine.define("do_rdma", do_rdma);

    std::string query = "SELECT * FROM dataset WHERE total_amount >= 1030;";
    std::string path = "/mnt/dataset/nyc.1.parquet";

    // Do some warmup iterations
    for (int i = 0; i < 50; i++) {
        init_scan.on(endpoint)(query, path, 1);
        get_data_bytes.on(endpoint)(1);
    }
    
    init_scan.on(endpoint)(query, path, 0);
    // Run 1000 iterations of reading a single byte from the server
    for (int i = 0; i < 50; i++) {
        auto start = std::chrono::high_resolution_clock::now();
        get_data_bytes.on(endpoint)(0);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
        std::cout << "Iteration " << i << " took " << duration << " microseconds" << std::endl;
    }
    return 0;
}
