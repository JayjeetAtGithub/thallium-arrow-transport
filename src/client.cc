#include <thallium.hpp>
#include <chrono>
#include <thallium/serialization/stl/string.hpp>
#include "utils.h"
#include "headers.h"
#include "constants.h"
#include "engine.h"

namespace tl = thallium;

int main(int argc, char** argv) {
    if(argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <address>" << std::endl;
        exit(0);
    }

    std::string query = "SELECT * FROM dataset WHERE total_amount >= 1030;";
    std::string path = "/users/noobjc/thallium-arrow-transport/scripts/nyc.parquet";
    std::shared_ptr<DuckDBEngine> db = std::make_shared<DuckDBEngine>();
    db->Create(path);
    std::shared_ptr<arrow::RecordBatchReader> reader = db->Execute(query);
    auto schema = reader->schema();

    // Read the server uri from the user
    std::string uri = argv[1];

    // Define thallium client engine and lookup server endpoint
    tl::engine engine("ofi+verbs", THALLIUM_SERVER_MODE);
    tl::endpoint endpoint = engine.lookup(uri);

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
        tl::bulk local = engine.expose(segments, tl::bulk_mode::write_only);

        // Pull the single byte from the remote bulk handle
        bulk.on(req.get_endpoint()) >> local;

        auto batch = UnpackBatch(buff, schema);
        std::cout << "Batch size: " << batch->num_rows() << std::endl;
        
        // Respond back with 0
        return req.respond(0);
    };
    // Define the `do_rdma` procedure
    engine.define("do_rdma", do_rdma);

    // Do 50 iterations to warmup the server
    // TODO: Look into thallium for why the first couple requests
    // are slow, mostly due to scheduling issues
    for (int i = 0; i < 50; i++) {
        get_data_bytes.on(endpoint)(1);
    }
    std::cout << "Warmup done" << std::endl;
    
    // Run 1000 iterations of reading a single byte from the server
    for (int i = 0; i < 1000; i++) {
        auto start = std::chrono::high_resolution_clock::now();
        get_data_bytes.on(endpoint)(0);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
        std::cout << "Iteration " << i << " took " << duration << " microseconds" << std::endl;
    }
    return 0;
}
