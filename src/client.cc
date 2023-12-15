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

void call_init_scan_rpc(tl::remote_procedure &init_scan, tl::endpoint& endpoint, std::string &query, std::string &path) {
    init_scan.on(endpoint)(query, path, 0);
}

void call_get_data_bytes_rpc(tl::remote_procedure &get_data_bytes, tl::endpoint& endpoint) {
    for (int i = 0; i < 200; i++) {
        get_data_bytes.on(endpoint)(1);
    }
    auto start = std::chrono::high_resolution_clock::now();
    get_data_bytes.on(endpoint)(0);
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
    std::cout << "total: " << duration << std::endl;
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
        // Reserve a single segment
        // Allocate memory for a single char and add it to the segment
        auto s4 = std::chrono::high_resolution_clock::now();
        std::vector<std::pair<void*,std::size_t>> segments;
        segments.reserve(1);
        std::shared_ptr<arrow::Buffer> buff = arrow::AllocateBuffer(data_size).ValueOrDie();
        segments.emplace_back(std::make_pair((void*)buff->mutable_data(), buff->size()));
        auto e4 = std::chrono::high_resolution_clock::now();
        std::cout << "client/allocate_buffer: " << std::chrono::duration_cast<std::chrono::microseconds>(e4-s4).count() << std::endl;
 
        // Expose the segment as a local bulk handle
        auto s1 = std::chrono::high_resolution_clock::now();
        tl::bulk local = engine.expose(segments, tl::bulk_mode::write_only);
        auto e1 = std::chrono::high_resolution_clock::now();
        std::cout << "client.expose: " << std::chrono::duration_cast<std::chrono::microseconds>(e1-s1).count() << std::endl;

        // Pull the single byte from the remote bulk handle
        auto s2 = std::chrono::high_resolution_clock::now();
        bulk.on(req.get_endpoint()) >> local;
        auto e2 = std::chrono::high_resolution_clock::now();
        std::cout << "client/rdma: " << std::chrono::duration_cast<std::chrono::microseconds>(e2-s2).count() << std::endl;

        auto s3 = std::chrono::high_resolution_clock::now();
        auto batch = UnpackBatch(buff, schema);
        auto e3 = std::chrono::high_resolution_clock::now();
        std::cout << "client/unpack_batch: " << std::chrono::duration_cast<std::chrono::microseconds>(e3-s3).count() << std::endl;

        // Respond back with 0
        return req.respond(0);
    };
    // Define the `do_rdma` procedure
    engine.define("do_rdma", do_rdma);

    std::string query = "SELECT * FROM dataset WHERE total_amount >= 1030;";
    std::string path = "/mnt/dataset/nyc.1.parquet";

    // Run 1000 iterations of reading a single byte from the server
    for (int i = 0; i < 200; i++) {
        call_init_scan_rpc(init_scan, endpoint, query, path);
        call_get_data_bytes_rpc(get_data_bytes, endpoint);
    }
    return 0;
}
