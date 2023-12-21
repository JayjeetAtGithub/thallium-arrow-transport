#include <thallium.hpp>
#include <chrono>
#include <thallium/serialization/stl/string.hpp>

namespace tl = thallium;

void expose_memory_rpc(tl::remote_procedure &expose_memory, tl::endpoint& endpoint) {
    for (int i = 0; i < 200; i++) {
        expose_memory.on(endpoint)(1);
    }
    auto s = std::chrono::high_resolution_clock::now();
    expose_memory.on(endpoint)(0);
    auto e = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(e-s).count();
    std::cout << "client/total: " << duration << std::endl;
}

int main(int argc, char** argv) {
    if(argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <address>" << std::endl;
        exit(0);
    }

    // Read the server uri from the user
    std::string uri = argv[1];
    int32_t data_size = argv[2] ? atoi(argv[2]) : 1;

    // Define thallium client engine and lookup server endpoint
    tl::engine engine("ofi+verbs", THALLIUM_SERVER_MODE);
    tl::endpoint endpoint = engine.lookup(uri);

    // Declare the `expose_memory` remote procedure
    tl::remote_procedure expose_memory = engine.define("expose_memory");

    // Define the `do_rdma` remote procedure
    std::function<void(const tl::request&, const tl::bulk&)> do_rdma = 
        [&engine, &data_size](const tl::request &req, const tl::bulk &bulk) {
        // Reserve a single segment
        auto s1 = std::chrono::high_resolution_clock::now();
        std::vector<std::pair<void*,std::size_t>> segments;
        segments.reserve(1);
        char *data = new char[data_size];
        segments.emplace_back(std::make_pair((void*)(&data[0]), data_size));
        auto e1 = std::chrono::high_resolution_clock::now();
        std::cout << "client/create_segment: " << std::chrono::duration_cast<std::chrono::microseconds>(e1-s1).count() << std::endl;
        
        // Expose the segment as a local bulk handle
        auto s2 = std::chrono::high_resolution_clock::now();
        tl::bulk local = engine.expose(segments, tl::bulk_mode::write_only);
        auto e2 = std::chrono::high_resolution_clock::now();
        std::cout << "client/expose: " << std::chrono::duration_cast<std::chrono::microseconds>(e2-s2).count() << std::endl;

        // Pull the single byte from the remote bulk handle
        auto s3 = std::chrono::high_resolution_clock::now();
        bulk.on(req.get_endpoint()) >> local;
        auto e3 = std::chrono::high_resolution_clock::now();
        std::cout << "client/rdma_pull: " << std::chrono::duration_cast<std::chrono::microseconds>(e3-s3).count() << std::endl;
        
        // Respond back with 0
        return req.respond(0);
    };
    // Define the `do_rdma` procedure
    engine.define("do_rdma", do_rdma);

    // Run 400 iterations of reading a single byte from the server
    for (int i = 0; i < 200; i++) {
        expose_memory_rpc(expose_memory, endpoint);
    }
    return 0;
}
