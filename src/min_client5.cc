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

    // Define the `do_rdma` procedure
    engine.define("do_rdma", do_rdma);

    // Run 400 iterations of reading a single byte from the server
    for (int i = 0; i < 200; i++) {
        expose_memory_rpc(expose_memory, endpoint);
    }
    return 0;
}
