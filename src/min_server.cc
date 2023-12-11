#include <iostream>
#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

#include "utils.h"
#include "constants.h"

namespace tl = thallium;

int main(int argc, char** argv) {
    int32_t data_size = argv[1] ? atoi(argv[1]) : 1;

    // Define the thallium server
    tl::engine engine("ofi+verbs", THALLIUM_SERVER_MODE);

    // Declare the `do_rdma` remote procedure
    tl::remote_procedure do_rdma = engine.define("do_rdma");

    // Define the `get_data_bytes` procedure
    std::function<void(const tl::request&, const int&)> get_data_bytes = 
    [&do_rdma, &engine, &data_size](const tl::request &req, const int& warmup) {
        if (warmup == 1) {
            std::cout << "Warmup get_data_bytes" << std::endl;
            return req.respond(0);
        }
        std::cout << "get_data_bytes" << std::endl;
        
        // Reserve a single segment
        std::vector<std::pair<void*,std::size_t>> segments;
        segments.reserve(1);
        
        // Map the buffer for the single char to the segment
        std::string data = "";
        // Append `n` chars to the string
        for (int32_t i = 0; i < data_size; i++) {
            data += "x";
        }
        segments.emplace_back(std::make_pair((void*)(&data[0]), data.size()));

        // Expose the segment and send it as argument to `do_rdma`
        auto s = std::chrono::high_resolution_clock::now();
        tl::bulk bulk = engine.expose(segments, tl::bulk_mode::read_only);
        auto e = std::chrono::high_resolution_clock::now();
        std::cout << "server.expose: " << std::chrono::duration_cast<std::chrono::microseconds>(e-s).count() << std::endl;

        do_rdma.on(req.get_endpoint())(bulk);

        // Respond back with 0
        return req.respond(0);
    };
    // Declare the `get_data_bytes` procedure
    engine.define("get_data_bytes", get_data_bytes);
    
    // Define the `init_scan` procedure
    std::function<void(const tl::request&, const int&)> init_scan = 
        [](const tl::request &req, const int& warmup) {
            if (warmup == 1) {
                std::cout << "Warmup init_scan" << std::endl;
                return req.respond(0);
            }
            std::cout << "init_scan" << std::endl;
            return req.respond(0);
    };
    // Declare the `init_scan` procedure
    engine.define("init_scan", init_scan);

    // Write the server uri to a file
    WriteToFile(engine.self(), TL_URI_PATH, false);
    std::cout << "Server running at address " << engine.self() << std::endl;
    return 0;
}