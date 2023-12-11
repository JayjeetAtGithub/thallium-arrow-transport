#include <iostream>
#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

#include "utils.h"
#include "constants.h"
#include <iostream>
#include <string>
#include <random>

namespace tl = thallium;


std::string generateRandomString(int N) {
    // Define the characters allowed in the random string
    const std::string allowedChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    // Create a random number generator
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<int> distribution(0, allowedChars.size() - 1);

    // Generate the random string
    std::string randomString;
    for (int i = 0; i < N; ++i) {
        randomString += allowedChars[distribution(generator)];
    }

    return randomString;
}

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
        // Reserve a single segment
        std::vector<std::pair<void*,std::size_t>> segments;
        segments.reserve(1);
        
        // Map the buffer for the single char to the segment
        auto s2 = std::chrono::high_resolution_clock::now();
        std::string data = generateRandomString(data_size);
        auto e2 = std::chrono::high_resolution_clock::now();
        std::cout << "generateRandomString: " << std::chrono::duration_cast<std::chrono::microseconds>(e2-s2).count() << std::endl;

        std::cout << "Sending: " << data << std::endl;

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