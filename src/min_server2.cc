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

    // Preregister some buffers
    std::vector<std::pair<void*,std::size_t>> segments;
    segments.reserve(20);
    for (int i = 0; i < 20; i++) {
        char *data = new char[128];
        segments.emplace_back(std::make_pair((void*)data, data_size));
    }
    auto s = std::chrono::high_resolution_clock::now();
    tl::bulk bulk = engine.expose(segments, tl::bulk_mode::read_write);
    auto e = std::chrono::high_resolution_clock::now();
    std::cout << "server.expose: " << std::chrono::duration_cast<std::chrono::microseconds>(e-s).count() << std::endl;

    // Define the `get_data_bytes` procedure
    std::function<void(const tl::request&, const int&)> get_data_bytes = 
    [&do_rdma, &engine, &data_size, &segments, &bulk](const tl::request &req, const int& warmup) {
        if (warmup == 1) {
            return req.respond(0);
        }

        std::vector<std::string> dataset;
        for (int i = 0; i < 20; i++) {
            std::string s = generateRandomString(data_size);
            // copy this into the segment
            std::memcpy(segments[i].first, s.c_str(), data_size);
        }

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
                return req.respond(0);
            }
            return req.respond(0);
    };
    // Declare the `init_scan` procedure
    engine.define("init_scan", init_scan);

    // Write the server uri to a file
    WriteToFile(engine.self(), TL_URI_PATH, false);
    std::cout << "Server running at address " << engine.self() << std::endl;
    return 0;
}