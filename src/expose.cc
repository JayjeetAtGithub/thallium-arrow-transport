#include <iostream>
#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

#include "utils.h"
#include "constants.h"
#include <iostream>
#include <string>
#include <random>

namespace tl = thallium;


std::string GenerateData(int N) {
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

    tl::engine engine("ofi+verbs", THALLIUM_SERVER_MODE);

    std::vector<std::string> list_of_strs;
    for (int i = 0; i < 20; i++) {
        list_of_strs.push_back(GenerateData(data_size));
    }  

    auto s1 = std::chrono::high_resolution_clock::now();
    std::vector<std::pair<void*,std::size_t>> segments;
    segments.reserve(20);
    auto e1 = std::chrono::high_resolution_clock::now();
    std::cout << "server/create_segments: " << std::chrono::duration_cast<std::chrono::microseconds>(e1-s1).count() << std::endl;

    auto s2 = std::chrono::high_resolution_clock::now();
    std::vector<std::string> dataset(list_of_strs.size());
    for (int i = 0; i < 20; i++) {
        dataset.emplace_back(list_of_strs[i]);
    }
    auto e2 = std::chrono::high_resolution_clock::now();
    std::cout << "server/generate_data: " << std::chrono::duration_cast<std::chrono::microseconds>(e2-s2).count() << std::endl;

    auto s3 = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < 20; i++) {
        segments.emplace_back(std::make_pair((void*)(&dataset[i][0]), dataset[i].size()));
    }
    auto e3 = std::chrono::high_resolution_clock::now();
    std::cout << "server/populate_segments: " << std::chrono::duration_cast<std::chrono::microseconds>(e3-s3).count() << std::endl;
    
    // Expose the segment and send it as argument to `do_rdma`
    auto s4 = std::chrono::high_resolution_clock::now();
    tl::bulk bulk = engine.expose(segments, tl::bulk_mode::read_only);
    auto e4 = std::chrono::high_resolution_clock::now();
    std::cout << "server/expose: " << std::chrono::duration_cast<std::chrono::microseconds>(e4-s4).count() << std::endl;

    // Write the server URI to a file
    return 0;
}
