#include <iostream>
#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

#include "utils.h"
#include "constants.h"

namespace tl = thallium;

int main(int argc, char** argv) {
    tl::engine engine("ofi+verbs", THALLIUM_SERVER_MODE);
    tl::remote_procedure do_rdma = engine.define("do_rdma");

    std::function<void(const tl::request&, const int&)> get_single_byte = 
    [&do_rdma](const tl::request &req, const int& warmup) {

        if (warmup == 1) {
            std::cout << "Warmup" << std::endl;
            return;
        }

        std::cout << "get_single_byte" << std::endl;

        std::vector<std::pair<void*,std::size_t>> segments;
        segments.reserve(1);
        
        std::string single_char = "x";
        segments.emplace_back(std::make_pair((void*)(&single_char[0]), single_char.size()));
        tl::bulk bulk = engine.expose(segments, tl::bulk_mode::read_only);
        do_rdma.on(req.get_endpoint())(bulk);
    };

    engine.define("get_single_byte", get_single_byte);
    WriteToFile(engine.self(), TL_URI_PATH, false);
    std::cout << "Server running at address " << myEngine.self() << std::endl;
    return 0;
}
