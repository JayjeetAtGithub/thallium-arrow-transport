#include <iostream>
#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

#include "utils.h"
#include "constants.h"

namespace tl = thallium;

int main(int argc, char** argv) {
    tl::engine engine("ofi+verbs", THALLIUM_SERVER_MODE);


    std::function<void(const tl::request&, tl::bulk&)> get_single_byte = 
    [](const tl::request &req, tl::bulk &bulk) {
        std::cout << "get_single_byte" << std::endl;

        std::vector<std::pair<void*,std::size_t>> segments;
        segments.reserve(1);
        
        std::string single_char = "x";
        segments.emplace_back(std::make_pair((void*)(&single_char[0]), single_char.size()));
        tl::bulk local = engine.expose(segments, tl::bulk_mode::read_only);
        bulk.on(req.get_endpoint()) >> local;
    };

    engine.define("get_single_byte", get_single_byte);
    WriteToFile(engine.self(), TL_URI_PATH, false);
    std::cout << "Server running at address " << myEngine.self() << std::endl;
    return 0;
}

