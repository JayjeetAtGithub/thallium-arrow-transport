#include <thallium.hpp>
#include <chrono>
#include <thallium/serialization/stl/string.hpp>


namespace tl = thallium;


int main(int argc, char** argv) {

    if(argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <address>" << std::endl;
        exit(0);
    }

    tl::engine engine("ofi+verbs", THALLIUM_SERVER_MODE);
    tl::remote_procedure get_single_byte = engine.define("get_single_byte");

    std::function<void(const tl::request&, const tl::bulk&)> do_rdma = 
        [](const tl::request &req, const tl::bulk &bulk) {
        std::cout << "do_rdma" << std::endl;

        std::vector<std::pair<void*,std::size_t>> segments;
        segments.reserve(1);

        char *single_char = new char[1];
        segments.emplace_back(std::make_pair((void*)(&single_char[0]), 1));
        
        tl::bulk local = engine.expose(segments, tl::bulk_mode::write_only);
        bulk.on(req.get_endpoint()) >> local;
    };

    get_single_byte.on(server)(0);
    std::cout << "Warmup done" << std::endl;
    for (int i = 0; i < 1000; i++) {
        auto start = std::chrono::high_resolution_clock::now();
        get_single_byte.on(server)(1);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
        std::cout << "Iteration " << i << " took " << duration << " microseconds" << std::endl;
    }
    return 0;
}
