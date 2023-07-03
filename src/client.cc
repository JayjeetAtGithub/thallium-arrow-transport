#include <thallium.hpp>
#include <chrono>
#include <thallium/serialization/stl/string.hpp>


namespace tl = thallium;

int main(int argc, char** argv) {

    if(argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <address>" << std::endl;
        exit(0);
    }

    tl::engine myEngine("verbs", THALLIUM_CLIENT_MODE);
    tl::remote_procedure hello = myEngine.define("hello");
    tl::endpoint server = myEngine.lookup(argv[1]);

    std::string s = "hello world";

    hello.on(server)(1, s);
    std::cout << "Warmup done" << std::endl;
    for (int i = 0; i < 100; i++) {
        auto start = std::chrono::high_resolution_clock::now();
        hello.on(server)(2, s);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
        std::cout << "Iteration: " << i << " took " << duration << std::endl;
    }
    return 0;
}
