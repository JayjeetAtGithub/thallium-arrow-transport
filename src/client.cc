#include <thallium.hpp>
#include <chrono>


namespace tl = thallium;

int main(int argc, char** argv) {

    if(argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <address>" << std::endl;
        exit(0);
    }

    tl::engine myEngine("tcp", THALLIUM_CLIENT_MODE);
    tl::remote_procedure hello = myEngine.define("hello").disable_response();
    tl::endpoint server = myEngine.lookup(argv[1]);
    
    auto start = std::chrono::high_resolution_clock::now();
    hello.on(server)();
    auto end = std::chrono::high_resolution_clock::now();
    std::string exec_time_ms = std::to_string((double)std::chrono::duration_cast<std::chrono::microseconds>(end-start).count()/1000) + "\n";
    std::cout << exec_time_ms << std::endl;
    return 0;
}