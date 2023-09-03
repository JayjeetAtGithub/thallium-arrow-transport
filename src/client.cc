#include <thallium.hpp>
#include <chrono>
#include <thallium/serialization/stl/string.hpp>


namespace tl = thallium;

class RespStub {
    public:
        std::string buffer;
        int ret_code;

        RespStub() {}
        RespStub(std::string buffer, int ret_code) : buffer(buffer), ret_code(ret_code) {}

        template<typename A>
        void save(A& ar) const {
            ar & buffer;
            ar & ret_code;
        }

        template<typename A>
        void load(A& ar) {
            ar & buffer;
            ar & ret_code;
        }
};

int main(int argc, char** argv) {

    if(argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <address>" << std::endl;
        exit(0);
    }

    tl::engine myEngine("ofi+verbs", THALLIUM_CLIENT_MODE);
    tl::remote_procedure hello = myEngine.define("hello");
    
    std::string uri = argv[1];
    tl::endpoint server = myEngine.lookup(uri);

    std::string s = "helloworldhelloworldhelloworldhe";
    std::cout << "Size of request: " << s.size() << std::endl;

    hello.on(server)(1, s);
    std::cout << "Warmup done" << std::endl;
    for (int i = 0; i < 1000; i++) {
        auto start = std::chrono::high_resolution_clock::now();
        RespStub resp = hello.on(server)(2, s);
        std::cout << "Size of response: " << resp.buffer.size() << std::endl;
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
        std::cout << "Iteration " << i << " took " << duration << " microseconds" << std::endl;
    }
    return 0;
}
