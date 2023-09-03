#include <iostream>
#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

#include "utils.h"
#include "constants.h"

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


std::function<void(const tl::request&, const int&, std::string&)> hello = 
    [](const tl::request &req, const int& a, const std::string &s) {
        RespStub resp;
        resp.buffer = "helloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhellow";
        resp.ret_code = 0;
        return req.respond(resp);
    };


int main(int argc, char** argv) {
    tl::engine myEngine("ofi+verbs", THALLIUM_SERVER_MODE);
    myEngine.define("hello", hello);
    WriteToFile(engine.self(), TL_URI_PATH, false);
    std::cout << "Server running at address " << myEngine.self() << std::endl;
    return 0;
}

