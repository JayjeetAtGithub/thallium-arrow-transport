#include <iostream>
#include <thallium.hpp>

namespace tl = thallium;


std::function<void(const tl::request&)> hello = 
    [](const tl::request &req, const int& a, const std::string &s) {
        return req.respond(0);
    };

int main(int argc, char** argv) {

    tl::engine myEngine("tcp", THALLIUM_SERVER_MODE);
    myEngine.define("hello", hello);
    std::cout << "Server running at address " << myEngine.self() << std::endl;

    return 0;
}

