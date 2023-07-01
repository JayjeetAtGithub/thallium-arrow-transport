#include <iostream>
#include <thallium.hpp>

namespace tl = thallium;

void hello(const tl::request& req) {
    return req.respond(0);
}

int main(int argc, char** argv) {

    tl::engine myEngine("tcp", THALLIUM_SERVER_MODE);
    myEngine.define("hello", hello);
    std::cout << "Server running at address " << myEngine.self() << std::endl;

    return 0;
}

