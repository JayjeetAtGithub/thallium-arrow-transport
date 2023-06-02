#include <iostream>
#include <thread>
#include <chrono>
#include <fstream>
#include <unistd.h>


#include <thallium.hpp>
#include "utils.h"

namespace tl = thallium;

int main(int argc, char** argv) {
    std::string uri = argv[1];
    std::string path = argv[2];
    std::string query = argv[3];
    std::string mode = argv[4];

    std::cout << uri << std::endl;

    tl::engine engine("ofi+verbs", THALLIUM_CLIENT_MODE, true);
    tl::endpoint endpoint = engine.lookup(uri);
    tl::remote_procedure scan = engine.define("scan");

    for (int i = 0; i < 100; i++) {
        std::cout << getTimestamp() << std::endl;
        scan.on(endpoint)();
        sleep(2);
    }
}
