#include <iostream>
#include <thread>
#include <chrono>
#include <fstream>

#include "utils.h"
#include "headers.h"
#include "constants.h"

namespace tl = thallium;

arrow::Status Main(int argc, char **argv) {
    std::string uri = argv[1];
    std::string path = argv[2];
    std::string query = argv[3];
    std::string mode = argv[4];

    tl::engine engine("ofi+verbs", THALLIUM_SERVER_MODE, true);
    tl::endpoint endpoint = engine.lookup(uri);
    tl::remote_procedure scan = engine.define("scan");

    std::cout << getTimestamp() << std::endl;
    scan.on(endpoint)();
    
    return arrow::Status::OK();
}

int main(int argc, char** argv) {
    arrow::Status s = Main(argc, argv);
    return s.ok() ? 0 : 1;
}
