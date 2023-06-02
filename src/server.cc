#include <iostream>

#include "utils.h"
#include "headers.h"
#include "constants.h"

namespace tl = thallium;


int main(int argc, char** argv) {
    tl::engine engine("ofi+verbs", THALLIUM_SERVER_MODE, true);
    margo_instance_id mid = engine.get_margo_instance();
    hg_addr_t svr_addr;
    hg_return_t hret = margo_addr_self(mid, &svr_addr);
    if (hret != HG_SUCCESS) {
        std::cerr << "Error: margo_addr_lookup()\n";
        margo_finalize(mid);
        return -1;
    }

    std::function<void(const tl::request&)> scan = 
        [](const tl::request &req) {
            std::cout << "Hello World : " << getTimestamp() << std::endl;
        };


    engine.define("scan", scan);
    WriteToFile(engine.self(), TL_URI_PATH, false);
    std::cout << "Serving at: " << engine.self() << std::endl;
    engine.wait_for_finalize();
};
