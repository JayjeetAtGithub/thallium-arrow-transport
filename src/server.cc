#include <iostream>
#include <thallium.hpp>
#include "utils.h"

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
            std::cout << "Server: Reached inside RPC body at: " << PrintTimestamp() << std::endl;
            return req.respond(0);
        };


    engine.define("scan", scan);
    WriteToFile(engine.self(), "/proj/schedock-PG0/thallium_uri", false);
    std::cout << "Serving at: " << engine.self() << std::endl;
    engine.wait_for_finalize();
};
