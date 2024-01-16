#include <iostream>
#include <csignal>

#include "utils.h"
#include "headers.h"
#include "engine.h"
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

    std::unordered_map<std::string, std::shared_ptr<arrow::RecordBatchReader>> reader_map;
    std::function<void(const tl::request&, const std::string&, const std::string&)> init_scan = 
        [&reader_map](const tl::request &req, const std::string& path, const std::string& query) {
            std::cout << "Request: " << query << "@" << path << std::endl;
            std::shared_ptr<DuckDBEngine> db = std::make_shared<DuckDBEngine>();
            db->Create(path);
            std::shared_ptr<arrow::RecordBatchReader> reader = db->Execute(query);
            std::string uuid = boost::uuids::to_string(boost::uuids::random_generator()());
            reader_map[uuid] = reader;
            std::shared_ptr<arrow::Buffer> buff = arrow::ipc::SerializeSchema(*(reader->schema())).ValueOrDie();

            InitScanRespStub resp;
            resp.schema = std::string(reinterpret_cast<const char*>(buff->data()), static_cast<size_t>(buff->size()));
            resp.uuid = uuid;
            return req.respond(resp);
        };

    std::function<void(const tl::request&)> finalize = 
        [&reader_map, &engine](const tl::request &req) {
            reader_map.clear();
            engine.finalize();
        };

    std::function<void(const tl::request&, const int&, const std::string&)> get_next_batch = 
        [&reader_map, &engine](const tl::request &req, const int& warmup, const std::string &uuid) {
            if (warmup) {
                return req.respond(0);
            }

            IterateRespStub resp;
            std::shared_ptr<arrow::RecordBatchReader> reader = reader_map[uuid];
            std::shared_ptr<arrow::RecordBatch> batch;
            reader_map[uuid]->ReadNext(&batch);

            if (batch != nullptr) {
                auto start = std::chrono::high_resolution_clock::now();
                auto buffer = PackBatch(batch);
                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                std::cout << "PackBatch: " << duration.count() << "us" << std::endl;
                resp = IterateRespStub(buffer, RPC_DONE_WITH_BATCH);   
                return req.respond(resp);             
            } else {
                resp.ret_code = RPC_DONE;
                return req.respond(resp);
            }
        };

    engine.define("init_scan", init_scan);
    engine.define("get_next_batch", get_next_batch);
    engine.define("finalize", finalize).disable_response();
    WriteToFile(engine.self(), TL_URI_PATH, false);
    std::cout << "Serving at: " << engine.self() << std::endl;
    engine.wait_for_finalize();
};
