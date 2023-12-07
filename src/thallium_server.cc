#include <iostream>
#include <csignal>

#include "utils.h"
#include "headers.h"
#include "engine.h"
#include "constants.h"

#include <chrono>

namespace tl = thallium;

int push_batch(tl::remote_procedure &do_rdma, tl::engine& engine, const tl::request& req, std::shared_ptr<arrow::RecordBatch> batch) {
    std::vector<int64_t> data_buff_sizes;
    std::vector<int64_t> offset_buff_sizes;
    int64_t num_rows = batch->num_rows();

    auto s2 = std::chrono::high_resolution_clock::now();
    std::vector<std::pair<void*,std::size_t>> segments;
    segments.reserve(batch->num_columns()*2);

    std::string null_buff = "x";

    for (int64_t i = 0; i < batch->num_columns(); i++) {
        std::shared_ptr<arrow::Array> col_arr = batch->column(i);

        int64_t data_size = 0;
        int64_t offset_size = 0;

        if (is_base_binary_like(col_arr->type_id())) {
            std::shared_ptr<arrow::Buffer> data_buff = 
                std::static_pointer_cast<arrow::BinaryArray>(col_arr)->value_data();
            std::shared_ptr<arrow::Buffer> offset_buff = 
                std::static_pointer_cast<arrow::BinaryArray>(col_arr)->value_offsets();
            
            data_size = data_buff->size();
            offset_size = offset_buff->size();
            segments.emplace_back(std::make_pair((void*)data_buff->data(), data_size));
            segments.emplace_back(std::make_pair((void*)offset_buff->data(), offset_size));
        } else {

            std::shared_ptr<arrow::Buffer> data_buff = 
                std::static_pointer_cast<arrow::PrimitiveArray>(col_arr)->values();

            data_size = data_buff->size();
            offset_size = null_buff.size(); 
            segments.emplace_back(std::make_pair((void*)data_buff->data(), data_size));
            segments.emplace_back(std::make_pair((void*)(&null_buff[0]), offset_size));
        }

        data_buff_sizes.push_back(data_size);
        offset_buff_sizes.push_back(offset_size);
    }
    auto e2 = std::chrono::high_resolution_clock::now();
    auto diff2 = std::chrono::duration_cast<std::chrono::microseconds>(e2 - s2);
    std::cout << "server.allocations: " << diff2.count() << " us" << std::endl;

    auto s1 = std::chrono::high_resolution_clock::now();
    tl::bulk arrow_bulk = engine.expose(segments, tl::bulk_mode::read_only);
    auto e1 = std::chrono::high_resolution_clock::now();
    auto diff1 = std::chrono::duration_cast<std::chrono::microseconds>(e1 - s1);
    std::cout << "server.engine.expose: " << diff1.count() << " us" << std::endl;
    return do_rdma.on(req.get_endpoint())(num_rows, data_buff_sizes, offset_buff_sizes, arrow_bulk);
}

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

    tl::remote_procedure do_rdma = engine.define("do_rdma");
    std::function<void(const tl::request&, const int&, const std::string&)> iterate = 
        [&do_rdma, &reader_map, &engine](const tl::request &req, const int& warmup, const std::string &uuid) {
            if (warmup) {
                return req.respond(0);
            }

            std::shared_ptr<arrow::RecordBatch> batch;
            auto s1 = std::chrono::high_resolution_clock::now();
            reader_map[uuid]->ReadNext(&batch);
            auto e1 = std::chrono::high_resolution_clock::now();
            // in milliseconds
            auto diff1 = std::chrono::duration_cast<std::chrono::microseconds>(e1 - s1);
            std::cout << "ReadNext: " << diff1.count() << " us" << std::endl;
            IterateRespStub resp;

            while (batch != nullptr) {
                // if (batch->num_rows() <= START_OPT_BATCH_SIZE_THRSHOLD) {
                //     auto s = std::chrono::high_resolution_clock::now();
                //     auto buffer = PackBatch(batch);
                //     auto e = std::chrono::high_resolution_clock::now();
                //     // in milliseconds
                //     auto diff = std::chrono::duration_cast<std::chrono::microseconds>(e - s);
                //     std::cout << "PackBatch: " << diff.count() << " us" << std::endl;

                //     resp = IterateRespStub(buffer, RPC_DONE_WITH_BATCH);
                //     auto s5 = std::chrono::high_resolution_clock::now();
                //     req.respond(resp);
                //     auto e5 = std::chrono::high_resolution_clock::now();
                //     auto diff5 = std::chrono::duration_cast<std::chrono::microseconds>(e5 - s5);
                //     std::cout << "respond: " << diff5.count() << " us" << std::endl;
                //     return;
                // }
                auto s4 = std::chrono::high_resolution_clock::now();
                int ret = push_batch(do_rdma, engine, req, batch);
                auto e4 = std::chrono::high_resolution_clock::now();
                auto diff4 = std::chrono::duration_cast<std::chrono::microseconds>(e4 - s4);
                std::cout << "push_batch: " << diff4.count() << " us" << std::endl;
                if (ret != 0) {
                    std::cerr << "Error: push_batch()" << std::endl;
                    exit(ret);
                }
                reader_map[uuid]->ReadNext(&batch);
            }

            resp.ret_code = RPC_DONE;
            auto s10 = std::chrono::high_resolution_clock::now();
            req.respond(resp);
            auto e10 = std::chrono::high_resolution_clock::now();
            auto diff10 = std::chrono::duration_cast<std::chrono::microseconds>(e10 - s10);
            std::cout << "respond: " << diff10.count() << " us" << std::endl;
            return;
        };

    engine.define("init_scan", init_scan);
    engine.define("iterate", iterate);
    engine.define("finalize", finalize).disable_response();
    WriteToFile(engine.self(), TL_URI_PATH, false);
    std::cout << "Serving at: " << engine.self() << std::endl;
    engine.wait_for_finalize();
};
