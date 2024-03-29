#include <iostream>
#include <csignal>

#include "utils.h"
#include "headers.h"
#include "engine.h"
#include "constants.h"

namespace tl = thallium;

int push_batch(tl::remote_procedure &do_rdma, tl::engine& engine, const tl::request& req, std::shared_ptr<arrow::RecordBatch> batch) {
    std::vector<int64_t> data_buff_sizes;
    std::vector<int64_t> offset_buff_sizes;
    int64_t num_rows = batch->num_rows();

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

    tl::bulk arrow_bulk = engine.expose(segments, tl::bulk_mode::read_only);
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

    if (argc < 2) {
        std::cerr << "Usage: ./thallium_server <start_opt_batch_threshold>\n";
        margo_finalize(mid);
        return -1;
    }

    int64_t start_opt_batch_threshold = std::stoi(argv[1]);

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
        [&do_rdma, &reader_map, &engine, &start_opt_batch_threshold](const tl::request &req, const int& warmup, const std::string &uuid) {
            if (warmup) {
                return req.respond(0);
            }

            std::shared_ptr<arrow::RecordBatch> batch;
            reader_map[uuid]->ReadNext(&batch);
            IterateRespStub resp;

            while (batch != nullptr) {
                if (batch->num_rows() <= start_opt_batch_threshold) {
                    std::cout << "Using RPC\n";
                    auto buffer = PackBatch(batch);
                    resp = IterateRespStub(buffer, RPC_DONE_WITH_BATCH);
                    return req.respond(resp);
                }
                int ret = push_batch(do_rdma, engine, req, batch);
                if (ret != 0) {
                    std::cerr << "Error: push_batch()" << std::endl;
                    exit(ret);
                }
                reader_map[uuid]->ReadNext(&batch);
            }

            resp.ret_code = RPC_DONE;
            return req.respond(resp);
        };

    engine.define("init_scan", init_scan);
    engine.define("iterate", iterate);
    engine.define("finalize", finalize).disable_response();
    WriteToFile(engine.self(), TL_URI_PATH, false);
    std::cout << "Serving at: " << engine.self() << std::endl;
    engine.wait_for_finalize();
};
