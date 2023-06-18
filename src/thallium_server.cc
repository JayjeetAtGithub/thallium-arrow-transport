#include <iostream>

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
    std::function<void(const tl::request&, const std::string&, const std::string&, const std::string&)> init_scan = 
        [&reader_map](const tl::request &req, const std::string& path, const std::string& query, const std::string& mode) {
            std::cout << "Request: " << query << "@" << path << "@" << mode << std::endl;
            std::shared_ptr<DuckDBEngine> db = std::make_shared<DuckDBEngine>();
            db->Create(path);

            std::shared_ptr<arrow::RecordBatchReader> reader;
            if (mode == "t") {
                reader = db->ExecuteEager(query);
            }
            else {
                reader = db->Execute(query);
            }
            std::string uuid = boost::uuids::to_string(boost::uuids::random_generator()());
            reader_map[uuid] = reader;
            std::shared_ptr<arrow::Buffer> buff = arrow::ipc::SerializeSchema(*(reader->schema())).ValueOrDie();

            InitScanRespStub resp;
            resp.schema = std::string(reinterpret_cast<const char*>(buff->data()), static_cast<size_t>(buff->size()));
            resp.uuid = uuid;
            return req.respond(resp);
        };

    tl::remote_procedure do_rdma = engine.define("do_rdma");
    std::function<void(const tl::request&, const int&, const std::string&)> get_next_batch = 
        [&do_rdma, &reader_map, &engine](const tl::request &req, const int& warmup, const std::string &uuid) {
            if (warmup) {
                return req.respond(0);
            }
            
            std::shared_ptr<arrow::RecordBatch> batch;
            reader_map[uuid]->ReadNext(&batch);

            GetNextBatchRespStub resp;
            if (batch != nullptr) {

                if (batch->num_rows() < 131072) {
                    auto buffer = PackBatch(batch);
                    std::string str_buffer = 
                        std::string(reinterpret_cast<const char*>(buffer->data()), static_cast<size_t>(buffer->size()));
                    resp.buffer = str_buffer;
                    resp.ret_code = RPC_BATCH;
                    return req.respond(resp);
                }

                std::vector<int64_t> data_buff_sizes;
                std::vector<int64_t> offset_buff_sizes;
                int64_t num_rows = batch->num_rows();

                std::vector<std::pair<void*,std::size_t>> segments;
                segments.reserve(batch->num_columns()*2);

                std::string null_buff = "x";

                for (int64_t i = 0; i < batch->num_columns(); i++) {
                    std::shared_ptr<arrow::Array> col_arr = batch->column(i);
                    arrow::Type::type type = col_arr->type_id();
                    int64_t null_count = col_arr->null_count();

                    int64_t data_size = 0;
                    int64_t offset_size = 0;

                    if (is_binary_like(type)) {
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
                resp.ret_code = do_rdma.on(req.get_endpoint())(num_rows, data_buff_sizes, offset_buff_sizes, arrow_bulk);
                return req.respond(resp);
            } else {
                reader_map.erase(uuid);
                resp.ret_code = NO_BATCH;
                return req.respond(resp);
            }
        };

    engine.define("init_scan", init_scan);
    engine.define("get_next_batch", get_next_batch);
    WriteToFile(engine.self(), TL_URI_PATH, false);
    std::cout << "Serving at: " << engine.self() << std::endl;
    engine.wait_for_finalize();
};
