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

    std::shared_ptr<arrow::RecordBatchReader> reader;
    std::function<void(const tl::request&, const std::string&, const std::string&, const std::string&)> init_scan = 
        [&reader](const tl::request &req, const std::string& path, const std::string& query, const std::string& mode) {
            std::cout << "Request: " << query << "@" << path << "@" << mode << std::endl;
            std::shared_ptr<DuckDBEngine> db = std::make_shared<DuckDBEngine>();
            db->Create(path);
            if (mode == "t") {
                reader = db->ExecuteEager(query);
            }
            else {
                reader = db->Execute(query);
            }
            std::shared_ptr<arrow::Buffer> buff = arrow::ipc::SerializeSchema(*(reader->schema())).ValueOrDie();
            return req.respond(
                std::string(reinterpret_cast<const char*>(buff->data()), static_cast<size_t>(buff->size())));
        };

    double total_time = 0;
    tl::remote_procedure do_rdma = engine.define("do_rdma");
    std::function<void(const tl::request&)> get_next_batch = 
        [&do_rdma, &reader, &engine, &total_time](const tl::request &req) {
            auto start = std::chrono::high_resolution_clock::now();
            std::shared_ptr<arrow::RecordBatch> batch;
            reader->ReadNext(&batch);

            if (batch != nullptr) {
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

                tl::bulk arrow_bulk;
                arrow_bulk = engine.expose(segments, tl::bulk_mode::read_only);
                
                int e = do_rdma.on(req.get_endpoint())(num_rows, data_buff_sizes, offset_buff_sizes, arrow_bulk);
                auto end = std::chrono::high_resolution_clock::now();
                total_time += CalcDuration(start, end);
                std::cout << "Total time: " << total_time << std::endl;
                return req.respond(e);
            } else {
                return req.respond(1);
            }
        };

    engine.define("init_scan", init_scan);
    engine.define("get_next_batch", get_next_batch);
    WriteToFile(engine.self(), TL_URI_PATH, false);
    std::cout << "Serving at: " << engine.self() << std::endl;
    engine.wait_for_finalize();
};
