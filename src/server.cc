#include <iostream>
#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

#include "utils.h"
#include "headers.h"
#include "constants.h"
#include "engine.h"

namespace tl = thallium;


int main(int argc, char** argv) {
    // Define the thallium server
    tl::engine engine("ofi+verbs", THALLIUM_SERVER_MODE);

    // Declare the `do_rdma` remote procedure
    tl::remote_procedure do_rdma = engine.define("do_rdma");

    std::unordered_map<int, std::shared_ptr<arrow::RecordBatchReader>> reader_map;

    // Define the `init_scan` procedure
    // This procedure reads out a single batch from the result iterator
    std::function<void(const tl::request&, const std::string&, const std::string&, const int64_t&)> init_scan = 
        [&reader_map](const tl::request &req, const std::string& query, const std::string& path, const int64_t& warmup) {
            if (warmup == 1) {
                return req.respond(0);
            }
            std::shared_ptr<DuckDBEngine> db = std::make_shared<DuckDBEngine>();
            db->Create(path);
            std::shared_ptr<arrow::RecordBatchReader> reader = db->Execute(query);
            // std::shared_ptr<arrow::RecordBatch> batch;
            // reader->ReadNext(&batch);
            reader_map[0] = reader;
            return req.respond(0);
    };

    // Define the `get_data_bytes` procedure
    std::function<void(const tl::request&, const int&)> get_data_bytes = 
    [&do_rdma, &engine, &reader_map](const tl::request &req, const int& warmup) {
        // If warmup, then just return
        if (warmup == 1) {
            return req.respond(0);
        }

        auto reader = reader_map[0];
        std::shared_ptr<arrow::RecordBatch> batch;
        
        auto s1 = std::chrono::high_resolution_clock::now();
        reader->ReadNext(&batch);
        auto e1 = std::chrono::high_resolution_clock::now();
        auto d1 = std::chrono::duration_cast<std::chrono::microseconds>(e1-s1).count();
        std::cout << "ReadNext: " << d1 << std::endl;

        if (batch != nullptr) {
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

            // Expose the segment and send it as argument to `do_rdma`
            auto s = std::chrono::high_resolution_clock::now();
            tl::bulk bulk = engine.expose(segments, tl::bulk_mode::read_only);
            auto e = std::chrono::high_resolution_clock::now();
            auto d = std::chrono::duration_cast<std::chrono::microseconds>(e-s).count();
            std::cout << "serverexpose: " << d << std::endl;
            do_rdma.on(req.get_endpoint())(num_rows, data_buff_sizes, offset_buff_sizes, bulk);
        } else {
            std::cout << "No more batches" << std::endl;
        }

        // Respond back with 0
        return req.respond(0);
    };

    // Define the `init_scan` procedure
    engine.define("init_scan", init_scan);

    // Define the `get_data_bytes` procedure
    engine.define("get_data_bytes", get_data_bytes);

    // Write the server uri to a file
    WriteToFile(engine.self(), TL_URI_PATH, false);
    std::cout << "Server running at address " << engine.self() << std::endl;
    return 0;
}
