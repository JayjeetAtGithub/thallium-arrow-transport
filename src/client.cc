#include <thallium.hpp>
#include <chrono>
#include <thallium/serialization/stl/string.hpp>
#include "utils.h"
#include "headers.h"
#include "constants.h"
#include "engine.h"

namespace tl = thallium;

std::shared_ptr<arrow::Schema> read_schema() {
    std::string query = "SELECT * FROM dataset WHERE total_amount >= 1030;";
    std::string path = "/mnt/dataset/nyc.parquet";
    std::shared_ptr<DuckDBEngine> db = std::make_shared<DuckDBEngine>();
    db->Create(path);
    std::shared_ptr<arrow::RecordBatchReader> reader = db->Execute(query);
    return reader->schema();
}

void call_init_scan_rpc(tl::remote_procedure &init_scan, tl::endpoint& endpoint, std::string &query, std::string &path) {
    init_scan.on(endpoint)(query, path, 0);
}

void call_get_data_bytes_rpc(tl::remote_procedure &get_data_bytes, tl::endpoint& endpoint) {
    for (int i = 0; i < 100; i++) {
        get_data_bytes.on(endpoint)(1);
    }
    auto start = std::chrono::high_resolution_clock::now();
    get_data_bytes.on(endpoint)(0);
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
    std::cout << "Iteration of get_data_bytes " << " took " << duration << " microseconds" << std::endl;
}

int main(int argc, char** argv) {
    if(argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <address>" << std::endl;
        exit(0);
    }

    // Read the server uri from the user
    std::string uri = argv[1];

    std::shared_ptr<arrow::Schema> schema = read_schema();

    // Define thallium client engine and lookup server endpoint
    tl::engine engine("ofi+verbs", THALLIUM_SERVER_MODE);
    tl::endpoint endpoint = engine.lookup(uri);

    // Declare the `init_scan` remote procedure
    tl::remote_procedure init_scan = engine.define("init_scan");
    
    // Declare the `get_data_bytes` remote procedure
    tl::remote_procedure get_data_bytes = engine.define("get_data_bytes");

    // Define the `do_rdma` remote procedure
    std::function<void(const tl::request&, int64_t&, std::vector<int64_t>&, std::vector<int64_t>&, tl::bulk&)> do_rdma = 
        [&engine, &schema](const tl::request &req, int64_t& num_rows, std::vector<int64_t>& data_buff_sizes, std::vector<int64_t>& offset_buff_sizes, tl::bulk &b) {
        int num_cols = schema->num_fields();
                    
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::unique_ptr<arrow::Buffer>> data_buffs(num_cols);
        std::vector<std::unique_ptr<arrow::Buffer>> offset_buffs(num_cols);
        std::vector<std::pair<void*,std::size_t>> segments;
        segments.reserve(num_cols*2);
        
        for (int64_t i = 0; i < num_cols; i++) {
            data_buffs[i] = arrow::AllocateBuffer(data_buff_sizes[i]).ValueOrDie();
            offset_buffs[i] = arrow::AllocateBuffer(offset_buff_sizes[i]).ValueOrDie();

            segments.emplace_back(std::make_pair(
                (void*)data_buffs[i]->mutable_data(),
                data_buff_sizes[i]
            ));
            segments.emplace_back(std::make_pair(
                (void*)offset_buffs[i]->mutable_data(),
                offset_buff_sizes[i]
            ));
        }

        tl::bulk local = engine.expose(segments, tl::bulk_mode::write_only);
        b.on(req.get_endpoint()) >> local;

        for (int64_t i = 0; i < num_cols; i++) {
            std::shared_ptr<arrow::DataType> type = schema->field(i)->type();  
            if (is_base_binary_like(type->id())) {
                std::shared_ptr<arrow::Array> col_arr = std::make_shared<arrow::LargeStringArray>(num_rows, std::move(offset_buffs[i]), std::move(data_buffs[i]));
                columns.push_back(col_arr);
            } else {
                std::shared_ptr<arrow::Array> col_arr = std::make_shared<arrow::PrimitiveArray>(type, num_rows, std::move(data_buffs[i]));
                columns.push_back(col_arr);
            }
        }

        std::cout << "Rebuilding batch from columns" << std::endl;
        auto batch = arrow::RecordBatch::Make(schema, num_rows, columns);
        std::cout << "Batch: " << batch->ToString() << std::endl;

        // Respond back with 0
        return req.respond(0);
    };
    // Define the `do_rdma` procedure
    engine.define("do_rdma", do_rdma);

    std::string query = "SELECT * FROM dataset WHERE total_amount >= 1030;";
    std::string path = "/mnt/dataset/nyc.1.parquet";

    // Run 1000 iterations of reading a single byte from the server
    for (int i = 0; i < 200; i++) {
        call_init_scan_rpc(init_scan, endpoint, query, path);
        call_get_data_bytes_rpc(get_data_bytes, endpoint);
    }
    return 0;
}
