#include <iostream>
#include <thread>
#include <chrono>
#include <fstream>

#include "headers.h"
#include "constants.h"

namespace tl = thallium;

struct ConnCtx {
    tl::engine engine;
    tl::endpoint endpoint;
};

ConnCtx Init(std::string &uri) {
    tl::engine engine("ofi+verbs", THALLIUM_SERVER_MODE, true);
    tl::endpoint endpoint = engine.lookup(uri);
    ConnCtx ctx;
    ctx.engine = engine;
    ctx.endpoint = endpoint;
    return ctx;
}

void Scan(ConnCtx &ctx, std::string &path, std::string &query) {
    tl::remote_procedure init_scan = ctx.engine.define("init_scan");
    tl::remote_procedure start_scan = ctx.engine.define("start_scan");

    std::vector<std::pair<void*,std::size_t>> segments(1);
    segments[0].first = (uint8_t*)malloc(BUFFER_SIZE);
    segments[0].second = BUFFER_SIZE;
    tl::bulk local = ctx.engine.expose(segments, tl::bulk_mode::write_only);

    std::string schema_str = init_scan.on(ctx.endpoint)(path, query);
    std::shared_ptr<arrow::Buffer> schema_buff = arrow::Buffer::Wrap(schema_str.c_str(), schema_str.size());
    arrow::ipc::DictionaryMemo dict_memo;
    arrow::io::BufferReader buff_reader(schema_buff);
    std::shared_ptr<arrow::Schema> schema = arrow::ipc::ReadSchema(&buff_reader, &dict_memo).ValueOrDie();

    std::function<void(const tl::request&, std::vector<int32_t>&, std::vector<int32_t>&, std::vector<int32_t>&, std::vector<int32_t>&, std::vector<int32_t>&, int32_t&, tl::bulk&)> do_rdma =
    [&segments, &local, &schema](const tl::request& req, std::vector<int32_t> &batch_sizes, std::vector<int32_t>& data_offsets, std::vector<int32_t>& data_sizes, std::vector<int32_t>& off_offsets, std::vector<int32_t>& off_sizes, int32_t& total_size, tl::bulk& b) {
        b(0, total_size).on(req.get_endpoint()) >> local(0, total_size);
        
        int num_cols = data_offsets.size() / batch_sizes.size();           
        for (int32_t batch_idx = 0; batch_idx < batch_sizes.size(); batch_idx++) {
            int32_t num_rows = batch_sizes[batch_idx];
            
            std::vector<std::shared_ptr<arrow::Array>> columns;
            
            for (int64_t i = 0; i < num_cols; i++) {
                int32_t magic_off = (batch_idx * num_cols) + i;
                std::shared_ptr<arrow::DataType> type = schema->field(i)->type();  
                if (is_binary_like(type->id())) {
                    std::shared_ptr<arrow::Buffer> data_buff = arrow::Buffer::Wrap(
                        (uint8_t*)segments[0].first + data_offsets[magic_off], data_sizes[magic_off]
                    );
                    std::shared_ptr<arrow::Buffer> offset_buff = arrow::Buffer::Wrap(
                        (uint8_t*)segments[0].first + off_offsets[magic_off], off_sizes[magic_off]
                    );

                    std::shared_ptr<arrow::Array> col_arr = std::make_shared<arrow::StringArray>(num_rows, std::move(offset_buff), std::move(data_buff));
                    columns.push_back(col_arr);
                } else {
                    std::shared_ptr<arrow::Buffer> data_buff = arrow::Buffer::Wrap(
                        (uint8_t*)segments[0].first  + data_offsets[magic_off], data_sizes[magic_off]
                    );
                    std::shared_ptr<arrow::Array> col_arr = std::make_shared<arrow::PrimitiveArray>(type, num_rows, std::move(data_buff));
                    columns.push_back(col_arr);
                }
            }
            auto batch = arrow::RecordBatch::Make(schema, num_rows, columns);
        }
        return req.respond(0);
    };
    ctx.engine.define("do_rdma", do_rdma);
    start_scan.on(ctx.endpoint)();
    ctx.engine.finalize();
}

arrow::Status Main(int argc, char **argv) {
    std::string uri = argv[1];
    std::string path = argv[2];
    std::string query = argv[3];

    ConnCtx ctx = Init(uri);
    Scan(ctx, path, query);

    return arrow::Status::OK();
}

int main(int argc, char** argv) {
    arrow::Status s = Main(argc, argv);
    return s.ok() ? 0 : 1;
}
