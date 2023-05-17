#include <iostream>
#include <thread>
#include <chrono>
#include <fstream>

#include "headers.h"

namespace tl = thallium;


auto schema = arrow::schema({
        arrow::field("VendorID", arrow::int64()),
        arrow::field("tpep_pickup_datetime", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("tpep_dropoff_datetime", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("passenger_count", arrow::int64()),
        arrow::field("trip_distance", arrow::float64()),
        arrow::field("RatecodeID", arrow::int64()),
        arrow::field("store_and_fwd_flag", arrow::utf8()),
        arrow::field("PULocationID", arrow::int64()),
        arrow::field("DOLocationID", arrow::int64()),
        arrow::field("payment_type", arrow::int64()),
        arrow::field("fare_amount", arrow::float64()),
        arrow::field("extra", arrow::float64()),
        arrow::field("mta_tax", arrow::float64()),
        arrow::field("tip_amount", arrow::float64()),
        arrow::field("tolls_amount", arrow::float64()),
        arrow::field("improvement_surcharge", arrow::float64()),
        arrow::field("total_amount", arrow::float64())
    });


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
    tl::remote_procedure scan = ctx.engine.define("scan");
    int32_t kTransferSize = 19 * 1024 * 1024;
    std::vector<std::pair<void*,std::size_t>> segments(1);
    segments[0].first = (uint8_t*)malloc(kTransferSize);
    segments[0].second = kTransferSize;
    tl::bulk local = ctx.engine.expose(segments, tl::bulk_mode::write_only);

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    std::function<void(const tl::request&, std::vector<int32_t>&, std::vector<int32_t>&, std::vector<int32_t>&, std::vector<int32_t>&, std::vector<int32_t>&, int32_t&, tl::bulk&)> do_rdma =
    [&batches, &segments, &local, &schema](const tl::request& req, std::vector<int32_t> &batch_sizes, std::vector<int32_t>& data_offsets, std::vector<int32_t>& data_sizes, std::vector<int32_t>& off_offsets, std::vector<int32_t>& off_sizes, int32_t& total_size, tl::bulk& b) {
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
            batches.push_back(batch);
        }
        return req.respond(0);
    };

    engine.define("do_rdma", do_rdma);
    scan.on(ctx.endpoint)(path, query);
    ctx.engine.finalize();
    return arrow::Table::FromRecordBatches(schema, batches);
}


arrow::Status Main(int argc, char **argv) {
    if (argc < 2) {
        std::cout << "./tc [uri] [path] [query]" << std::endl;
        exit(1);
    }

    std::string uri = argv[1];
    std::string path = argv[2];
    std::string query = argv[3];

    ConnCtx ctx = Init(uri);
    Scan(ctx, path, query);

    return arrow::Status::OK();
}

int main(int argc, char** argv) {
    Main(argc, argv);
}
