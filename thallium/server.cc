#include <iostream>

#include "utils.h"
#include "headers.h"
#include "engine.h"

namespace tl = thallium;

const int32_t kTransferSize = 19 * 1024 * 1024;
const int32_t kBatchSize = 1 << 17;
const std::string kThalliumResultPath = "/proj/schedock-PG0/thallium_result";
const std::string kThalliumUriPath = "/proj/schedock-PG0/thallium_uri";


void scan_handler(void *arg) {
    ScanThreadContext *ctx = (ScanThreadContext*)arg;
    std::shared_ptr<arrow::RecordBatch> batch;
    auto s = ctx->reader->ReadNext(&batch);
    while (batch != nullptr) {
        ctx->cq->push_back(batch);
        s = ctx->reader->ReadNext(&batch);
    }    
    ctx->cq->push_back(nullptr);
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

    tl::remote_procedure do_rdma = engine.define("do_rdma");    
    tl::managed<tl::pool> new_pool = tl::pool::create(tl::pool::access::spmc);
    tl::managed<tl::xstream> xstream = 
        tl::xstream::create(tl::scheduler::predef::deflt, *new_pool);
    
    std::function<void(const tl::request&, const std::string&, const std::string&)> scan = 
        [&xstream, &engine, &do_rdma](const tl::request &req, const std::string &path, const std::string& query) {
            std::cout << "Request: " << path << "@" << query << std::endl;
            std::shared_ptr<DuckDBEngine> db = std::make_shared<DuckDBEngine>();
            db->Create(path);
            std::shared_ptr<arrow::RecordBatchReader> reader = db->Execute(query);

            auto start = std::chrono::high_resolution_clock::now();
            
            bool finished = false;
            std::vector<std::pair<void*,std::size_t>> segments(1);
            uint8_t* segment_buffer = (uint8_t*)malloc(kTransferSize);
            segments[0].first = (void*)segment_buffer;
            segments[0].second = kTransferSize;
            tl::bulk arrow_bulk = engine.expose(segments, tl::bulk_mode::read_write);
            
            std::shared_ptr<ScanThreadContext> ctx = std::make_shared<ScanThreadContext>();
            std::shared_ptr<ConcurrentRecordBatchQueue> cq = std::make_shared<ConcurrentRecordBatchQueue>();
            ctx->cq = cq;
            ctx->reader = reader;

            xstream->make_thread([&]() {
                scan_handler((void*)ctx.get());
            }, tl::anonymous());

            std::shared_ptr<arrow::RecordBatch> new_batch;

            while (1 && !finished) {
                std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
                std::vector<int32_t> batch_sizes;
                int64_t rows_processed = 0;

                while (rows_processed < kBatchSize) {
                    cq->wait_n_pop(new_batch);
                    if (new_batch == nullptr) {
                        finished = true;
                        break;
                    }

                    rows_processed += new_batch->num_rows();

                    batches.push_back(new_batch);
                    batch_sizes.push_back(new_batch->num_rows());
                } 

                if (batches.size() != 0) {
                    int32_t curr_pos = 0;
                    int32_t total_size = 0;

                    std::vector<int32_t> data_offsets;
                    std::vector<int32_t> data_sizes;

                    std::vector<int32_t> off_offsets;
                    std::vector<int32_t> off_sizes;

                    std::string null_buff = "x";
                    
                    for (auto b : batches) {
                        for (int32_t i = 0; i < b->num_columns(); i++) {
                            std::shared_ptr<arrow::Array> col_arr = b->column(i);
                            arrow::Type::type type = col_arr->type_id();
                            int32_t null_count = col_arr->null_count();

                            if (is_binary_like(type)) {
                                std::shared_ptr<arrow::Buffer> data_buff = 
                                    std::static_pointer_cast<arrow::BinaryArray>(col_arr)->value_data();
                                std::shared_ptr<arrow::Buffer> offset_buff = 
                                    std::static_pointer_cast<arrow::BinaryArray>(col_arr)->value_offsets();

                                int32_t data_size = data_buff->size();
                                int32_t offset_size = offset_buff->size();

                                data_offsets.emplace_back(curr_pos);
                                data_sizes.emplace_back(data_size); 
                                memcpy(segment_buffer + curr_pos, data_buff->data(), data_size);
                                curr_pos += data_size;

                                off_offsets.emplace_back(curr_pos);
                                off_sizes.emplace_back(offset_size);
                                memcpy(segment_buffer + curr_pos, offset_buff->data(), offset_size);
                                curr_pos += offset_size;

                                total_size += (data_size + offset_size);
                            } else {
                                std::shared_ptr<arrow::Buffer> data_buff = 
                                    std::static_pointer_cast<arrow::PrimitiveArray>(col_arr)->values();

                                int32_t data_size = data_buff->size();
                                int32_t offset_size = null_buff.size(); 

                                data_offsets.emplace_back(curr_pos);
                                data_sizes.emplace_back(data_size);
                                memcpy(segment_buffer + curr_pos, data_buff->data(), data_size);
                                curr_pos += data_size;

                                off_offsets.emplace_back(curr_pos);
                                off_sizes.emplace_back(offset_size);
                                memcpy(segment_buffer + curr_pos, (uint8_t*)null_buff.c_str(), offset_size);
                                curr_pos += offset_size;

                                total_size += (data_size + offset_size);
                            }
                        }
                    }

                    segments[0].second = total_size;
                    do_rdma.on(req.get_endpoint())(batch_sizes, data_offsets, data_sizes, off_offsets, off_sizes, total_size, arrow_bulk);
                }
            }

            auto end = std::chrono::high_resolution_clock::now();
            std::string exec_time_ms = std::to_string((double)std::chrono::duration_cast<std::chrono::microseconds>(end-start).count()/1000) + "\n";
            WriteToFile(exec_time_ms, kThalliumResultPath, true);
            return req.respond(0);
        };
    
    engine.define("scan", scan);
    WriteToFile(engine.self(), kThalliumUriPath, false);
    std::cout << "Server running at address " << engine.self() << std::endl;
    engine.wait_for_finalize();
};
