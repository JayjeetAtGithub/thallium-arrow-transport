#include "crbq.h"
#include "headers.h"
#include <thallium.hpp>

#include "executor.h"


namespace tl = thallium;
namespace cp = arrow::compute;

const int32_t kTransferSize = 19 * 1024 * 1024;
const int32_t kBatchSize = 1 << 17;

class ThalliumTransportService {
    private:
        tl::engine _engine;
        std::string _backend;
        std::string _selectivity;
        ConcurrentRecordBatchQueue _cq;

    public:
        ThalliumTransportService(std::string backend, std::string selectivity) {
            _backend = backend;
            _selectivity = selectivity;
        }

        void _scan_handler(void *arg) {
            arrow::RecordBatchReader *reader = (arrow::RecordBatchReader*)arg;
            std::shared_ptr<arrow::RecordBatch> batch;
            reader->ReadNext(&batch);
            while (batch != nullptr) {
                _cq.push_back(batch);
                reader->ReadNext(&batch);
            }    
            _cq.push_back(nullptr);
        }

        void Init() {
            // initiate the thallium engine
            _engine = tl::engine("ofi+verbs", THALLIUM_SERVER_MODE, true);

            // get the uri of the thallium instance
            margo_instance_id mid = _engine.get_margo_instance();
            hg_addr_t svr_addr;
            hg_return_t hret = margo_addr_self(mid, &svr_addr);
            if (hret != HG_SUCCESS) {
                std::cerr << "Error: margo_addr_lookup()\n";
                margo_finalize(mid);
                exit(1);
            }

            // define the client procedure to rdma data from the server
            tl::remote_procedure do_rdma = _engine.define("do_rdma");

            // create a new thallium pool for the scanner to keep scanning batches in background
            tl::managed<tl::pool> scanner_pool = tl::pool::create(tl::pool::access::spmc);
            tl::managed<tl::xstream> xstream = 
                tl::xstream::create(tl::scheduler::predef::deflt, *scanner_pool);

            // define the scan procedure
            std::function<void(const tl::request&, const ScanReqRPCStub&)> scan = 
                [&xstream, &do_rdma, this](const tl::request &req, const ScanReqRPCStub& stub) {
                    arrow::dataset::internal::Initialize();
                    cp::ExecContext exec_ctx;
                    std::shared_ptr<arrow::RecordBatchReader> reader = ScanDataset(exec_ctx, stub, _backend, _selectivity).ValueOrDie();
                    
                    bool finished = false;
                    std::vector<std::pair<void*,std::size_t>> segments(1);
                    uint8_t* segment_buffer = (uint8_t*)malloc(kTransferSize);
                    segments[0].first = (void*)segment_buffer;
                    segments[0].second = kTransferSize;
                    tl::bulk arrow_bulk = _engine.expose(segments, tl::bulk_mode::read_write);
                    _cq.clear();

                    xstream->make_thread([&]() {
                        _scan_handler((void*)reader.get());
                    }, tl::anonymous());

                    std::shared_ptr<arrow::RecordBatch> new_batch;

                    while (1 && !finished) {
                        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
                        std::vector<int32_t> batch_sizes;
                        int64_t rows_processed = 0;

                        while (rows_processed < kBatchSize) {
                            _cq.wait_n_pop(new_batch);
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
                    return req.respond(0);
                };
            _engine.define("scan", scan);
        };

        std::string uri() {
            return _engine.self();
        }

        void Serve() {
            _engine.wait_for_finalize();
        }
};
