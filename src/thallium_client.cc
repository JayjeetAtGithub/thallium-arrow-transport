#include <iostream>
#include <thread>
#include <chrono>
#include <fstream>

#include "utils.h"
#include "headers.h"
#include "constants.h"

namespace tl = thallium;

class ThalliumDescriptor {
    public:
        std::string query;
        std::string path;

    static ThalliumDescriptor Create(std::string &query, std::string &path) {
        ThalliumDescriptor desc;
        desc.query = query;
        desc.path = path;
        return desc;
    }
};

class ThalliumInfo {
    public:
        std::shared_ptr<arrow::Schema> schema;
        std::string uuid;
};

class ThalliumClient {
    public:
        tl::engine engine;
        tl::endpoint endpoint;

        tl::remote_procedure init_scan;
        tl::remote_procedure iterate;
        tl::remote_procedure finalize;

        std::string uri;

        ThalliumClient(std::string &uri) : uri(uri) {}
        ~ThalliumClient() { engine.finalize(); }

        void DefineProcedures() {
            this->init_scan = engine.define("init_scan");
            this->iterate = engine.define("iterate");
            this->finalize = engine.define("finalize").disable_response();
        }

        void Connect() {
            engine = tl::engine("ofi+verbs", THALLIUM_SERVER_MODE, true);
            endpoint = engine.lookup(uri);
            this->DefineProcedures();
        }

        void GetThalliumInfo(ThalliumDescriptor &desc, ThalliumInfo &info) {
            InitScanRespStub resp = this->init_scan.on(endpoint)(desc.path, desc.query);
            std::shared_ptr<arrow::Buffer> schema_buff = arrow::Buffer::Wrap(resp.schema.c_str(), resp.schema.size());
            arrow::ipc::DictionaryMemo dict_memo;
            arrow::io::BufferReader buff_reader(schema_buff);
            std::shared_ptr<arrow::Schema> schema = arrow::ipc::ReadSchema(&buff_reader, &dict_memo).ValueOrDie();
            info.schema = schema;
            info.uuid = resp.uuid;
        }

        void Warmup() {
            std::string fake_uuid = "x";
            this->iterate.on(endpoint)(1, fake_uuid);
        }

        void Finalize() {
            this->finalize.on(endpoint)();
        }

        int Iterate(ThalliumInfo &info, int64_t &total_rows_read, int64_t &total_rpcs_made) {
            auto schema = info.schema;
            auto engine = this->engine;

            std::shared_ptr<arrow::RecordBatch> batch;
            std::function<void(const tl::request&, int64_t&, std::vector<int64_t>&, std::vector<int64_t>&, tl::bulk&)> do_rdma =
                [&schema, &batch, &engine, &total_rows_read, &total_rpcs_made](const tl::request& req, int64_t& num_rows, std::vector<int64_t>& data_buff_sizes, std::vector<int64_t>& offset_buff_sizes, tl::bulk& b) {
                    total_rpcs_made += 1;

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

                    batch = arrow::RecordBatch::Make(schema, num_rows, columns);
                    std::cout << batch->ToString() << std::endl;
                    total_rows_read += batch->num_rows();
                    return req.respond(0);
                };
            
            engine.define("do_rdma", do_rdma);
            IterateRespStub resp = this->iterate.on(endpoint)(0, info.uuid);
            if (resp.batch) {
                std::cout << resp.batch->ToString() << std::endl;
                total_rows_read += resp.batch->num_rows();
            }
            return 0;
        }
};

arrow::Status Main(int argc, char **argv) {
    std::string uri = argv[1];
    std::string path = argv[2];
    std::string query = argv[3];

    auto client = std::make_shared<ThalliumClient>(uri);
    client->Connect();

    auto desc = ThalliumDescriptor::Create(query, path);

    ThalliumInfo info;
    client->GetThalliumInfo(desc, info);

    // Do a couple of warmup blank RPC to get around libfabrics cold start
    for (int i = 0; i < 30; i++) {
        client->Warmup();
    }

    int64_t total_rows_read = 0;
    int64_t total_rpcs_made = 2;

    auto start = std::chrono::high_resolution_clock::now();
    client->Iterate(info, total_rows_read, total_rpcs_made);
    auto end = std::chrono::high_resolution_clock::now();

    std::string exec_time_ms = std::to_string((double)std::chrono::duration_cast<std::chrono::microseconds>(end-start).count()/1000) + "\n";
    WriteToFile(exec_time_ms, TL_RES_PATH, true);

    std::cout << "Total time taken (ms): " << exec_time_ms;
    std::cout << "Total rows read: " << total_rows_read << std::endl;
    std::cout << "Total messages exchanged: " << total_rpcs_made << std::endl;

    return arrow::Status::OK();
}

int main(int argc, char** argv) {
    arrow::Status s = Main(argc, argv);
    return s.ok() ? 0 : 1;
}
