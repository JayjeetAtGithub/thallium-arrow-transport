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
        std::string mode;

    static ThalliumDescriptor Create(std::string &query, std::string &path, std::string &mode) {
        ThalliumDescriptor desc;
        desc.query = query;
        desc.path = path;
        desc.mode = mode;
        return desc;
    }
};

class ThalliumInfo {
    public:
        std::shared_ptr<arrow::Schema> schema;
};

class ThalliumClient {
    public:
        tl::engine engine;
        tl::endpoint endpoint;
        std::string uri;

        ThalliumClient(std::string &uri) : uri(uri) {}
        ~ThalliumClient() { engine.finalize(); }

        void Connect() {
            engine = tl::engine("ofi+verbs", THALLIUM_SERVER_MODE, true);
            endpoint = engine.lookup(uri);
        }

        void GetThalliumInfo(ThalliumDescriptor &desc, ThalliumInfo &info) {
            tl::remote_procedure init_scan = engine.define("init_scan");
            std::string schema_str = init_scan.on(endpoint)(desc.path, desc.query, desc.mode);
            std::shared_ptr<arrow::Buffer> schema_buff = arrow::Buffer::Wrap(schema_str.c_str(), schema_str.size());
            arrow::ipc::DictionaryMemo dict_memo;
            arrow::io::BufferReader buff_reader(schema_buff);
            std::shared_ptr<arrow::Schema> schema = arrow::ipc::ReadSchema(&buff_reader, &dict_memo).ValueOrDie();
            info.schema = schema;
        }

        void Warmup() {
            tl::remote_procedure get_next_batch = this->engine.define("get_next_batch");
            get_next_batch.on(endpoint)(1);
        }

        std::shared_ptr<arrow::RecordBatch> GetNextBatch(ThalliumInfo &info) {    
            auto schema = info.schema;
            auto engine = this->engine;

            std::shared_ptr<arrow::RecordBatch> batch;
            std::function<void(const tl::request&, int64_t&, std::vector<int64_t>&, std::vector<int64_t>&, tl::bulk&)> do_rdma =
                [&schema, &batch, &engine](const tl::request& req, int64_t& num_rows, std::vector<int64_t>& data_buff_sizes, std::vector<int64_t>& offset_buff_sizes, tl::bulk& b) {
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
                        if (is_binary_like(type->id())) {
                            std::shared_ptr<arrow::Array> col_arr = std::make_shared<arrow::StringArray>(num_rows, std::move(offset_buffs[i]), std::move(data_buffs[i]));
                            columns.push_back(col_arr);
                        } else {
                            std::shared_ptr<arrow::Array> col_arr = std::make_shared<arrow::PrimitiveArray>(type, num_rows, std::move(data_buffs[i]));
                            columns.push_back(col_arr);
                        }
                    }

                    batch = arrow::RecordBatch::Make(schema, num_rows, columns);
                    return req.respond(0);
                };
            
            engine.define("do_rdma", do_rdma);
            tl::remote_procedure get_next_batch = engine.define("get_next_batch");
            int e = get_next_batch.on(endpoint)(0);
            if (e == 0) {
                return batch;
            } else {
                return nullptr;
            }
        }
};

arrow::Status Main(int argc, char **argv) {
    std::string uri = argv[1];
    std::string path = argv[2];
    std::string query = argv[3];
    std::string mode = argv[4];

    auto client = std::make_shared<ThalliumClient>(uri);
    client->Connect();

    auto desc = ThalliumDescriptor::Create(query, path, mode);

    ThalliumInfo info;
    client->GetThalliumInfo(desc, info);

    // Do a warmup blank RPC to get around libfabrics cold start
    client->Warmup();

    auto start = std::chrono::high_resolution_clock::now();
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    std::shared_ptr<arrow::RecordBatch> batch;
    while ((batch = client->GetNextBatch(info)) != nullptr) {
        batches.push_back(batch);
    }
    auto table = arrow::Table::FromRecordBatches(info.schema, batches).ValueOrDie();
    auto end = std::chrono::high_resolution_clock::now();

    std::string exec_time_ms = std::to_string((double)std::chrono::duration_cast<std::chrono::microseconds>(end-start).count()/1000) + "\n";
    WriteToFile(exec_time_ms, TL_RES_PATH, true);
    
    std::cout << table->num_rows() << "read in " << exec_time_ms << " ms" << std::endl;
    return arrow::Status::OK();
}

int main(int argc, char** argv) {
    arrow::Status s = Main(argc, argv);
    return s.ok() ? 0 : 1;
}
