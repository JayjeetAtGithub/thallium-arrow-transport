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
        tl::remote_procedure get_next_batch;
        tl::remote_procedure finalize;

        std::string uri;

        ThalliumClient(std::string &uri) : uri(uri) {}
        ~ThalliumClient() { engine.finalize(); }

        void DefineProcedures() {
            this->init_scan = engine.define("init_scan");
            this->get_next_batch = engine.define("get_next_batch");
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
            std::cout << "Num cols: " << schema->field_names().size() << std::endl;
            info.schema = schema;
            info.uuid = resp.uuid;
        }

        void Warmup() {
            std::string fake_uuid = "x";
            this->get_next_batch.on(endpoint)(1, fake_uuid);
        }

        void Finalize() {
            this->finalize.on(endpoint)();
        }

        int GetNextBatch(ThalliumInfo &info, int64_t &total_rows_read, int64_t &total_rpcs_made) {
            auto schema = info.schema;

            IterateRespStub resp = this->get_next_batch.on(endpoint)(0, info.uuid);
            total_rpcs_made += 1;
            if (resp.ret_code == RPC_DONE_WITH_BATCH) {
                auto start = std::chrono::high_resolution_clock::now();
                std::shared_ptr<arrow::RecordBatch> batch = UnpackBatch(resp.buffer, schema);
                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end-start);
                std::cout << "UnpackBatch: " << duration.count() << "us" << std::endl;
                total_rows_read += batch->num_rows();
                return 0;
            } else {
                return -1;
            }
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
    while (true) {
        auto start = std::chrono::high_resolution_clock::now();
        int ret = client->GetNextBatch(info, total_rows_read, total_rpcs_made);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end-start);
        std::cout << "GetNextBatch: " << duration.count() << "us" << std::endl;
        if (ret == -1) {
            break;
        }
    }
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
