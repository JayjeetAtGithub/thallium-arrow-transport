#include <mutex>
#include <deque>
#include <condition_variable>
#include <fstream>
#include <chrono>
#include <iomanip>
#include <ctime>
#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>


void WriteToFile(std::string data, std::string path, bool append) {
    std::ofstream file;
    if (append) {
        file.open(path, std::ios_base::app);
    } else {
        file.open(path);
    }
    file << data;
    file.close();
}

std::pair<std::string, std::string> SplitString(std::string s) {
    std::string delimiter = "@";
    std::string part1 = s.substr(0, s.find(delimiter));
    std::string part2 = s.substr(s.find(delimiter) + 1, s.length());
    return std::make_pair(part1, part2);
}

class InitScanRespStub {
    public:
        std::string schema;
        std::string uuid;

        InitScanRespStub() {}
        InitScanRespStub(std::string schema, std::string uuid) : schema(schema), uuid(uuid) {}

        template<typename A>
        void save(A& ar) const {
            ar & schema;
            ar & uuid;
        }

        template<typename A>
        void load(A& ar) {
            ar & schema;
            ar & uuid;
        }
};

class IterateRespStub {
    public:
        std::shared_ptr<arrow::Buffer> buffer;
        int ret_code;

        IterateRespStub() {}
        IterateRespStub(std::shared_ptr<arrow::Buffer> buffer, int ret_code) : buffer(buffer), ret_code(ret_code) {}

        template<typename A>
        void save(A& ar) const {
            size_t size = buffer->size();
            ar & size;
            ar.write(buffer->data(), size);
            ar & ret_code;
        }

        template<typename A>
        void load(A& ar) {
            size_t size;
            ar & size;
            buffer = arrow::AllocateBuffer(size).ValueOrDie();
            ar.read(buffer->mutable_data(), size);
            ar & ret_code;
        }
};

std::shared_ptr<arrow::Buffer> PackBatch(std::shared_ptr<arrow::RecordBatch> batch) {
    arrow::ipc::IpcWriteOptions options;
    return arrow::ipc::SerializeRecordBatch(*batch, options).ValueOrDie();
}

std::shared_ptr<arrow::RecordBatch> UnpackBatch(std::shared_ptr<arrow::Buffer> buff, std::shared_ptr<arrow::Schema> schema) {
    arrow::io::BufferReader buff_reader(buff);
    arrow::ipc::DictionaryMemo dictionary_memo;
    arrow::ipc::IpcReadOptions read_options;
    return ReadRecordBatch(schema, &dictionary_memo, read_options, &buff_reader).ValueOrDie();
}
