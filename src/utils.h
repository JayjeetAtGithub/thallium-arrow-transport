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

#include "constants.h"

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

template<typename Archive>
struct ThalliumOutputStreamAdaptor : public arrow::io::OutputStream {
	ThalliumOutputStreamAdaptor(Archive& ar)
	: m_archive(ar) {}

	arrow::Status Close() override {
	    m_closed = true;
		return arrow::Status::OK();
	}
	
	arrow::Result<int64_t> Tell() const override {
		return m_written;
	}
	
	bool closed() const override {
		return m_closed;
	}
	
	arrow::Status Write(const void* data, int64_t nbytes) override {
		m_written += nbytes;
        std::cout << "writing " << nbytes << " bytes into archive\n";
		m_archive.write(static_cast<const char*>(data), static_cast<size_t>(nbytes));
		return arrow::Status::OK();
	}
	
	Archive& m_archive;
	int64_t  m_written = 0;
	bool     m_closed = false;
};

template<typename Archive>
struct ThalliumInputStreamAdaptor : public arrow::io::InputStream {

	ThalliumInputStreamAdaptor(Archive& ar)
	: m_archive(ar) {}

	arrow::Status Close() override {
		m_closed = true;
		return arrow::Status::OK();
	}
	
	arrow::Result<int64_t> Tell() const override {
		return m_read;
	}
	
	bool closed() const override {
		return m_closed;
	}
	
	arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
		auto buffer = Read(nbytes).ValueOrDie();
        out = reinterpret_cast<void*>(buffer->mutable_data());
        return nbytes;
	}
	
	arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
        std::shared_ptr<arrow::Buffer> buffer = arrow::AllocateBuffer(nbytes).ValueOrDie();
        m_archive.read(reinterpret_cast<char*>(buffer->mutable_data()), static_cast<size_t>(nbytes));
        m_read += nbytes;
        return buffer;
	}
	
	bool supports_zero_copy() const override {
		return false;
	}
	
	Archive& m_archive;
	int64_t  m_read = 0;
	bool     m_closed = false;

};

class IterateRespStub {
    public:
        std::shared_ptr<arrow::RecordBatch> batch;
        int ret_code;

        IterateRespStub() {}
        IterateRespStub(std::shared_ptr<arrow::RecordBatch> batch, int ret_code) : batch(batch), ret_code(ret_code) {}

        template<typename Archive>
        void save(Archive& ar) const {
            if (ret_code == RPC_DONE_WITH_BATCH) {
                ThalliumOutputStreamAdaptor<Archive> output_stream{ar};
                arrow::ipc::IpcWriteOptions options;
                arrow::ipc::SerializeRecordBatch(*batch, options, &output_stream);
            }
            ar & ret_code;
        }

        template<typename Archive>
        void load(Archive& ar) {
            if (ret_code == RPC_DONE_WITH_BATCH) {
                ThalliumInputStreamAdaptor<Archive> input_stream{ar};
                arrow::ipc::DictionaryMemo dict_memo;
                arrow::ipc::IpcReadOptions options;
                auto schema = arrow::ipc::ReadSchema(&input_stream, &dict_memo).ValueOrDie();
                auto result = arrow::ipc::ReadRecordBatch(schema, &dict_memo, options,  &input_stream).ValueOrDie();
                batch = std::move(result);
                std::cout << "batch->num_rows() = " << batch->num_rows() << std::endl;
            }
            ar & ret_code;
        }
};
