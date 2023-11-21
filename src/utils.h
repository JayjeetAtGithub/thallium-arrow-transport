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

class time_block {
public:
    time_block(std::string tag) : __start(std::chrono::high_resolution_clock::now()), _tag(tag) {}
    ~time_block() {
        std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
        auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(end - __start);
        std::cout << _tag << " : " << dur.count() << " ns" << std::endl;
    }
private:
    std::string _tag;
    std::chrono::high_resolution_clock::time_point __start;
};

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
        if (m_closed) return arrow::Status::OK();
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
        std::cout << "writing " << nbytes << " bytes\n";
        if (closed()) return arrow::Status::Invalid("Cannot write to a closed stream");
		{
            time_block t("write to archive");
            m_archive.write(reinterpret_cast<const char*>(data), nbytes);
        }
        m_written += nbytes;
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
        if (closed()) return arrow::Status::Invalid("Cannot read from a closed stream");
        m_archive.read(static_cast<char*>(out), nbytes);
        m_read += nbytes;
        return nbytes;
	}
    
	arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
        if (closed()) return arrow::Status::Invalid("Cannot read from a closed stream");
        auto buffer = arrow::AllocateResizableBuffer(nbytes).ValueOrDie();
        m_archive.read(reinterpret_cast<char*>(buffer->mutable_data()), nbytes);
        m_read += nbytes;
        buffer->Resize(nbytes, true);
        return std::shared_ptr<arrow::Buffer>(std::move(buffer));
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

        IterateRespStub() {}
        IterateRespStub(std::shared_ptr<arrow::RecordBatch> batch) : batch(batch) {}

        template<typename Archive>
        void save(Archive& ar) const {
            if (batch) {
                ThalliumOutputStreamAdaptor<Archive> output_stream{ar};
                arrow::ipc::IpcWriteOptions options;
                arrow::ipc::WriteRecordBatchStream(std::vector<std::shared_ptr<arrow::RecordBatch>>{batch}, options, &output_stream);
            }
        }

        template<typename Archive>
        void load(Archive& ar) {
            ThalliumInputStreamAdaptor<Archive> input_stream{ar};
            arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchStreamReader>> stream = 
                arrow::ipc::RecordBatchStreamReader::Open(&input_stream);
            if (!stream.ok()) {
                std::cout << "nothing sent back with RPC result" << std::endl;
                return;
            }
            auto reader = stream.ValueOrDie();
            arrow::Result<std::shared_ptr<arrow::RecordBatch>> result = reader->Next();
            batch = result.ValueOrDie();
        }
};
