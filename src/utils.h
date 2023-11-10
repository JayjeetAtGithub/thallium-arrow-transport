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
            size_t size = (buffer == nullptr) ? 0 : buffer->size();
            ar & size;
            if (size != 0) {
                ar.write(buffer->data(), size);

            }
            ar & ret_code;
        }

        template<typename A>
        void load(A& ar) {
            size_t size;
            ar & size;
            if (size != 0) {
                buffer = arrow::AllocateBuffer(size).ValueOrDie();
                ar.read(buffer->mutable_data(), size);
            }
            ar & ret_code;
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
	
	Result<int64_t> Tell() const override {
		return m_written;
	}
	
	bool closed() const override {
		return m_closed;
	}
	
	arrow::Status Write(const void* data, int64_t nbytes) override {
		m_written += nbytes;
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
	
	Result<int64_t> Tell() const override {
		return m_read;
	}
	
	bool closed() const override {
		return m_closed;
	}
	
	Result<int64_t> Read(int64_t nbytes, void* out) override {
		m_archive.read(static_cast<const char*>(data), static_cast<size_t>(nbytes));
        m_read += nbytes;
		return nbytes; // not sure this is the way to return a Result<T>
	}
	
	Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) override {
		// I *think* we should be able to just do that, since zero-copy is
		// not supported by out InputStream, it should not be called...
		return nullptr;
	}
	
	bool supports_zero_copy() const override {
		return false;
	}
	
	Archive& m_archive;
	int64_t  m_read = 0;
	bool     m_closed = false;

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
