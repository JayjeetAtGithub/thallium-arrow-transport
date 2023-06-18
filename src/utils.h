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


std::string PrintTimestamp() {
  const auto now = std::chrono::system_clock::now();
  const auto nowAsTimeT = std::chrono::system_clock::to_time_t(now);
  const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
      now.time_since_epoch()) % 1000;
  std::stringstream nowSs;
  nowSs
      << std::put_time(std::localtime(&nowAsTimeT), "%a %b %d %Y %T")
      << '.' << std::setfill('0') << std::setw(3) << nowMs.count();
  return nowSs.str();
}

class ConcurrentRecordBatchQueue {
    public:
        std::deque<std::shared_ptr<arrow::RecordBatch>> queue;
        std::mutex mutex;
        std::condition_variable cv;
    
        void push_back(std::shared_ptr<arrow::RecordBatch> batch) {
            std::unique_lock<std::mutex> lock(mutex);
            queue.push_back(batch);
            lock.unlock();
            cv.notify_one();
        }

        void wait_n_pop(std::shared_ptr<arrow::RecordBatch> &batch) {
            std::unique_lock<std::mutex> lock(mutex);
            while (queue.empty()) {
                cv.wait(lock);
            }
            batch = queue.front();
            queue.pop_front();
        }

        void clear() {
            std::unique_lock<std::mutex> lock(mutex);
            queue.clear();
        }
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

struct ScanThreadContext {
    std::shared_ptr<ConcurrentRecordBatchQueue> cq;
    std::shared_ptr<arrow::RecordBatchReader> reader;
};

std::pair<std::string, std::string> SplitString(std::string s) {
    std::string delimiter = "@";
    std::string part1 = s.substr(0, s.find(delimiter));
    std::string part2 = s.substr(s.find(delimiter) + 1, s.length());
    return std::make_pair(part1, part2);
}

double CalcDuration(std::chrono::time_point<std::chrono::system_clock> start, std::chrono::time_point<std::chrono::system_clock> end) {
    return ((double)std::chrono::duration_cast<std::chrono::microseconds>(end-start).count())/1000;
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


std::shared_ptr<arrow::Buffer> SerializeBatch(std::shared_ptr<arrow::RecordBatch> batch) {
    arrow::ipc::IpcWriteOptions options;
    return arrow::ipc::SerializeRecordBatch(*batch, options).ValueOrDie();
}

std::shared_ptr<arrow::RecordBatch> DeserializeBatch(std::shared_ptr<arrow::Buffer> buffer, std::shared_ptr<arrow::Schema> schema) {
    std::shared_ptr<arrow::RecordBatch> batch;
    arrow::io::BufferReader buffer_reader(buffer);
    arrow::DictionaryMemo dictionary_memo;
    arrow::ipc::IpcReadOptions read_options;
    return ReadRecordBatch(schema, &dictionary_memo, read_options, &buf_reader).ValueOrDie();
}
