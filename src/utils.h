#include <mutex>
#include <deque>
#include <condition_variable>
#include <fstream>

#include <arrow/api.h>

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

std::pair<std::string, std::string> SplitRequest(std::string request) {
    std::string delimiter = "@";
    std::string path = request.substr(0, request.find(delimiter));
    std::string query = request.substr(request.find(delimiter) + 1, request.length());
    return std::make_pair(path, query);
}
