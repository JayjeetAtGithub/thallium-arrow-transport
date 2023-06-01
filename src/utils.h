#include <mutex>
#include <deque>
#include <condition_variable>
#include <fstream>
#include <chrono>
#include <ctime>
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

std::pair<std::string, std::string> SplitString(std::string s) {
    std::string delimiter = "@";
    std::string part1 = s.substr(0, s.find(delimiter));
    std::string part2 = s.substr(s.find(delimiter) + 1, s.length());
    return std::make_pair(part1, part2);
}

void PrintCurrentTimestamp() {
    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
    std::time_t now_c = std::chrono::system_clock::to_time_t(now);
    std::cout << "Timestamp: " << std::ctime(&now_c) << std::endl;
}

std::string CalcDuration(std::chrono::time_point<std::chrono::system_clock> start, std::chrono::time_point<std::chrono::system_clock> end) {
    return std::to_string((double)std::chrono::duration_cast<std::chrono::microseconds>(end-start).count()/1000);
}
