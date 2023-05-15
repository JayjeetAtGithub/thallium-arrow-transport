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
