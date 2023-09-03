#include <iostream>
#include <time.h>

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>

#include "utils.h"
#include "constants.h"

void write_to_file(std::string data, std::string path) {
    std::ofstream file;
    file.open(path, std::ios_base::app);
    file << data;
    file.close();
}

struct ConnectionInfo {
    std::string host;
    int32_t port;
};

arrow::Result<std::unique_ptr<arrow::flight::FlightClient>> ConnectToFlightServer(ConnectionInfo info) {
    auto location = arrow::flight::Location::ForGrpcTcp(info.host, info.port).ValueOrDie();
    return arrow::flight::FlightClient::Connect(location).ValueOrDie();
}

int main(int argc, char *argv[]) {
    ConnectionInfo info;
    std::string host = "10.10.1.2";
    info.host = host;
    info.port = 3000;

    std::string path = argv[1];
    std::string query = argv[2];

    auto client = ConnectToFlightServer(info).ValueOrDie();

    std::string request = query + "@" + path;

    auto descriptor = arrow::flight::FlightDescriptor::Command(request);
    auto flight_info = client->GetFlightInfo(descriptor).ValueOrDie();
    auto stream = client->DoGet(flight_info->endpoints()[0].ticket).ValueOrDie();
    
    int64_t total_rows_read = 0;
    int64_t total_round_trips = 0;
    auto start = std::chrono::high_resolution_clock::now();
    auto chunk = stream->Next().ValueOrDie();
    while (chunk.data != nullptr) {
        total_rows_read += chunk.data->num_rows();
        total_round_trips += 1;
        stream->Next().Value(&chunk);
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::string exec_time_ms = std::to_string((double)std::chrono::duration_cast<std::chrono::microseconds>(end-start).count()/1000) + "\n";
    WriteToFile(exec_time_ms, FL_RES_PATH, true);
    
    std::cout << total_rows_read << " rows read in " << exec_time_ms << " ms and " << total_round_trips << " round trips" << std::endl;
    return 0;
}
