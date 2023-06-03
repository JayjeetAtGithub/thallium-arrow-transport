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
    arrow::flight::Location location;
    ARROW_RETURN_NOT_OK(
      arrow::flight::Location::ForGrpcTcp(info.host, info.port, &location));

    std::unique_ptr<arrow::flight::FlightClient> client;
    ARROW_RETURN_NOT_OK(arrow::flight::FlightClient::Connect(location, &client));
    return client;
}

int main(int argc, char *argv[]) {
    ConnectionInfo info;
    std::string host = "10.10.1.2";
    info.host = host;
    info.port = 3000;

    std::string path = argv[1];
    std::string query = argv[2];
    std::string mode = argv[3];

    auto client = ConnectToFlightServer(info).ValueOrDie();

    std::string request = query + "@" + path + "@" + mode;

    auto descriptor = arrow::flight::FlightDescriptor::Command(request);
    std::unique_ptr<arrow::flight::FlightInfo> flight_info;
    client->GetFlightInfo(descriptor, &flight_info);

    std::unique_ptr<arrow::flight::FlightStreamReader> stream;
    client->DoGet(flight_info->endpoints()[0].ticket, &stream);
    
    std::shared_ptr<arrow::Table> table;
    auto start = std::chrono::high_resolution_clock::now();
    stream->ReadAll(&table);
    auto end = std::chrono::high_resolution_clock::now();

    std::cout << table->ToString() << std::endl;
    
    std::string exec_time_ms = std::to_string((double)std::chrono::duration_cast<std::chrono::microseconds>(end-start).count()/1000) + "\n";
    WriteToFile(exec_time_ms, FL_RES_PATH, true);
    
    std::cout << "Time taken (ms): " << exec_time_ms << std::endl;
    return 0;
}
