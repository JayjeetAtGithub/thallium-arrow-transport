#include <iostream>
#include <time.h>

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>

#include "utils.h"


const std::string kFlightResultPath = "/proj/schedock-PG0/flight_result";

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

  auto client = ConnectToFlightServer(info).ValueOrDie();

  std::string path = "/mnt/cephfs/dataset/*";
  std::string query = "/mnt/cephfs/dataset/*@SELECT * FROM dataset WHERE total_amount > 69;";

  auto descriptor = arrow::flight::FlightDescriptor::Command(query);
  std::unique_ptr<arrow::flight::FlightInfo> flight_info;
  client->GetFlightInfo(descriptor, &flight_info);

  std::cout << flight_info->SerializeToString().ValueOrDie() << std::endl;

  std::unique_ptr<arrow::flight::FlightStreamReader> stream;
  client->DoGet(flight_info->endpoints()[0].ticket, &stream);
  
  std::shared_ptr<arrow::Table> table;
  auto start = std::chrono::high_resolution_clock::now();
  stream->ReadAll(&table);
  auto end = std::chrono::high_resolution_clock::now();
  
  std::string exec_time_ms = std::to_string((double)std::chrono::duration_cast<std::chrono::microseconds>(end-start).count()/1000) + "\n";
  WriteToFile(exec_time_ms, kFlightResultPath);

  std::cout << "Read " << table->num_rows() << " rows and " << table->num_columns() << " columns" << std::endl;
}
