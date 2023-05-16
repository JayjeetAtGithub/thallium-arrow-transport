#include <iostream>

#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/flight/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/arrow/writer.h"
#include "parquet/file_reader.h"

#include "engine.h"

class ParquetStorageService : public arrow::flight::FlightServerBase {
    public:
        explicit ParquetStorageService(
            std::shared_ptr<arrow::fs::FileSystem> fs, std::string host, int32_t port 
        ) : fs_(std::move(fs)), host_(host), port_(port) {}

        int32_t Port() { return port_; }

        arrow::Status GetFlightInfo(const arrow::flight::ServerCallContext&,
                                    const arrow::flight::FlightDescriptor& descriptor,
                                    std::unique_ptr<arrow::flight::FlightInfo>* info) {
            ARROW_ASSIGN_OR_RAISE(auto file_info, fs_->GetFileInfo(descriptor.path[0]));
            ARROW_ASSIGN_OR_RAISE(auto flight_info, MakeFlightInfo(file_info));
            *info = std::unique_ptr<arrow::flight::FlightInfo>(
                new arrow::flight::FlightInfo(std::move(flight_info)));
            return arrow::Status::OK();
        }

        arrow::Status DoGet(const arrow::flight::ServerCallContext&,
                            const arrow::flight::Ticket& request,
                            std::unique_ptr<arrow::flight::FlightDataStream>* stream) {
            std::shared_ptr<DuckDBEngine> db = std::make_shared<DuckDBEngine>();

            std::cout << "Creating table at: " << request.ticket << std::endl;
            
            db->Create(request.ticket);

            std::cout << "Created table" << std::endl;

            std::string query = "SELECT * FROM dataset WHERE total_amount > 69;";

            std::shared_ptr<arrow::RecordBatchReader> reader = db->Execute(query);

            std::cout << "Initiated execution" << std::endl;

            *stream = std::unique_ptr<arrow::flight::FlightDataStream>(
                new arrow::flight::RecordBatchStream(reader));

            std::cout << "Instantiated Flight Data Stream" << std::endl;
            return arrow::Status::OK();
        }

    private:
        arrow::Result<arrow::flight::FlightInfo> MakeFlightInfo(
            const arrow::fs::FileInfo& file_info) {
            std::shared_ptr<arrow::Schema> schema = arrow::schema({});
            std::string path = file_info.path();
            auto descriptor = arrow::flight::FlightDescriptor::Path({path});
            arrow::flight::FlightEndpoint endpoint;
            endpoint.ticket.ticket = path;
            arrow::flight::Location location;
            ARROW_RETURN_NOT_OK(
                arrow::flight::Location::ForGrpcTcp(host_, port(), &location));
            endpoint.locations.push_back(location);

            return arrow::flight::FlightInfo::Make(*schema, descriptor, {endpoint}, 0, 0);
        }

        std::shared_ptr<arrow::fs::FileSystem> fs_;
        std::string host_;
        int32_t port_;
};

int main(int argc, char *argv[]) {    
    std::string host = "10.10.1.2";
    int32_t port = 3000;
    std::string transport = "tcp+grpc";

    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();

    arrow::flight::Location server_location;
    arrow::flight::Location::ForGrpcTcp(host, port, &server_location);

    arrow::flight::FlightServerOptions options(server_location);
    auto server = std::unique_ptr<arrow::flight::FlightServerBase>(
        new ParquetStorageService(std::move(fs), host, port));
    server->Init(options);
    std::cout << "Listening on port " << server->port() << std::endl;
    server->Serve();
}
