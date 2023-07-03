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
#include "utils.h"


class ParquetStorageService : public arrow::flight::FlightServerBase {
    public:
        explicit ParquetStorageService(std::string host, int32_t port) : host_(host), port_(port) {}

        int32_t Port() { return port_; }

        arrow::Status GetFlightInfo(const arrow::flight::ServerCallContext&,
                                    const arrow::flight::FlightDescriptor& descriptor,
                                    std::unique_ptr<arrow::flight::FlightInfo>* info) {
            ARROW_ASSIGN_OR_RAISE(auto flight_info, MakeFlightInfo(descriptor));
            *info = std::unique_ptr<arrow::flight::FlightInfo>(
                new arrow::flight::FlightInfo(std::move(flight_info)));
            return arrow::Status::OK();
        }

        arrow::Status DoGet(const arrow::flight::ServerCallContext&,
                            const arrow::flight::Ticket& request,
                            std::unique_ptr<arrow::flight::FlightDataStream>* stream) {
            std::cout << "Request: " << request.ticket << std::endl;
            std::shared_ptr<AceroEngine> db = std::make_shared<AceroEngine>();
            std::pair<std::string, std::string> split_a = SplitString(request.ticket);
            std::pair<std::string, std::string> split_b = SplitString(split_a.second);
            db->Create(split_b.first);
            std::shared_ptr<arrow::RecordBatchReader> reader;
            if (split_b.second == "t") {
                reader = db->ExecuteEager(split_a.first);
            } else {
                reader = db->Execute(split_a.first);
            }
            *stream = std::unique_ptr<arrow::flight::FlightDataStream>(
                new arrow::flight::RecordBatchStream(reader));
            return arrow::Status::OK();
        }

    private:
        arrow::Result<arrow::flight::FlightInfo> MakeFlightInfo(
            const arrow::flight::FlightDescriptor& descriptor) {
            std::shared_ptr<arrow::Schema> schema = arrow::schema({});

            arrow::flight::FlightEndpoint endpoint;
            endpoint.ticket.ticket = descriptor.cmd;
            arrow::flight::Location location;
            ARROW_RETURN_NOT_OK(
                arrow::flight::Location::ForGrpcTcp(host_, port(), &location));
            endpoint.locations.push_back(location);

            return arrow::flight::FlightInfo::Make(*schema, descriptor, {endpoint}, 0, 0);
        }

        std::string host_;
        int32_t port_;
};

int main(int argc, char *argv[]) {    
    std::string host = "10.10.1.2";
    int32_t port = 3000;
    std::string transport = "tcp+grpc";

    arrow::flight::Location server_location;
    arrow::flight::Location::ForGrpcTcp(host, port, &server_location);

    arrow::flight::FlightServerOptions options(server_location);
    auto server = std::unique_ptr<arrow::flight::FlightServerBase>(new ParquetStorageService(host, port));
    server->Init(options);
    std::cout << "Serving at: " << server->location().ToString() << std::endl;
    server->Serve();
}
