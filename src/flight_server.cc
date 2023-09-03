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

        arrow::Result<arrow::flight::FlightInfo> GetFlightInfo(const arrow::flight::ServerCallContext&,
                                    const arrow::flight::FlightDescriptor& descriptor) {
            return MakeFlightInfo(descriptor);
        }

        arrow::Status DoGet(const arrow::flight::ServerCallContext&,
                            const arrow::flight::Ticket& request,
                            std::unique_ptr<arrow::flight::FlightDataStream>* stream) {
            std::cout << "Request: " << request.ticket << std::endl;
            std::shared_ptr<AceroEngine> db = std::make_shared<AceroEngine>();
            std::pair<std::string, std::string> tuple = SplitString(request.ticket);
            db->Create(tuple.second);
            std::shared_ptr<arrow::RecordBatchReader> reader = db->Execute(tuple.first);
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
            auto location = arrow::flight::Location::ForGrpcTcp(host_, port()).ValueOrDie();
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

    auto server_location = arrow::flight::Location::ForGrpcTcp(host, port).ValueOrDie();
    arrow::flight::FlightServerOptions options(server_location);
    auto server = std::unique_ptr<arrow::flight::FlightServerBase>(new ParquetStorageService(host, port));
    server->Init(options);
    std::cout << "Serving at: " << server->location().ToString() << std::endl;
    server->Serve();
}
