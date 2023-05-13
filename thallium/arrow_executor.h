#include <iostream>

#include "rao.h"
#include "payload.h"

namespace cp = arrow::compute;
namespace ac = arrow::acero;


arrow::compute::Expression GetFilter(std::string selectivity) {
  if (selectivity == "100") {
      return arrow::compute::greater(arrow::compute::field_ref("total_amount"),
                                      arrow::compute::literal(-200));
  } else if (selectivity == "10") {
      return arrow::compute::greater(arrow::compute::field_ref("total_amount"),
                                      arrow::compute::literal(27));
  } else if (selectivity == "1") {
      return arrow::compute::greater(arrow::compute::field_ref("total_amount"),
                                      arrow::compute::literal(69));
  } else {
    return arrow::compute::literal(true);
  }
}

arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> ScanDataset(cp::ExecContext& exec_context, const ScanReqRPCStub& stub, std::string backend, std::string selectivity) {
    std::string uri = "file:///mnt/cephfs/dataset";

    auto schema = arrow::schema({
      arrow::field("VendorID", arrow::int64()),
      arrow::field("tpep_pickup_datetime", arrow::timestamp(arrow::TimeUnit::MICRO)),
      arrow::field("tpep_dropoff_datetime", arrow::timestamp(arrow::TimeUnit::MICRO)),
      arrow::field("passenger_count", arrow::int64()),
      arrow::field("trip_distance", arrow::float64()),
      arrow::field("RatecodeID", arrow::int64()),
      arrow::field("store_and_fwd_flag", arrow::utf8()),
      arrow::field("PULocationID", arrow::int64()),
      arrow::field("DOLocationID", arrow::int64()),
      arrow::field("payment_type", arrow::int64()),
      arrow::field("fare_amount", arrow::float64()),
      arrow::field("extra", arrow::float64()),
      arrow::field("mta_tax", arrow::float64()),
      arrow::field("tip_amount", arrow::float64()),
      arrow::field("tolls_amount", arrow::float64()),
      arrow::field("improvement_surcharge", arrow::float64()),
      arrow::field("total_amount", arrow::float64())
    });
    
    std::string path;
    ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(uri, &path)); 
    auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
      
    arrow::fs::FileSelector s;
    s.base_dir = std::move(path);
    s.recursive = true;

    arrow::dataset::FileSystemFactoryOptions options;
    ARROW_ASSIGN_OR_RAISE(auto factory, 
      arrow::dataset::FileSystemDatasetFactory::Make(std::move(fs), s, std::move(format), options));
    arrow::dataset::FinishOptions finish_options;
    ARROW_ASSIGN_OR_RAISE(auto dataset,factory->Finish(finish_options));

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ac::ExecPlan> plan,
                          ac::ExecPlan::Make(&exec_context));

    ARROW_ASSIGN_OR_RAISE(auto scanner_builder, dataset->NewScan());
    ARROW_RETURN_NOT_OK(scanner_builder->Filter(GetFilter(selectivity)));
    ARROW_RETURN_NOT_OK(scanner_builder->UseThreads(true));
    ARROW_RETURN_NOT_OK(scanner_builder->Project(schema->field_names()));
    ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());

    std::shared_ptr<arrow::RecordBatchReader> reader; 
    if (backend == "dataset") {
      ARROW_ASSIGN_OR_RAISE(reader, scanner->ToRecordBatchReader());
    } else if (backend == "dataset+mem") {
      ARROW_ASSIGN_OR_RAISE(auto table, scanner->ToTable())
      auto im_ds = std::make_shared<arrow::dataset::InMemoryDataset>(table);
      ARROW_ASSIGN_OR_RAISE(auto im_ds_scanner_builder, im_ds->NewScan());
      ARROW_ASSIGN_OR_RAISE(auto im_ds_scanner, im_ds_scanner_builder->Finish());
      ARROW_ASSIGN_OR_RAISE(reader, im_ds_scanner->ToRecordBatchReader());
    }

    return reader;
}
