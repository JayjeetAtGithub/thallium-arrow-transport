cmake_minimum_required(VERSION 3.2)

find_package(thallium REQUIRED)
find_package(Arrow REQUIRED)
find_package(Boost REQUIRED)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(c client.cc)
target_link_libraries(c thallium arrow arrow_dataset duckdb)

add_executable(s server.cc)
target_link_libraries(s thallium arrow arrow_dataset duckdb)

add_executable(tc thallium_client.cc)
target_link_libraries(tc thallium arrow arrow_dataset duckdb)

add_executable(ts thallium_server.cc)
target_link_libraries(ts thallium arrow arrow_dataset duckdb)

add_executable(fc flight_client.cc)
target_link_libraries(fc arrow arrow_dataset arrow_flight parquet duckdb)

add_executable(fs flight_server.cc)
target_link_libraries(fs arrow arrow_dataset arrow_flight parquet duckdb)