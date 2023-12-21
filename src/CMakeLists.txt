cmake_minimum_required(VERSION 3.2)

find_package(thallium REQUIRED)
find_package(Arrow REQUIRED)
find_package(Boost REQUIRED)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(c client.cc)
target_link_libraries(c thallium arrow arrow_dataset parquet duckdb)

add_executable(s server.cc)
target_link_libraries(s thallium arrow arrow_dataset parquet duckdb)

add_executable(c2 client2.cc)
target_link_libraries(c2 thallium arrow arrow_dataset parquet duckdb)

add_executable(s2 server2.cc)
target_link_libraries(s2 thallium arrow arrow_dataset parquet duckdb)

add_executable(min_client min_client.cc)
target_link_libraries(min_client thallium arrow arrow_dataset parquet duckdb)

add_executable(min_server min_server.cc)
target_link_libraries(min_server thallium arrow arrow_dataset parquet duckdb)

add_executable(min_client2 min_client2.cc)
target_link_libraries(min_client2 thallium arrow arrow_dataset parquet duckdb)

add_executable(min_server2 min_server2.cc)
target_link_libraries(min_server2 thallium arrow arrow_dataset parquet duckdb)

add_executable(min_client3 min_client3.cc)
target_link_libraries(min_client3 thallium arrow arrow_dataset parquet duckdb)

add_executable(min_server3 min_server3.cc)
target_link_libraries(min_server3 thallium arrow arrow_dataset parquet duckdb)

add_executable(min_client4 min_client4.cc)
target_link_libraries(min_client4 thallium arrow arrow_dataset parquet duckdb)

add_executable(min_server4 min_server4.cc)
target_link_libraries(min_server4 thallium arrow arrow_dataset parquet duckdb)

add_executable(min_client5 min_client5.cc)
target_link_libraries(min_client5 thallium arrow arrow_dataset parquet duckdb)

add_executable(min_server5 min_server5.cc)
target_link_libraries(min_server5 thallium arrow arrow_dataset parquet duckdb)

add_executable(tc thallium_client.cc)
target_link_libraries(tc thallium arrow arrow_dataset duckdb)

add_executable(ts thallium_server.cc)
target_link_libraries(ts thallium arrow arrow_dataset duckdb)

add_executable(fc flight_client.cc)
target_link_libraries(fc arrow arrow_dataset arrow_flight parquet duckdb)

add_executable(fs flight_server.cc)
target_link_libraries(fs arrow arrow_dataset arrow_flight parquet duckdb)

add_executable(expose expose.cc)
target_link_libraries(expose thallium arrow arrow_dataset parquet duckdb)
