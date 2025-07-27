include(FetchContent)

# Download GoogleTest if not already present
# See https://google.github.io/googletest/quickstart-cmake.html
FetchContent_Declare(
    googletest
    URL                    https://github.com/google/googletest/archive/refs/tags/v1.17.0.zip
    DOWNLOAD_EXTRACT_TIMESTAMP true
)
FetchContent_MakeAvailable(googletest)

# Compile project protobuf files
include(../cmake/CompileProtobuf.cmake)
CompileProtobuf(generated_test_proto ${CMAKE_SOURCE_DIR}/proto)

# Collect test source files
file(GLOB TEST_SOURCE_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

# Add the test executable
add_executable(schemaregistry-test
    ${TEST_SOURCE_FILES}
    ${PROTO_SOURCES}
)

# Include directories for the test executable
target_include_directories(schemaregistry-test
    PRIVATE
        ${Protobuf_INCLUDE_DIRS}
        $<$<BOOL:${PROTO_INCLUDE_DIR}>:${PROTO_INCLUDE_DIR}>
        ${CMAKE_BINARY_DIR}/generated_proto  # Include main project's generated protos
)

# Link libraries for the test executable
target_link_libraries(schemaregistry-test
    PRIVATE
        GTest::gtest_main
        schemaregistry
        ${Protobuf_LIBRARIES}
        absl::log_internal_log_sink_set
        absl::log_internal_proto
        absl::absl_log
        absl::log_internal_message
        absl::hash
        tink::static
)

# Discover and register tests
include(GoogleTest)
gtest_discover_tests(schemaregistry-test)
