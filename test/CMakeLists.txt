include(FetchContent)

# Download GoogleTest if not already present
# See https://google.github.io/googletest/quickstart-cmake.html
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/refs/tags/v1.17.0.zip
        DOWNLOAD_EXTRACT_TIMESTAMP true
)
FetchContent_MakeAvailable(googletest)

# Function to compile protobuf files in the test/proto/ directory
function(compile_test_proto_files)
    # Find all .proto files in the test/proto/ directory only
    file(GLOB_RECURSE TEST_PROTO_FILES "${CMAKE_CURRENT_SOURCE_DIR}/proto/*.proto")
    
    if(TEST_PROTO_FILES)
        # Create directory for generated files
        set(TEST_PROTO_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated_test_proto")
        file(MAKE_DIRECTORY ${TEST_PROTO_GEN_DIR})
        
        foreach(PROTO_FILE ${TEST_PROTO_FILES})
            # Get the file name without extension
            get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
            
            # Get relative path from test/proto/ directory
            file(RELATIVE_PATH PROTO_REL_PATH "${CMAKE_CURRENT_SOURCE_DIR}/proto" "${PROTO_FILE}")
            get_filename_component(PROTO_REL_DIR ${PROTO_REL_PATH} DIRECTORY)
            
            # Generate the output file names
            set(PROTO_OUT_DIR "${TEST_PROTO_GEN_DIR}/${PROTO_REL_DIR}")
            file(MAKE_DIRECTORY ${PROTO_OUT_DIR})
            set(PROTO_SRCS "${PROTO_OUT_DIR}/${PROTO_NAME}.pb.cc")
            set(PROTO_HDRS "${PROTO_OUT_DIR}/${PROTO_NAME}.pb.h")
            
            # Add custom command to generate C++ files from .proto
            add_custom_command(
                OUTPUT ${PROTO_SRCS} ${PROTO_HDRS}
                COMMAND ${Protobuf_PROTOC_EXECUTABLE}
                ARGS --cpp_out=${TEST_PROTO_GEN_DIR}
                     --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto
                     --proto_path=${CMAKE_SOURCE_DIR}/proto
                     ${PROTO_FILE}
                DEPENDS ${PROTO_FILE}
                COMMENT "Generating C++ code from test/proto/${PROTO_REL_PATH}"
                VERBATIM
            )
            
            # Add generated files to the list
            list(APPEND TEST_PROTO_GENERATED_SRCS ${PROTO_SRCS})
            list(APPEND TEST_PROTO_GENERATED_HDRS ${PROTO_HDRS})
        endforeach()
        
        # Set variables in parent scope
        set(TEST_PROTO_SOURCES ${TEST_PROTO_GENERATED_SRCS} PARENT_SCOPE)
        set(TEST_PROTO_HEADERS ${TEST_PROTO_GENERATED_HDRS} PARENT_SCOPE)
        set(TEST_PROTO_INCLUDE_DIR ${TEST_PROTO_GEN_DIR} PARENT_SCOPE)
    endif()
endfunction()

# Compile test protobuf files
compile_test_proto_files()

file(GLOB TEST_SOURCE_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

# Add the test executable
add_executable(schemaregistry-test
        ${TEST_SOURCE_FILES}
        ${TEST_PROTO_SOURCES}
)

target_include_directories(schemaregistry-test
    PRIVATE
        ${Protobuf_INCLUDE_DIRS}
        $<$<BOOL:${TEST_PROTO_INCLUDE_DIR}>:${TEST_PROTO_INCLUDE_DIR}>
        ${CMAKE_BINARY_DIR}/generated_proto  # Include main project's generated protos
)

target_link_libraries(schemaregistry-test
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

include(GoogleTest)
gtest_discover_tests(schemaregistry-test)