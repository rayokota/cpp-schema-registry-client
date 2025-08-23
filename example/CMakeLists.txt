# CMakeLists.txt for Schema Registry Client Examples
# This file is designed to be used as part of the main project build

# Find required packages
find_package(PkgConfig REQUIRED)
find_package(Protobuf REQUIRED)

# Find librdkafka
pkg_check_modules(RDKAFKA REQUIRED rdkafka)

# Common include directories
include_directories(
    ${RDKAFKA_INCLUDE_DIRS}
)

# Common link directories
link_directories(
    ${RDKAFKA_LIBRARY_DIRS}
)

# Compile project protobuf files
include(../cmake/CompileProtobuf.cmake)
CompileProtobuf(generated_example_proto ${CMAKE_SOURCE_DIR}/proto)

# Function to create example executable
function(add_example_executable target_name source_file)
    add_executable(${target_name} ${source_file})
    
    target_compile_options(${target_name} PRIVATE ${RDKAFKA_CFLAGS_OTHER})
    
    # Add protobuf include directory for protobuf examples
    if(${target_name} MATCHES "protobuf_.*")
        target_include_directories(${target_name} PRIVATE
            ${PROTO_INCLUDE_DIR}
            ${CMAKE_BINARY_DIR}/generated_proto  # Include main project's generated protos
        )
        target_sources(${target_name} PRIVATE ${PROTO_SOURCES})
        target_link_libraries(${target_name}
            schemaregistry
            ${RDKAFKA_LIBRARIES}
            ${Protobuf_LIBRARIES}
            ${ARGN}  # Additional libraries passed as arguments
        )
    else()
        target_link_libraries(${target_name}
            schemaregistry
            ${RDKAFKA_LIBRARIES}
            ${ARGN}  # Additional libraries passed as arguments
        )
    endif()
    
    # Add to example target
    add_dependencies(example ${target_name})
endfunction()

# Create example target
add_custom_target(example)

# Basic Avro Consumer
add_example_executable(avro_consumer AvroConsumer.cpp)

# Basic Avro Producer  
add_example_executable(avro_producer AvroProducer.cpp)

# Avro Consumer with Encryption
add_example_executable(avro_consumer_encryption AvroConsumerEncryption.cpp
    tink::static
)

# Avro Producer with Encryption
add_example_executable(avro_producer_encryption AvroProducerEncryption.cpp
    tink::static
)

# Basic JSON Consumer
add_example_executable(json_consumer JsonConsumer.cpp)

# Basic JSON Producer  
add_example_executable(json_producer JsonProducer.cpp)

# JSON Consumer with Encryption
add_example_executable(json_consumer_encryption JsonConsumerEncryption.cpp
    tink::static
)

# JSON Producer with Encryption
add_example_executable(json_producer_encryption JsonProducerEncryption.cpp
    tink::static
)

# Basic Protobuf Consumer
add_example_executable(protobuf_consumer ProtobufConsumer.cpp)

# Basic Protobuf Producer  
add_example_executable(protobuf_producer ProtobufProducer.cpp)

# Protobuf Consumer with Encryption
add_example_executable(protobuf_consumer_encryption ProtobufConsumerEncryption.cpp
    tink::static
)

# Protobuf Producer with Encryption
add_example_executable(protobuf_producer_encryption ProtobufProducerEncryption.cpp
    tink::static
)

# Protobuf compilation and dependencies are handled in add_example_executable function

# Set output directory for example
set_target_properties(
    avro_consumer
    avro_producer
    avro_consumer_encryption
    avro_producer_encryption
    json_consumer
    json_producer
    json_consumer_encryption
    json_producer_encryption
    protobuf_consumer
    protobuf_producer
    protobuf_consumer_encryption
    protobuf_producer_encryption
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/example
)

# Add install targets for example
install(TARGETS
    avro_consumer
    avro_producer
    avro_consumer_encryption
    avro_producer_encryption
    json_consumer
    json_producer
    json_consumer_encryption
    json_producer_encryption
    protobuf_consumer
    protobuf_producer
    protobuf_consumer_encryption
    protobuf_producer_encryption
    DESTINATION bin/example
)

# Install README
install(FILES README.md DESTINATION share/doc/schemaregistry-examples)

# Display information
message(STATUS "Schema Registry Examples will be built in: ${CMAKE_BINARY_DIR}/example")
message(STATUS "Run 'make example' to build all examples")
