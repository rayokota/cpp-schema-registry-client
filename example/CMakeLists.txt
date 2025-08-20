# CMakeLists.txt for Schema Registry Client Examples
# This file is designed to be used as part of the main project build

# Find required packages
find_package(PkgConfig REQUIRED)

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

# Function to create example executable
function(add_example_executable target_name source_file)
    add_executable(${target_name} ${source_file})
    
    target_compile_options(${target_name} PRIVATE ${RDKAFKA_CFLAGS_OTHER})
    
    target_link_libraries(${target_name}
        schemaregistry
        ${RDKAFKA_LIBRARIES}
        ${ARGN}  # Additional libraries passed as arguments
    )
    
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

# Set output directory for example
set_target_properties(
    avro_consumer
    avro_producer
    avro_consumer_encryption
    avro_producer_encryption
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/example
)

# Add install targets for example
install(TARGETS
    avro_consumer
    avro_producer
    avro_consumer_encryption
    avro_producer_encryption
    DESTINATION bin/example
)

# Install README
install(FILES README.md DESTINATION share/doc/schemaregistry-examples)

# Display information
message(STATUS "Schema Registry Examples will be built in: ${CMAKE_BINARY_DIR}/example")
message(STATUS "Run 'make example' to build all examples")
