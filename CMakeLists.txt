cmake_minimum_required(VERSION 3.22)

# Set policies for better compatibility
if(POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)  # Set timestamp policy for URL downloads
endif()

project(SchemaRegistryClient CXX)

set(CMAKE_CXX_STANDARD 17)
set(CXX_STANDARD_REQUIRED ON)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)

FetchContent_Declare(
        avrocpp
        GIT_REPOSITORY https://github.com/apache/avro.git
        GIT_TAG release-1.12.0
        EXCLUDE_FROM_ALL
        SOURCE_SUBDIR lang/c++
)

FetchContent_MakeAvailable(avrocpp)

FetchContent_Declare(
        jsoncons
        GIT_REPOSITORY https://github.com/danielaparker/jsoncons.git
        GIT_TAG        v1.3.2
)
# Disable jsoncons unit tests
set(JSONCONS_BUILD_TESTS OFF CACHE BOOL "Build jsoncons tests" FORCE)
FetchContent_MakeAvailable(jsoncons)

# Note that cel-cpp has no CMake build and is not packaged anywhere,
# so we build protovalidate_cc as an in-tree dependency to get cel-cpp.
FetchContent_Declare(
    protovalidate_cc
    GIT_REPOSITORY "https://github.com/bufbuild/protovalidate-cc.git"
    GIT_TAG "v0.5.0"
)
FetchContent_MakeAvailable(protovalidate_cc)

# Import Tink as an in-tree dependency.
# See https://developers.google.com/tink/setup/cc
# Note: absl is provided by Tink, so we don't find_package it separately
FetchContent_Declare(
        tink
        URL       https://github.com/tink-crypto/tink-cc/archive/refs/tags/v2.4.0.zip
        URL_HASH  SHA256=06c4d49b0b1357f0b8c3abc77a7d920130dc868e4597d432a9ce1cda4f65e382
)
FetchContent_GetProperties(tink)
if(NOT googletest_POPULATED)
    FetchContent_Populate(tink)
    add_subdirectory(${tink_SOURCE_DIR} ${tink_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

find_package(httplib QUIET)
if(NOT httplib_FOUND)
    FetchContent_Declare(
            httplib
            GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
            GIT_TAG v0.23.1
    )
    FetchContent_MakeAvailable(httplib)
endif()

find_package(nlohmann_json QUIET)
if (NOT nlohmann_json_FOUND)
    FetchContent_Declare(
            nlohmann_json
            GIT_REPOSITORY https://github.com/nlohmann/json.git
            GIT_TAG v3.12.0
    )
    FetchContent_MakeAvailable(nlohmann_json)
endif()

# Compile project protobuf files
include(cmake/CompileProtobuf.cmake)
CompileProtobuf(generated_proto)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

file(GLOB_RECURSE HEADER_FILES "include/*.h")
file(GLOB_RECURSE SOURCE_FILES "src/*.cpp")

# Add generated protobuf files to the source list
if(PROTO_SOURCES)
    list(APPEND SOURCE_FILES ${PROTO_SOURCES})
endif()

add_library(schemaregistry ${HEADER_FILES} ${SOURCE_FILES})

include(cmake/Format.cmake)
Format(schemaregistry include)
Format(schemaregistry src)

target_compile_options(schemaregistry
    PRIVATE
        $<$<OR:$<CXX_COMPILER_ID:Clang>,$<CXX_COMPILER_ID:AppleClang>,$<CXX_COMPILER_ID:GNU>>:
            -Wall -Wno-unused-variable -Wno-unused-lambda-capture>
)

target_include_directories(schemaregistry
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE
        ${Protobuf_INCLUDE_DIRS}
        $<$<BOOL:${PROTO_INCLUDE_DIR}>:${PROTO_INCLUDE_DIR}>
)

target_link_libraries(schemaregistry
    PUBLIC 
        avrocpp
        cel_cpp
        cel_cpp_parser
        cel_cpp_minimal_descriptor_set
        jsoncons
        ${Protobuf_LIBRARIES}
    PRIVATE
        absl::log_internal_log_sink_set
        absl::log_internal_proto
        absl::absl_log
        absl::log_internal_message
        absl::hash
        tink::static
)

configure_package_config_file("cmake/${PROJECT_NAME}Config.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
)

# Enable testing
enable_testing()
add_subdirectory(test)
