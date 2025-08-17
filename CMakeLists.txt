cmake_minimum_required(VERSION 3.22)

# Set policies for better compatibility
if(POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)  # Set timestamp policy for URL downloads
endif()

project(schemaregistry VERSION 0.1.0 LANGUAGES CXX)

# Enforce C++17 globally so all fetched dependencies (e.g., ANTLR4 runtime)
# are built with C++17 features available (std::any, std::shared_mutex, etc.).
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)

# Fetch Apache Avro C++
FetchContent_Declare(
    avrocpp
    GIT_REPOSITORY https://github.com/apache/avro.git
    GIT_TAG        release-1.12.0
    EXCLUDE_FROM_ALL
    SOURCE_SUBDIR  lang/c++
)
# Disable Avro subproject tests to avoid registering them with ctest here
set(AVROCPP_BUILD_TESTS OFF CACHE BOOL "Build Avro C++ tests" FORCE)
set(AVRO_BUILD_TESTS OFF CACHE BOOL "Build Avro tests" FORCE)
FetchContent_MakeAvailable(avrocpp)

# Fetch jsonata
FetchContent_Declare(
    jsonata
    GIT_REPOSITORY https://github.com/rayokota/jsonata-cpp.git
    GIT_TAG 9981428df6fe5bb7c1bbda2ee3a532117cb5c951
)
# Disable jsonata unit tests
set(JSONATA_BUILD_TESTS OFF CACHE BOOL "Build jsonata tests" FORCE)
FetchContent_MakeAvailable(jsonata)

# Fetch jsoncons
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
    GIT_REPOSITORY https://github.com/bufbuild/protovalidate-cc.git
    GIT_TAG        v0.5.0
)
FetchContent_MakeAvailable(protovalidate_cc)

# Import Tink as an in-tree dependency.
# See https://developers.google.com/tink/setup/cc
# Note: absl is provided by Tink, so we don't find_package it separately
FetchContent_Declare(
    tink
    URL      https://github.com/tink-crypto/tink-cc/archive/refs/tags/v2.4.0.zip
    URL_HASH SHA256=06c4d49b0b1357f0b8c3abc77a7d920130dc868e4597d432a9ce1cda4f65e382
)
FetchContent_GetProperties(tink)
if(NOT tink_POPULATED)
    FetchContent_Populate(tink)
    add_subdirectory(${tink_SOURCE_DIR} ${tink_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

# Fetch Tink AWS KMS extension
FetchContent_Declare(
    tink_cc_awskms
    GIT_REPOSITORY https://github.com/tink-crypto/tink-cc-awskms.git
    GIT_TAG main
)

# Fetch Tink GCP KMS extension
FetchContent_Declare(
    tink_cc_gcpkms
    GIT_REPOSITORY https://github.com/tink-crypto/tink-cc-gcpkms.git
    GIT_TAG main
)

# Fetch the KMS extensions
FetchContent_GetProperties(tink_cc_awskms)
if(NOT tink_cc_awskms_POPULATED)
    FetchContent_Populate(tink_cc_awskms)
endif()

FetchContent_GetProperties(tink_cc_gcpkms)
if(NOT tink_cc_gcpkms_POPULATED)
    FetchContent_Populate(tink_cc_gcpkms)
endif()

# Create interface libraries for the KMS extensions
add_library(tink_awskms INTERFACE)
add_library(tink_gcpkms INTERFACE)

# Set include directories for KMS extensions
target_include_directories(tink_awskms INTERFACE
    $<BUILD_INTERFACE:${tink_cc_awskms_SOURCE_DIR}>
)

target_include_directories(tink_gcpkms INTERFACE
    $<BUILD_INTERFACE:${tink_cc_gcpkms_SOURCE_DIR}>
)

# Link AWS SDK to AWS KMS extension if enabled
if(SCHEMAREGISTRY_ENABLE_AWS_KMS AND AWS_SDK_TARGETS)
    target_link_libraries(tink_awskms INTERFACE ${AWS_SDK_TARGETS})
    # Also add AWS SDK include directories to tink_awskms
    if(TARGET aws-cpp-sdk-core)
        target_include_directories(tink_awskms INTERFACE 
            $<TARGET_PROPERTY:aws-cpp-sdk-core,INTERFACE_INCLUDE_DIRECTORIES>
        )
    endif()
    if(TARGET aws-cpp-sdk-kms)
        target_include_directories(tink_awskms INTERFACE 
            $<TARGET_PROPERTY:aws-cpp-sdk-kms,INTERFACE_INCLUDE_DIRECTORIES>
        )
    endif()
endif()



# Create namespaced aliases for KMS extensions
add_library(schemaregistry::tink_awskms ALIAS tink_awskms)
add_library(schemaregistry::tink_gcpkms ALIAS tink_gcpkms)

# Find httplib or fetch if not found
find_package(httplib QUIET)
if(NOT httplib_FOUND)
    FetchContent_Declare(
        httplib
        GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
        GIT_TAG        v0.23.1
    )
    FetchContent_MakeAvailable(httplib)
endif()

# Find nlohmann_json or fetch if not found
find_package(nlohmann_json QUIET)
if(NOT nlohmann_json_FOUND)
    FetchContent_Declare(
        nlohmann_json
        GIT_REPOSITORY https://github.com/nlohmann/json.git
        GIT_TAG        v3.12.0
    )
    FetchContent_MakeAvailable(nlohmann_json)
endif()

# AWS SDK C++ modular components: aws-cpp-sdk-core and aws-cpp-sdk-kms
option(SCHEMAREGISTRY_ENABLE_AWS_KMS "Enable AWS KMS support with modular AWS SDK components" ON)
if(SCHEMAREGISTRY_ENABLE_AWS_KMS)
    # Try to find AWS SDK with specific modular components first
    find_package(AWSSDK QUIET COMPONENTS core kms)
    
    if(NOT AWSSDK_FOUND)
        message(STATUS "AWS SDK not found, fetching modular components (aws-cpp-sdk-core, aws-cpp-sdk-kms) from source...")
        
        # Configure AWS SDK build options for modular components only
        set(BUILD_ONLY "core;kms" CACHE STRING "Build only aws-cpp-sdk-core and aws-cpp-sdk-kms modular components")
        set(ENABLE_TESTING OFF CACHE BOOL "Disable AWS SDK testing")
        set(AUTORUN_UNIT_TESTS OFF CACHE BOOL "Disable AWS SDK unit tests")
        set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build AWS SDK as static libraries")
        set(ENABLE_UNITY_BUILD ON CACHE BOOL "Enable unity build for faster compilation")
        set(CPP_STANDARD 17 CACHE STRING "C++ standard for AWS SDK")
        # Fix CMake compatibility issues with CRT
        set(CMAKE_POLICY_VERSION_MINIMUM 3.5 CACHE STRING "Set minimum CMake policy version for compatibility")
        set(BUILD_DEPS OFF CACHE BOOL "Disable AWS SDK dependency builds to avoid CRT issues")
        
        FetchContent_Declare(
            aws-sdk-cpp
            GIT_REPOSITORY https://github.com/aws/aws-sdk-cpp.git
            GIT_TAG        1.11.400  # Use a newer stable release with better CMake support
            GIT_SHALLOW    TRUE
        )
        
        FetchContent_MakeAvailable(aws-sdk-cpp)
        
        # Set modular AWS SDK component targets for linking
        set(AWS_SDK_TARGETS aws-cpp-sdk-core aws-cpp-sdk-kms)
        message(STATUS "Successfully built modular AWS SDK components: aws-cpp-sdk-core, aws-cpp-sdk-kms")
    else()
        message(STATUS "Found system AWS SDK with modular components: ${AWSSDK_LINK_LIBRARIES}")
        set(AWS_SDK_TARGETS ${AWSSDK_LINK_LIBRARIES})
    endif()
endif()

# Find Protobuf for protoc executable and libraries
find_package(Protobuf REQUIRED)

# Compile project protobuf files
include(cmake/CompileProtobuf.cmake)
CompileProtobuf(generated_proto)

# Collect source files
file(GLOB_RECURSE HEADERS "include/*.h")
file(GLOB_RECURSE SOURCES "src/*.cpp")

# Add generated protobuf files to the source list
if(PROTO_SOURCES)
    list(APPEND SOURCES ${PROTO_SOURCES})
endif()

# Create the main library
add_library(schemaregistry ${HEADERS} ${SOURCES})

# KMS extensions are available as optional targets (users can link these as needed)
# Example: target_link_libraries(your_target PRIVATE schemaregistry::schemaregistry schemaregistry::tink_awskms schemaregistry::tink_gcpkms)

# Provide a namespaced ALIAS for consumers (and internal consistency)
# Build-tree alias matching the installed namespace
add_library(schemaregistry::schemaregistry ALIAS schemaregistry)

# Set library version metadata for shared library consumers/package managers
set_target_properties(schemaregistry PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
)

# Setup formatting
include(cmake/Format.cmake)
Format(schemaregistry include)
Format(schemaregistry src)

# Compiler features
target_compile_features(schemaregistry PUBLIC cxx_std_17)

# Compiler options
target_compile_options(schemaregistry
    PRIVATE
        $<$<OR:$<CXX_COMPILER_ID:Clang>,$<CXX_COMPILER_ID:AppleClang>,$<CXX_COMPILER_ID:GNU>>:
            -Wall -Wno-unused-variable -Wno-unused-lambda-capture>
)

# Include directories
target_include_directories(schemaregistry
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/internal>
        ${Protobuf_INCLUDE_DIRS}
        $<$<BOOL:${PROTO_INCLUDE_DIR}>:${PROTO_INCLUDE_DIR}>
        $<BUILD_INTERFACE:${tink_cc_awskms_SOURCE_DIR}>
        $<BUILD_INTERFACE:${tink_cc_gcpkms_SOURCE_DIR}>
        # Add AWS SDK include directories when enabled
        $<$<BOOL:${SCHEMAREGISTRY_ENABLE_AWS_KMS}>:$<TARGET_PROPERTY:aws-cpp-sdk-core,INTERFACE_INCLUDE_DIRECTORIES>>
)

# Link libraries
# Use BUILD_INTERFACE so install(EXPORT ...) does not reference non-exported
# in-tree dependency targets (avoids CMake install errors).
target_link_libraries(schemaregistry
    PUBLIC
        $<BUILD_INTERFACE:avrocpp;cel_cpp;cel_cpp_parser;cel_cpp_minimal_descriptor_set;${Protobuf_LIBRARIES}>
    PRIVATE
        $<BUILD_INTERFACE:absl::log_internal_log_sink_set;absl::log_internal_proto;absl::absl_log;absl::log_internal_message;absl::hash;jsonata;jsoncons;tink::static>
        # Link AWS SDK for KMS driver compilation
        $<$<BOOL:${SCHEMAREGISTRY_ENABLE_AWS_KMS}>:${AWS_SDK_TARGETS}>
)

# Note: KMS extensions (tink_awskms, tink_gcpkms) are available as optional targets
# Users can link them separately as needed:
# target_link_libraries(your_target PRIVATE schemaregistry::tink_awskms schemaregistry::tink_gcpkms)

OPTION(SCHEMAREGISTRY_BUILD_TESTS "schemaregistry test suite" ON)

if(SCHEMAREGISTRY_BUILD_TESTS)
    include(CTest)
    enable_testing()
    add_subdirectory(test)
endif()

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# 1. Install the library itself (shared or static)
install(TARGETS schemaregistry
        EXPORT schemaregistryTargets  # This creates a target file for find_package()
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} # For static libraries (.a, .lib)
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} # For shared libraries (.so, .dylib)
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} # For module libraries or if you want shared libs in bin (less common for pure libs)
)

# 2. Install public header files
# Assuming your public headers are in the 'include' directory at the top level
install(DIRECTORY include/
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        FILES_MATCHING PATTERN "*.h"
)

# Install generated Protobuf headers
if(PROTO_INCLUDE_DIR AND EXISTS ${PROTO_INCLUDE_DIR})
    install(DIRECTORY ${PROTO_INCLUDE_DIR}/
            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
            FILES_MATCHING PATTERN "*.h"
    )
endif()

# 3. Generate and install the CMake package configuration files
#    These files allow other projects to find your library using find_package(schemaregistry)

# Configure the package config file for package name 'schemaregistry'
configure_package_config_file(
        "cmake/schemaregistryConfig.cmake.in"
        "${CMAKE_CURRENT_BINARY_DIR}/schemaregistryConfig.cmake"
        INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/schemaregistry"
)

# Generate the version file
write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/schemaregistryConfigVersion.cmake"
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY AnyNewerVersion
)

# Install the generated config and version files
install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/schemaregistryConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/schemaregistryConfigVersion.cmake"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/schemaregistry"
)

# Install the export file for targets. This should be done AFTER all targets are defined.
# This file is used by the Config file to import your library targets.
install(EXPORT schemaregistryTargets
        FILE schemaregistryTargets.cmake
        NAMESPACE schemaregistry::
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/schemaregistry"
)

