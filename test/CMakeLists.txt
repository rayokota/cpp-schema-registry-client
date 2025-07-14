# Download GoogleTest if not already present
include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)
FetchContent_MakeAvailable(googletest)

# Add the test executable
add_executable(srclient-test
        test.cpp
)

target_link_libraries(srclient-test
        GTest::gtest_main
        srclient
)

include(GoogleTest)
gtest_discover_tests(srclient-test)