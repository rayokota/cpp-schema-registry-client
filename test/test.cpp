#include <gtest/gtest.h>
#include "srclient/rest/ClientConfiguration.h"

TEST(MyLibraryTest, AddFunction) {
    org::openapitools::client::api::ClientConfiguration config({"http://localhost:8080"});
    EXPECT_EQ(config.getBaseUrls()[0], "http://localhost:8080");
}
