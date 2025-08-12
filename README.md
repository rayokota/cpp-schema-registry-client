## Schema Registry C++ Client Library

### Build

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
cmake --install build --prefix /usr/local
```

### CMake usage

```cmake
find_package(schemaregistry CONFIG REQUIRED)
target_link_libraries(myapp PRIVATE schemaregistry::schemaregistry)
```

This project installs a CMake package called `schemaregistry`. The installed target is exported under the `schemaregistry::` namespace. In the build tree, an alias with the same namespace is provided for consistency.
