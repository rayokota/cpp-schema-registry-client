/**
 * DekRegistryTypes
 * Common types and hash functions for DEK Registry Client
 */

#ifndef SRCLIENT_REST_DEK_REGISTRY_TYPES_H_
#define SRCLIENT_REST_DEK_REGISTRY_TYPES_H_

#include "srclient/rest/model/Dek.h"
#include <string>
#include <functional>
#include <cstdint>

namespace srclient::rest {

/**
 * Key ID for KEK caching
 */
struct KekId {
    std::string name;
    bool deleted;

    bool operator==(const KekId& other) const {
        return name == other.name && deleted == other.deleted;
    }
};

/**
 * Key ID for DEK caching
 */
struct DekId {
    std::string kekName;
    std::string subject;
    int32_t version;
    srclient::rest::model::Algorithm algorithm;
    bool deleted;

    bool operator==(const DekId& other) const {
        return kekName == other.kekName &&
               subject == other.subject &&
               version == other.version &&
               algorithm == other.algorithm &&
               deleted == other.deleted;
    }
};

} // namespace srclient::rest

// Hash specializations for std::unordered_map
namespace std {
    template<>
    struct hash<srclient::rest::KekId> {
        std::size_t operator()(const srclient::rest::KekId& k) const {
            return std::hash<std::string>()(k.name) ^ (std::hash<bool>()(k.deleted) << 1);
        }
    };

    template<>
    struct hash<srclient::rest::DekId> {
        std::size_t operator()(const srclient::rest::DekId& k) const {
            return std::hash<std::string>()(k.kekName) ^
                   (std::hash<std::string>()(k.subject) << 1) ^
                   (std::hash<int32_t>()(k.version) << 2) ^
                   (std::hash<int>()(static_cast<int>(k.algorithm)) << 3) ^
                   (std::hash<bool>()(k.deleted) << 4);
        }
    };
}

#endif // SRCLIENT_REST_DEK_REGISTRY_TYPES_H_ 