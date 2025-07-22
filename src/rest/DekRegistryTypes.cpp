#include "srclient/rest/DekRegistryTypes.h"

// Implementation of hash specializations declared in the header file
namespace std {
    std::size_t hash<srclient::rest::KekId>::operator()(const srclient::rest::KekId& k) const {
        return std::hash<std::string>()(k.name) ^ (std::hash<bool>()(k.deleted) << 1);
    }

    std::size_t hash<srclient::rest::DekId>::operator()(const srclient::rest::DekId& k) const {
        return std::hash<std::string>()(k.kek_name) ^
               (std::hash<std::string>()(k.subject) << 1) ^
               (std::hash<int32_t>()(k.version) << 2) ^
               (std::hash<int>()(static_cast<int>(k.algorithm)) << 3) ^
               (std::hash<bool>()(k.deleted) << 4);
    }
} 