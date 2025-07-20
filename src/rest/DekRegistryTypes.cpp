#include "srclient/rest/DekRegistryTypes.h"

namespace std {
    // Hash specialization for KekId (moved from header)
    template<>
    struct hash<srclient::rest::KekId> {
        std::size_t operator()(const srclient::rest::KekId& k) const {
            return std::hash<std::string>()(k.name) ^ (std::hash<bool>()(k.deleted) << 1);
        }
    };

    // Hash specialization for DekId (moved from header)
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
} // namespace std 