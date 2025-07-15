#ifndef SRCLIENT_REST_MODEL_SERVER_CONFIG_H_
#define SRCLIENT_REST_MODEL_SERVER_CONFIG_H_


#include <string>
#include "Metadata.h"
#include "RuleSet.h"
#include <optional>
#include <nlohmann/json.hpp>

namespace srclient::rest::model
{

enum class CompatibilityLevel {
    Backward,
    BackwardTransitive,
    Forward,
    ForwardTransitive,
    Full,
    FullTransitive,
    None,
};

/// <summary>
/// Config
/// </summary>
class  ServerConfig
{
public:
    ServerConfig();
    virtual ~ServerConfig() = default;

    bool operator==(const ServerConfig& rhs) const;
    bool operator!=(const ServerConfig& rhs) const;

    /////////////////////////////////////////////
    /// Config members

    /// <summary>
    /// Compatibility
    /// </summary>
    std::optional<CompatibilityLevel> getCompatibility() const;
    void setCompatibility(const std::optional<CompatibilityLevel>& value);
    /// <summary>
    /// Compatibility Level
    /// </summary>
    std::optional<CompatibilityLevel> getCompatibilityLevel() const;
    void setCompatibilityLevel(const std::optional<CompatibilityLevel>& value);
    /// <summary>
    /// 
    /// </summary>
    std::optional<std::string> getAlias() const;
    void setAlias(const std::optional<std::string>& value);
    /// <summary>
    /// 
    /// </summary>
    std::optional<bool> isNormalize() const;
    void setNormalize(const std::optional<bool>& value);
    /// <summary>
    /// 
    /// </summary>
    std::optional<bool> isValidateFields() const;
    void setValidateFields(const std::optional<bool>& value);
    /// <summary>
    /// 
    /// </summary>
    std::optional<bool> isValidateRules() const;
    void setValidateRules(const std::optional<bool>& value);
    /// <summary>
    /// 
    /// </summary>
    std::optional<std::string> getCompatibilityGroup() const;
    void setCompatibilityGroup(const std::optional<std::string>& value);
    /// <summary>
    /// 
    /// </summary>
    std::optional<srclient::rest::model::Metadata> getDefaultMetadata() const;
    void setDefaultMetadata(const std::optional<srclient::rest::model::Metadata>& value);
    /// <summary>
    /// 
    /// </summary>
    std::optional<srclient::rest::model::Metadata> getOverrideMetadata() const;
    void setOverrideMetadata(const std::optional<srclient::rest::model::Metadata>& value);
    /// <summary>
    /// 
    /// </summary>
    std::optional<srclient::rest::model::RuleSet> getDefaultRuleSet() const;
    void setDefaultRuleSet(const std::optional<srclient::rest::model::RuleSet>& value);
    /// <summary>
    /// 
    /// </summary>
    std::optional<srclient::rest::model::RuleSet> getOverrideRuleSet() const;
    void setOverrideRuleSet(const std::optional<srclient::rest::model::RuleSet>& value);

    friend  void to_json(nlohmann::json& j, const ServerConfig& o);
    friend  void from_json(const nlohmann::json& j, ServerConfig& o);
protected:
    std::optional<CompatibilityLevel> m_Compatibility;
    std::optional<CompatibilityLevel> m_CompatibilityLevel;
    std::optional<std::string> m_Alias;
    std::optional<bool> m_Normalize;
    std::optional<bool> m_ValidateFields;
    std::optional<bool> m_ValidateRules;
    std::optional<std::string> m_CompatibilityGroup;
    std::optional<srclient::rest::model::Metadata> m_DefaultMetadata;
    std::optional<srclient::rest::model::Metadata> m_OverrideMetadata;
    std::optional<srclient::rest::model::RuleSet> m_DefaultRuleSet;
    std::optional<srclient::rest::model::RuleSet> m_OverrideRuleSet;
    
};

} // namespace srclient::rest::model

#endif /* SRCLIENT_REST_MODEL_SERVER_CONFIG_H_ */
